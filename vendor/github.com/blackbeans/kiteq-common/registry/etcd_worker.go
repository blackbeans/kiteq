package registry

import (
	"github.com/blackbeans/kiteq-common/registry/bind"
	log "github.com/blackbeans/log4go"
	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"strings"
	"sync"
	"time"
)

type Worker interface {
	Start()
	Keepalive()
}

type EtcdWatcher interface {
	Watch()
	Stop()
}

type QServerWroker struct {
	Api             client.KeysAPI
	Hostport        string
	Topics          []string
	KeepalivePeriod time.Duration
	lastKeepalive   time.Time
}

func (self *QServerWroker) Start() {
	so := &client.SetOptions{TTL: 3 * self.KeepalivePeriod, Dir: true}
	for _, topic := range self.Topics {
		qpath := KITEQ_SERVER + "/" + topic + "/" + self.Hostport
		self.Api.Delete(context.Background(), qpath, &client.DeleteOptions{Recursive: true})
		_, err := self.Api.Set(context.Background(), qpath, "", so)
		if nil != err {
			log.ErrorLog("kiteq_registry", "QServerWroker|Keepalive|Start|FAIL|%s|%s", qpath, err)
		} else {
			log.InfoLog("kiteq_registry", "QServerWroker|Keepalive|Start|SUCC|%s", qpath)
		}
	}
	//先移除
	self.Api.Delete(context.Background(), KITEQ_ALL_SERVERS+"/"+self.Hostport, &client.DeleteOptions{Recursive: true})
	self.Api.Delete(context.Background(), KITEQ_ALIVE_SERVERS+"/"+self.Hostport, &client.DeleteOptions{Recursive: true})

	//create kiteqserver
	self.Api.Set(context.Background(), KITEQ_ALIVE_SERVERS+"/"+self.Hostport, "", so)
	self.Api.Set(context.Background(), KITEQ_ALL_SERVERS+"/"+self.Hostport, "", so)
}

//Keepalive with fixed period
func (self *QServerWroker) Keepalive() {
	//if not need keepalive
	if self.lastKeepalive.Add(self.KeepalivePeriod).After(time.Now()) {
		return
	}

	op := &client.SetOptions{Dir: true, TTL: 3 * self.KeepalivePeriod}
	for _, topic := range self.Topics {
		qpath := KITEQ_SERVER + "/" + topic + "/" + self.Hostport
		_, err := self.Api.Set(context.Background(), qpath, "", op)
		if nil != err {
			log.ErrorLog("kiteq_registry", "QServerWroker|Keepalive|FAIL|%s|%s", qpath, err)
		}
	}

	//keepalive alive server
	self.Api.Set(context.Background(), KITEQ_ALIVE_SERVERS+"/"+self.Hostport, "", op)
	self.Api.Set(context.Background(), KITEQ_ALL_SERVERS+"/"+self.Hostport, "", op)

	self.lastKeepalive = time.Now()
}

type QServersWatcher struct {
	Api          client.KeysAPI
	Topics       []string
	Watcher      IWatcher
	topic2Server map[string] /*topic*/ []string
	lock         sync.Mutex
	cancelWatch  bool
}

//watch QServer Change
func (self *QServersWatcher) Watch() {

	self.topic2Server = make(map[string][]string, 10)
	for _, topic := range self.Topics {
		qpath := KITEQ_SERVER + "/" + topic
		traverseCreateEtcdPath(self.Api, qpath)
		tmpTopic := topic
		//start watch topic server changed
		go func() {
			w := self.Api.Watcher(qpath, &client.WatcherOptions{Recursive: false})
			for !self.cancelWatch {

				resp, err := w.Next(context.Background())
				if nil != err {
					log.ErrorLog("kiteq_registry", "QServerWroker|Watch|FAIL|%s|%s", qpath, err)
					break
				}

				self.lock.Lock()
				//check local data
				servers, ok := self.topic2Server[tmpTopic]
				if !ok {
					servers = make([]string, 0, 10)
				}

				ipport := resp.Node.Key[strings.LastIndex(resp.Node.Key, "/")+1:]
				existIdx := -1
				for i, s := range servers {
					if s == ipport {
						existIdx = i
						break
					}
				}

				if resp.Action == "create" {
					if existIdx < 0 {
						servers = append(servers, ipport)
						//push node children changed
						self.Watcher.NodeChange(qpath, Child, servers)
						log.InfoLog("kiteq_registry", "QServerWroker|Watch|SUCC|%s|%s|%v", resp.Action, qpath, servers)
					}

				} else if resp.Action == "delete " || resp.Action == "expire" {
					//delete server
					if existIdx >= 0 {
						servers = append(servers[0:existIdx], servers[existIdx+1:]...)
						self.Watcher.NodeChange(qpath, Child, servers)
						log.InfoLog("kiteq_registry", "QServerWroker|Watch|SUCC|%s|%s|%v", resp.Action, qpath, servers)
					}
				}

				self.topic2Server[tmpTopic] = servers
				self.lock.Unlock()
			}
		}()
	}
}

func (self *QServersWatcher) Stop() {
	self.cancelWatch = true
}

//qclient workder
type QClientWorker struct {
	Api             client.KeysAPI
	PublishTopics   []string
	Hostport        string
	GroupId         string
	Bindings        []*bind.Binding
	KeepalivePeriod time.Duration
	lastKeepalive   time.Time
}

func (self *QClientWorker) Start() {

	//初始化订阅关系
	//按topic分组
	groupBind := make(map[string][]*bind.Binding, 10)
	for _, b := range self.Bindings {
		g, ok := groupBind[b.Topic]
		if !ok {
			g = make([]*bind.Binding, 0, 2)
		}
		b.GroupId = self.GroupId
		g = append(g, b)
		groupBind[b.Topic] = g
	}

	for topic, binds := range groupBind {
		data, err := bind.MarshalBinds(binds)
		if nil != err {
			log.ErrorLog("kiteq_registry", "QClientWorker|Start|MarshalBind|FAIL|%s|%s|%v\n", err, self.GroupId, binds)
			continue
		}

		path := KITEQ_SUB + "/" + topic + "/" + self.GroupId + "-bind"
		//注册对应topic的groupId //注册订阅信息 not expired
		_, err = self.Api.Set(context.Background(), path, string(data),
			&client.SetOptions{Dir: false})
		if nil != err {
			log.ErrorLog("kiteq_registry", "QClientWorker|Start||Bind|FAIL|%s|%s|%v\n", err, path, binds)
		} else {
			// log.InfoLog("kiteq_registry", "QClientWorker|Keepalive||Bind|SUCC|%s|%v\n", succpath, binds)
		}
	}

	//init
	self.Keepalive()
}

//Keepalive with fixed period
func (self *QClientWorker) Keepalive() {
	//if not need keepalive
	if self.lastKeepalive.Add(self.KeepalivePeriod).After(time.Now()) {
		return
	}

	//PublishTopics
	for _, topic := range self.PublishTopics {
		pubPath := KITEQ_PUB + "/" + topic + "/" + self.GroupId + "/" + self.Hostport
		_, err := self.Api.Set(context.Background(),
			pubPath, "", &client.SetOptions{Dir: false, TTL: self.KeepalivePeriod * 2})

		if nil != err {
			log.WarnLog("kiteq_registry", "QClientWorker|Keepalive|FAIL|%s|%s", pubPath, err)
		}
	}
	self.lastKeepalive = time.Now()
}

type BindWatcher struct {
	Api         client.KeysAPI
	Topics      []string
	Watcher     IWatcher
	binds       map[string] /*topic*/ map[string] /*groupId*/ []*bind.Binding
	cancelWatch bool
	lock        sync.Mutex
}

//watch QServer Change
func (self *BindWatcher) Watch() {

	self.binds = make(map[string]map[string][]*bind.Binding, 5)

	for _, topic := range self.Topics {

		path := KITEQ_SUB + "/" + topic
		traverseCreateEtcdPath(self.Api, path)

		tmpTopic := topic
		//start watch topic server changed
		go func() {
			//watch bind Group Change
			w := self.Api.Watcher(path, &client.WatcherOptions{Recursive: true})
			for !self.cancelWatch {
				resp, err := w.Next(context.Background())
				if nil != err {
					log.ErrorLog("kiteq_registry", "BindWatcher|Watch|FAIL|%s|%s", path, err)
					break
				}

				//groupLevel
				if strings.HasSuffix(resp.Node.Key, "-bind") {

					//group Level
					if resp.Action == "delete" || resp.Action == "expire" {
						//push bind data changed
						children := make([]string, 0, resp.Node.Nodes.Len())
						now := time.Now()
						for _, n := range resp.Node.Nodes {
							if !n.Expiration.Before(now) {
								children = append(children)
							}
						}

						//remove local binds
						self.lock.Lock()
						gb, ok := self.binds[tmpTopic]
						if ok {
							delete(gb, resp.Node.Key)
						}
						self.lock.Unlock()

						self.Watcher.NodeChange(path, Child, children)
						log.InfoLog("kiteq_registry", "BindWatcher|Watch|Bind GROUP LEVEL |SUCC|%s|%s|%v", path, resp.Node.Key, children)
					} else if resp.Action == "set" {
						//set bind data
						binds, err := bind.UmarshalBinds([]byte(resp.Node.Value))
						if nil != err {
							log.ErrorLog("kiteq_registry", "BindWatcher|Watch|Bind|UmarshalBinds|FAIL|%s|%v", resp.Node.Key, resp.Node.Value)
							continue
						}

						key := resp.Node.Key
						groupId := key[strings.LastIndex(key, "/")+1 : strings.LastIndex(key, "-bind")]
						//update local binds
						self.lock.Lock()
						gb, ok := self.binds[tmpTopic]
						if !ok {
							gb = make(map[string][]*bind.Binding, 2)
							self.binds[tmpTopic] = gb
						}
						gb[groupId] = binds
						self.lock.Unlock()

						//push bind data changed
						self.Watcher.DataChange(resp.Node.Key, binds)
						log.InfoLog("kiteq_registry", "BindWatcher|Watch|Bind|SUCC|%s|%v", resp.Node.Key, binds)
					}
				} else if strings.Index(resp.Node.Key, "-bind") >= 0 {
					//bind data level
					if resp.Action == "set" || resp.Action == "update" {
						binds, err := bind.UmarshalBinds([]byte(resp.Node.Value))
						if nil != err {
							log.ErrorLog("kiteq_registry", "BindWatcher|Watch|Bind|UmarshalBinds|FAIL|%s|%v", resp.Node.Key, resp.Node.Value)
							continue
						}

						key := resp.Node.Key
						groupId := key[strings.LastIndex(key, "/")+1 : strings.LastIndex(key, "-bind")]
						//update local binds
						self.lock.Lock()
						gb, ok := self.binds[tmpTopic]
						if !ok {
							gb = make(map[string][]*bind.Binding, 2)
							self.binds[tmpTopic] = gb
						}
						gb[groupId] = binds
						self.lock.Unlock()

						//push bind data changed
						self.Watcher.DataChange(resp.Node.Key, binds)
						log.InfoLog("kiteq_registry", "BindWatcher|Watch|Bind|SUCC|%s|%v", resp.Node.Key, binds)
					}
				} else {
					log.InfoLog("kiteq_registry", "BindWatcher|Watch|SUCC|NOT MATCHED LOGIC |%s|%v", resp.Node.Key, resp.Action)
				}
			}
		}()
	}

}

func (self *BindWatcher) Stop() {
	self.cancelWatch = true
}

func traverseCreateEtcdPath(api client.KeysAPI, path string) {
	split := strings.Split(path, "/")[1:]
	tmppath := "/"
	for _, v := range split {
		tmppath += v
		// log.Printf("ZKManager|traverseCreatePath|%s\n", tmppath)
		api.Set(context.Background(), tmppath, "", &client.SetOptions{Dir: true})
		tmppath += "/"

	}
}
