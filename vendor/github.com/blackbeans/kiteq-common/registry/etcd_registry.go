package registry

import (
	"container/list"
	"github.com/blackbeans/kiteq-common/registry/bind"
	log "github.com/blackbeans/log4go"
	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"strings"
	"time"
)

type EtcdRegistry struct {
	config      client.Config
	session     client.Client
	api         client.KeysAPI
	wathcers    map[string]IWatcher //基本的路径--->watcher zk可以复用了
	workerlink  *list.List
	watcherlink *list.List
	isClosed    bool
}

func NewEtcdRegistry(hosts string) *EtcdRegistry {

	ectdAp := strings.Split(hosts, ",")
	cfg := client.Config{
		Endpoints: ectdAp,
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: 5 * time.Second}

	return &EtcdRegistry{config: cfg, workerlink: list.New(),
		wathcers: make(map[string]IWatcher, 5), watcherlink: list.New(), isClosed: false}

}

func (self *EtcdRegistry) Start() {

	etcConn, err := client.New(self.config)
	if nil != err {
		panic(err)
	}

	api := client.NewKeysAPI(etcConn)
	self.session = etcConn
	self.api = api

	//create root

	_, err = self.api.Set(context.Background(), KITEQ, "", &client.SetOptions{Dir: true})
	if nil != err {
		log.ErrorLog("kiteq_registry", "QClientWorker|Start|FAIL|%s|%s", KITEQ, err)
	}

	_, err = self.api.Set(context.Background(), KITEQ_PUB, "", &client.SetOptions{Dir: true})
	if nil != err {
		log.ErrorLog("kiteq_registry", "QClientWorker|Start|FAIL|%s|%s", KITEQ_PUB, err)
	}
	_, err = self.api.Set(context.Background(), KITEQ_SUB, "", &client.SetOptions{Dir: true})
	if nil != err {
		log.ErrorLog("kiteq_registry", "QClientWorker|Start|FAIL|%s|%s", KITEQ_SUB, err)
	}
	go self.heartbeat()
	go self.checkAlive()

}

func (self *EtcdRegistry) checkAlive() {
	for !self.isClosed {
		for e := self.workerlink.Back(); nil != e; e = e.Next() {
			w := e.Value.(Worker)
			w.Keepalive()
		}
		time.Sleep(1 * time.Second)
	}
}

func (self *EtcdRegistry) heartbeat() {
	for !self.isClosed {
		err := self.session.AutoSync(context.Background(), 10*time.Second)
		if err == context.DeadlineExceeded || err == context.Canceled {
			break
		}
	}

}

// //如果返回false则已经存在
func (self *EtcdRegistry) RegisteWatcher(rootpath string, w IWatcher) bool {
	_, ok := self.wathcers[rootpath]
	if ok {
		return false
	}
	self.wathcers[rootpath] = w
	return true

}

//去除掉当前的KiteQServer
func (self *EtcdRegistry) UnpushlishQServer(hostport string, topics []string) {
	for _, topic := range topics {

		qpath := KITEQ_SERVER + "/" + topic + "/" + hostport

		//删除当前该Topic下的本机
		self.api.Delete(context.Background(), qpath, &client.DeleteOptions{Recursive: true})

	}
	//取消当前的kiteqserver
	self.api.Delete(context.Background(), KITEQ_ALIVE_SERVERS+"/"+hostport, &client.DeleteOptions{Recursive: true})
}

//发布topic对应的server
func (self *EtcdRegistry) PublishQServer(hostport string, topics []string) error {

	worker := &QServerWroker{Api: self.api, Topics: topics,
		KeepalivePeriod: 2 * time.Second, Hostport: hostport}
	worker.Start()

	//offer worker
	self.workerlink.PushBack(worker)
	return nil
}

//发布可以使用的topic类型的publisher
func (self *EtcdRegistry) PublishTopics(topics []string, groupId string, hostport string) error {
	// Api             client.KeysAPI
	// PublishTopics   []string
	// Hostport        string
	// GroupId         string
	// Bindings        []*bind.Binding
	// KeepalivePeriod time.Duration
	worker := &QClientWorker{Api: self.api, PublishTopics: topics, Hostport: hostport,
		GroupId: groupId, Bindings: []*bind.Binding{}, KeepalivePeriod: 5 * time.Second}

	self.workerlink.PushBack(worker)
	return nil
}

//获取QServer并添加watcher
func (self *EtcdRegistry) GetQServerAndWatch(topic string) ([]string, error) {

	path := KITEQ_SERVER + "/" + topic

	for k, watcher := range self.wathcers {
		if strings.HasPrefix(path, k) {
			qw := &QServersWatcher{Api: self.api, Topics: []string{topic}, Watcher: watcher, cancelWatch: false}
			qw.Watch()
			self.watcherlink.PushBack(qw)
		}
	}

	//pull servers
	resp, err := self.api.Get(context.Background(), path, &client.GetOptions{Recursive: false})
	if nil != err {
		return nil, err
	}

	//get childnodes
	children := make([]string, 0, 5)
	for _, n := range resp.Node.Nodes {
		ipport := n.Key[strings.LastIndex(n.Key, "/")+1:]
		children = append(children, ipport)
	}

	return children, nil
}

//发布订阅关系
func (self *EtcdRegistry) PublishBindings(groupId string, bindings []*bind.Binding) error {
	// Api             client.KeysAPI
	// PublishTopics   []string
	// Hostport        string
	// GroupId         string
	// Bindings        []*bind.Binding
	// KeepalivePeriod time.Duration
	worker := &QClientWorker{Api: self.api, PublishTopics: []string{},
		GroupId: groupId, Bindings: bindings, KeepalivePeriod: 1 * time.Hour}
	worker.Start()

	self.workerlink.PushBack(worker)
	return nil
}

//获取订阅关系并添加watcher
func (self *EtcdRegistry) GetBindAndWatch(topic string) (map[string][]*bind.Binding, error) {

	path := KITEQ_SUB + "/" + topic

	//scan and attach watcher
	for k, watcher := range self.wathcers {
		if strings.HasPrefix(path, k) {
			// Api     client.KeysAPI
			// Topics  []string
			// Watcher IWatcher
			qw := &BindWatcher{Api: self.api,
				Topics:      []string{topic},
				Watcher:     watcher,
				cancelWatch: false}
			qw.Watch()
			self.watcherlink.PushBack(qw)
		}
	}

	//pull servers
	resp, err := self.api.Get(context.Background(), path, &client.GetOptions{Recursive: false})
	if nil != err {
		return nil, err
	}

	//get bind group
	hps := make(map[string][]*bind.Binding, resp.Node.Nodes.Len())
	//获取topic对应的所有groupId下的订阅关系
	for _, n := range resp.Node.Nodes {
		binds, err := self.getBindData(n.Key)
		if nil != err {
			log.ErrorLog("kite_bind", "GetBindAndWatch|getBindData|FAIL|%s|%s\n", n.Key, err)
			continue
		}

		//去掉分组后面的-bind
		gid := strings.TrimSuffix(n.Key, "-bind")
		hps[gid] = binds
	}

	return hps, nil
}

// //获取绑定对象的数据
func (self *EtcdRegistry) getBindData(path string) ([]*bind.Binding, error) {
	resp, err := self.api.Get(context.Background(), path, &client.GetOptions{Recursive: false})
	if nil != err {
		log.ErrorLog("kite_bind", "EtcdRegistry|getBindData|Binding|FAIL|%s|%s\n", err, path)
		return nil, err
	}

	if len(resp.Node.Value) <= 0 {
		return []*bind.Binding{}, nil
	} else {
		binding, err := bind.UmarshalBinds([]byte(resp.Node.Value))
		if nil != err {
			log.ErrorLog("kite_bind", "EtcdRegistry|getBindData|UmarshalBind|FAIL|%s|%s|%s\n", err, path, resp.Node.Value)

		}
		return binding, err
	}

}

func (self *EtcdRegistry) Close() {
	self.isClosed = true
	for e := self.watcherlink.Back(); nil != e; e = e.Prev() {
		e.Prev().Value.(EtcdWatcher).Stop()
	}
}
