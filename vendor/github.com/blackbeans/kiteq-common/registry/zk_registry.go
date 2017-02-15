package registry

import (
	"github.com/blackbeans/go-zookeeper/zk"
	"github.com/blackbeans/kiteq-common/registry/bind"
	log "github.com/blackbeans/log4go"
	_ "net"
	"strings"
	"time"
)

type ZKManager struct {
	zkhosts   string
	wathcers  map[string]IWatcher //基本的路径--->watcher zk可以复用了
	session   *zk.Conn
	eventChan <-chan zk.Event
	isClose   bool
}

func NewZKManager(zkhosts string) *ZKManager {
	zkmanager := &ZKManager{zkhosts: zkhosts, wathcers: make(map[string]IWatcher, 10)}
	return zkmanager
}

func (self *ZKManager) Start() {
	if len(self.zkhosts) <= 0 {
		log.WarnLog("kiteq_registry", "使用默认zkhosts！|localhost:2181\n")
		self.zkhosts = "localhost:2181"
	} else {
		log.Info("使用zkhosts:[%s]！\n", self.zkhosts)
	}

	ss, eventChan, err := zk.Connect(strings.Split(self.zkhosts, ","), 5*time.Second)
	if nil != err {
		panic("连接zk失败..." + err.Error())
		return
	}

	exist, _, err := ss.Exists(KITEQ)
	if nil != err {
		ss.Close()
		panic("无法创建KITEQ " + err.Error())

	}

	if !exist {
		resp, err := ss.Create(KITEQ, nil, zk.CreatePersistent, zk.WorldACL(zk.PermAll))
		if nil != err {
			ss.Close()
			panic("NewZKManager|CREATE ROOT PATH|FAIL|" + KITEQ + "|" + err.Error())
		} else {
			log.InfoLog("kiteq_registry", "NewZKManager|CREATE ROOT PATH|SUCC|%s", resp)
		}
	}

	self.session = ss
	self.isClose = false
	self.eventChan = eventChan
	go self.listenEvent()
}

//如果返回false则已经存在
func (self *ZKManager) RegisteWatcher(rootpath string, w IWatcher) bool {
	_, ok := self.wathcers[rootpath]
	if ok {
		return false
	} else {
		self.wathcers[rootpath] = w
		return true
	}
}

//监听数据变更
func (self *ZKManager) listenEvent() {
	for !self.isClose {

		//根据zk的文档 Watcher机制是无法保证可靠的，其次需要在每次处理完Watcher后要重新注册Watcher
		change := <-self.eventChan
		path := change.Path
		//开始检查符合的watcher
		watcher := func() IWatcher {
			for k, w := range self.wathcers {
				//以给定的
				if strings.Index(path, k) >= 0 {

					return w
				}
			}
			return nil
		}()

		//如果没有wacher那么久忽略
		if nil == watcher {
			log.WarnLog("kiteq_registry", "ZKManager|listenEvent|NO  WATCHER|%s", path)
			continue
		}

		switch change.Type {
		case zk.EventSession:
			if change.State == zk.StateExpired ||
				change.State == zk.StateDisconnected {
				log.WarnLog("kiteq_registry", "ZKManager|OnSessionExpired!|Reconnect Zk ....")

				//session失效必须通知所有的watcher
				func() {
					for _, w := range self.wathcers {
						//zk链接开则需要重新链接重新推送
						w.OnSessionExpired()
					}
				}()

			}
		case zk.EventNodeDeleted:
			self.session.ExistsW(path)
			watcher.NodeChange(path, RegistryEvent(change.Type), []string{})
			// log.Info("ZKManager|listenEvent|%s|%s\n", path, change)
		case zk.EventNodeCreated, zk.EventNodeChildrenChanged:
			childnodes, _, _, err := self.session.ChildrenW(path)
			if nil != err {
				log.ErrorLog("kiteq_registry", "ZKManager|listenEvent|CD|%s|%s|%t\n", err, path, change.Type)
			} else {
				watcher.NodeChange(path, RegistryEvent(change.Type), childnodes)
				// log.Info("ZKManager|listenEvent|%s|%s|%s\n", path, change, childnodes)
			}

		case zk.EventNodeDataChanged:
			split := strings.Split(path, "/")
			//如果不是bind级别的变更则忽略
			if len(split) < 5 || strings.LastIndex(split[4], "-bind") <= 0 {
				continue
			}
			//获取一下数据
			binds, err := self.getBindData(path)
			if nil != err {
				log.ErrorLog("kiteq_registry", "ZKManager|listenEvent|Changed|Get DATA|FAIL|%s|%s\n", err, path)
				//忽略
				continue
			}
			watcher.DataChange(path, binds)
			// log.Info("ZKManager|listenEvent|%s|%s|%s\n", path, change, binds)

		}

	}

}

//去除掉当前的KiteQServer
func (self *ZKManager) UnpushlishQServer(hostport string, topics []string) {
	for _, topic := range topics {

		qpath := KITEQ_SERVER + "/" + topic + "/" + hostport

		//删除当前该Topic下的本机
		err := self.session.Delete(qpath, -1)
		if nil != err {
			log.ErrorLog("kiteq_registry", "ZKManager|UnpushlishQServer|FAIL|%s|%s\n", err, qpath)
		} else {
			log.InfoLog("kiteq_registry", "ZKManager|UnpushlishQServer|SUCC|%s\n", qpath)
		}
	}
	//注册当前的kiteqserver
	self.session.Delete(KITEQ_ALIVE_SERVERS+"/"+hostport, -1)
}

//发布topic对应的server
func (self *ZKManager) PublishQServer(hostport string, topics []string) error {

	for _, topic := range topics {
		qpath := KITEQ_SERVER + "/" + topic
		spath := KITEQ_SUB + "/" + topic
		ppath := KITEQ_PUB + "/" + topic

		//创建发送和订阅的根节点
		self.traverseCreatePath(ppath, nil, zk.CreatePersistent)
		// self.session.ExistsW(ppath)
		self.traverseCreatePath(spath, nil, zk.CreatePersistent)
		self.session.ExistsW(spath)

		//先删除当前这个临时节点再注册 避免监听不到临时节点变更的事件
		self.session.Delete(qpath+"/"+hostport, -1)

		//注册当前节点
		path, err := self.registePath(qpath, hostport, zk.CreateEphemeral, nil)
		if nil != err {
			log.ErrorLog("kiteq_registry", "ZKManager|PublishQServer|FAIL|%s|%s/%s\n", err, qpath, hostport)
			return err
		}
		log.InfoLog("kiteq_registry", "ZKManager|PublishQServer|SUCC|%s\n", path)
	}

	//注册当前的kiteqserver
	self.session.Delete(KITEQ_ALIVE_SERVERS+"/"+hostport, -1)
	self.registePath(KITEQ_ALIVE_SERVERS, hostport, zk.CreateEphemeral, nil)
	self.registePath(KITEQ_ALL_SERVERS, hostport, zk.CreatePersistent, nil)
	return nil
}

//发布可以使用的topic类型的publisher
func (self *ZKManager) PublishTopics(topics []string, groupId string, hostport string) error {

	for _, topic := range topics {
		pubPath := KITEQ_PUB + "/" + topic + "/" + groupId
		path, err := self.registePath(pubPath, hostport, zk.CreatePersistent, nil)
		if nil != err {
			log.ErrorLog("kiteq_registry", "ZKManager|PublishTopic|FAIL|%s|%s/%s\n", err, pubPath, hostport)
			return err
		}
		log.InfoLog("kiteq_registry", "ZKManager|PublishTopic|SUCC|%s\n", path)
	}
	return nil
}

//发布订阅关系
func (self *ZKManager) PublishBindings(groupId string, bindings []*bind.Binding) error {

	//按topic分组
	groupBind := make(map[string][]*bind.Binding, 10)
	for _, b := range bindings {
		g, ok := groupBind[b.Topic]
		if !ok {
			g = make([]*bind.Binding, 0, 2)
		}
		b.GroupId = groupId
		g = append(g, b)
		groupBind[b.Topic] = g
	}

	for topic, binds := range groupBind {
		data, err := bind.MarshalBinds(binds)
		if nil != err {
			log.ErrorLog("kiteq_registry", "ZKManager|PublishBindings|MarshalBind|FAIL|%s|%s|%t\n", err, groupId, binds)
			return err
		}

		createType := zk.CreatePersistent

		path := KITEQ_SUB + "/" + topic
		//注册对应topic的groupId //注册订阅信息
		succpath, err := self.registePath(path, groupId+"-bind", createType, data)
		if nil != err {
			log.ErrorLog("kiteq_registry", "ZKManager|PublishTopic|Bind|FAIL|%s|%s/%s\n", err, path, binds)
			return err
		} else {
			log.InfoLog("kiteq_registry", "ZKManager|PublishTopic|Bind|SUCC|%s|%s\n", succpath, binds)
		}
	}
	return nil
}

//注册当前进程节点
func (self *ZKManager) registePath(path string, childpath string, createType zk.CreateType, data []byte) (string, error) {
	err := self.traverseCreatePath(path, nil, zk.CreatePersistent)
	if nil == err {
		err := self.innerCreatePath(path+"/"+childpath, data, createType)
		if nil != err {
			log.ErrorLog("kiteq_registry", "ZKManager|registePath|CREATE CHILD|FAIL|%s|%s\n", err, path+"/"+childpath)
			return "", err
		} else {
			return path + "/" + childpath, nil
		}
	}
	return "", err

}

func (self *ZKManager) traverseCreatePath(path string, data []byte, createType zk.CreateType) error {
	split := strings.Split(path, "/")[1:]
	tmppath := "/"
	for i, v := range split {
		tmppath += v
		// log.Printf("ZKManager|traverseCreatePath|%s\n", tmppath)
		if i >= len(split)-1 {
			break
		}
		err := self.innerCreatePath(tmppath, nil, zk.CreatePersistent)
		if nil != err {
			log.ErrorLog("kiteq_registry", "ZKManager|traverseCreatePath|FAIL|%s\n", err)
			return err
		}
		tmppath += "/"

	}

	//对叶子节点创建及添加数据
	return self.innerCreatePath(tmppath, data, createType)
}

//内部创建节点的方法
func (self *ZKManager) innerCreatePath(tmppath string, data []byte, createType zk.CreateType) error {
	exist, _, _, err := self.session.ExistsW(tmppath)
	if nil == err && !exist {
		_, err := self.session.Create(tmppath, data, createType, zk.WorldACL(zk.PermAll))
		if nil != err {
			log.ErrorLog("kiteq_registry", "ZKManager|innerCreatePath|FAIL|%s|%s\n", err, tmppath)
			return err
		}

		//做一下校验等待
		for i := 0; i < 5; i++ {
			exist, _, _ = self.session.Exists(tmppath)
			if !exist {
				time.Sleep(time.Duration(i*100) * time.Millisecond)
			} else {
				break
			}
		}

		return err
	} else if nil != err {
		log.ErrorLog("kiteq_registry", "ZKManager|innerCreatePath|FAIL|%s\n", err)
		return err
	} else if nil != data {
		//存在该节点，推送新数据
		_, err := self.session.Set(tmppath, data, -1)
		if nil != err {
			log.ErrorLog("kiteq_registry", "ZKManager|innerCreatePath|PUSH DATA|FAIL|%s|%s|%s\n", err, tmppath, string(data))
			return err
		}
	}
	return nil
}

//获取QServer并添加watcher
func (self *ZKManager) GetQServerAndWatch(topic string) ([]string, error) {

	path := KITEQ_SERVER + "/" + topic

	exist, _, _, _ := self.session.ExistsW(path)
	if !exist {
		return []string{}, nil
	}
	//获取topic下的所有qserver
	children, _, _, err := self.session.ChildrenW(path)
	if nil != err {
		log.ErrorLog("kiteq_registry", "ZKManager|GetQServerAndWatch|FAIL|%s\n", path)
		return nil, err
	}
	return children, nil
}

//获取订阅关系并添加watcher
func (self *ZKManager) GetBindAndWatch(topic string) (map[string][]*bind.Binding, error) {

	path := KITEQ_SUB + "/" + topic

	exist, _, _, err := self.session.ExistsW(path)
	if !exist {
		//不存在订阅关系的时候需要创建该topic和
		return make(map[string][]*bind.Binding, 0), err
	}

	//获取topic下的所有qserver
	groupIds, _, _, err := self.session.ChildrenW(path)
	if nil != err {
		log.ErrorLog("kiteq_registry", "ZKManager|GetBindAndWatch|GroupID|FAIL|%s|%s\n", err, path)
		return nil, err
	}

	hps := make(map[string][]*bind.Binding, len(groupIds))
	//获取topic对应的所有groupId下的订阅关系
	for _, groupId := range groupIds {
		tmppath := path + "/" + groupId
		binds, err := self.getBindData(tmppath)
		if nil != err {
			log.ErrorLog("kiteq_registry", "GetBindAndWatch|getBindData|FAIL|%s|%s\n", tmppath, err)
			continue
		}

		//去掉分组后面的-bind
		gid := strings.TrimSuffix(groupId, "-bind")
		hps[gid] = binds
	}

	return hps, nil
}

//获取绑定对象的数据
func (self *ZKManager) getBindData(path string) ([]*bind.Binding, error) {
	bindData, _, _, err := self.session.GetW(path)
	if nil != err {
		log.ErrorLog("kiteq_registry", "ZKManager|getBindData|Binding|FAIL|%s|%s\n", err, path)
		return nil, err
	}

	// log.Printf("ZKManager|getBindData|Binding|SUCC|%s|%s\n", path, string(bindData))
	if nil == bindData || len(bindData) <= 0 {
		return []*bind.Binding{}, nil
	} else {
		binding, err := bind.UmarshalBinds(bindData)
		if nil != err {
			log.ErrorLog("kiteq_registry", "ZKManager|getBindData|UmarshalBind|FAIL|%s|%s|%s\n", err, path, string(bindData))

		}
		return binding, err
	}

}

func (self *ZKManager) Close() {
	self.isClose = true
	self.session.Close()
}
