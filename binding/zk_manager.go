package binding

import (
	"github.com/blackbeans/zk"
	"log"
	_ "net"
	"strings"
	"time"
)

const (
	KITEQ        = "/kiteq"
	KITEQ_SERVER = KITEQ + "/server" // 临时节点 # /kiteq/server/${topic}/ip:port
	KITEQ_PUB    = KITEQ + "/pub"    // 临时节点 # /kiteq/pub/${topic}/${groupId}/ip:port
	KITEQ_SUB    = KITEQ + "/sub"    // 持久订阅/或者临时订阅 # /kiteq/sub/${topic}/${groupId}-bind/#$data(bind)
)

type ZKManager struct {
	watcher   IWatcher
	zkwatcher chan zk.Event
	session   *zk.Session
	isClose   bool
}

type ZkEvent zk.EventType

const (
	Created ZkEvent = 1 // From Exists, Get NodeCreated (1),
	Deleted ZkEvent = 2 // From Exists, Get	NodeDeleted (2),
	Changed ZkEvent = 3 // From Exists, Get NodeDataChanged (3),
	Child   ZkEvent = 4 // From Children NodeChildrenChanged (4)
)

//每个watcher
type IWatcher interface {
	DataChange(path string, binds []*Binding)
	NodeChange(path string, eventType ZkEvent, children []string)
}

func NewZKManager(zkhosts string, watcher IWatcher) *ZKManager {
	if len(zkhosts) <= 0 {
		log.Println("使用默认zkhosts！|localhost:2181\n")
		zkhosts = "localhost:2181"
	} else {
		log.Printf("使用zkhosts:[%s]！\n", zkhosts)
	}

	conf := &zk.Config{Addrs: strings.Split(zkhosts, ","), Timeout: 5 * time.Second}

	ss, err := zk.Dial(conf)
	if nil != err {
		panic("连接zk失败..." + err.Error())
		return nil
	}

	exist, _, err := ss.Exists(KITEQ, nil)
	if nil != err {
		panic("无法创建KITEQ " + err.Error())
	}

	if !exist {
		resp, err := ss.Create(KITEQ, nil, zk.CreatePersistent, zk.AclOpen)
		if nil != err {
			panic("NewZKManager|CREATE ROOT PATH|FAIL|" + KITEQ + "|" + err.Error())
		} else {
			log.Printf("NewZKManager|CREATE ROOT PATH|SUCC|%s", resp)
		}
	}

	zkmanager := &ZKManager{session: ss, isClose: false, watcher: watcher, zkwatcher: make(chan zk.Event, 10)}
	go zkmanager.listenEvent()
	return zkmanager
}

//监听数据变更
func (self *ZKManager) listenEvent() {
	for !self.isClose {
		//根据zk的文档 watcher机制是无法保证可靠的，其次需要在每次处理完watcher后要重新注册watcher
		change := <-self.zkwatcher
		path := change.Path
		switch change.Type {
		case zk.Deleted:
			self.watcher.NodeChange(path, ZkEvent(change.Type), []string{})
			log.Printf("ZKManager|listenEvent|%s|%s\n", path, change)
		case zk.Created, zk.Child:
			childnodes, _, err := self.session.Children(path, self.zkwatcher)
			if nil != err {
				log.Printf("ZKManager|listenEvent|CD|%s|%s|%t\n", err, path, change.Type)
			} else {
				self.watcher.NodeChange(path, ZkEvent(change.Type), childnodes)
				log.Printf("ZKManager|listenEvent|%s|%s|%s\n", path, change, childnodes)
			}

		case zk.Changed:
			split := strings.Split(path, "/")
			//如果不是bind级别的变更则忽略
			if len(split) < 5 || strings.LastIndex(split[4], "-bind") <= 0 {
				self.session.Exists(path, self.zkwatcher)
				continue
			}
			//获取一下数据
			binds, err := self.getBindData(path, self.zkwatcher)
			if nil != err {
				log.Printf("ZKManager|listenEvent|Changed|Get DATA|FAIL|%s|%s\n", err, path)
				//忽略
				continue
			}
			self.watcher.DataChange(path, binds)
			log.Printf("ZKManager|listenEvent|%s|%s|%s\n", path, change, binds)

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
			log.Printf("ZKManager|UnpushlishQServer|FAIL|%s|%s/%s\n", err, qpath, hostport)
		} else {
			log.Printf("ZKManager|UnpushlishQServer|SUCC|%s/%s\n", qpath, hostport)
		}
	}
}

//发布topic对应的server
func (self *ZKManager) PublishQServer(hostport string, topics []string) error {

	for _, topic := range topics {

		qpath := KITEQ_SERVER + "/" + topic
		spath := KITEQ_SUB + "/" + topic
		ppath := KITEQ_PUB + "/" + topic

		//创建发送和订阅的根节点
		self.traverseCreatePath(ppath, nil, zk.CreatePersistent)
		self.addWatch(ppath)
		self.traverseCreatePath(spath, nil, zk.CreatePersistent)
		self.addWatch(spath)

		//注册当前节点
		path, err := self.registePath(qpath, hostport, zk.CreateEphemeral, nil)
		if nil != err {
			log.Printf("ZKManager|PublishQServer|FAIL|%s|%s/%s\n", err, qpath, hostport)
			return err
		}
		log.Printf("ZKManager|PublishQServer|SUCC|%s\n", path)
	}

	return nil
}

//发布可以使用的topic类型的publisher
func (self *ZKManager) PublishTopics(topics []string, groupId string, hostport string) error {

	for _, topic := range topics {
		pubPath := KITEQ_PUB + "/" + topic + "/" + groupId
		path, err := self.registePath(pubPath, hostport, zk.CreatePersistent, nil)
		if nil != err {
			log.Printf("ZKManager|PublishTopic|FAIL|%s|%s/%s\n", err, pubPath, hostport)
			return err
		}
		log.Printf("ZKManager|PublishTopic|SUCC|%s\n", path)
	}
	return nil
}

//发布订阅关系
func (self *ZKManager) PublishBindings(groupId string, bindings []*Binding) error {

	//按topic分组
	groupBind := make(map[string][]*Binding, 10)
	for _, b := range bindings {
		g, ok := groupBind[b.Topic]
		if !ok {
			g = make([]*Binding, 0, 2)
		}
		b.GroupId = groupId
		g = append(g, b)
		groupBind[b.Topic] = g
	}

	for topic, binds := range groupBind {
		data, err := MarshalBinds(binds)
		if nil != err {
			log.Printf("ZKManager|PublishBindings|MarshalBind|FAIL|%s|%s|%t\n", err, groupId, binds)
			return err
		}

		//如果为非持久订阅则直接注册临时节点
		createType := zk.CreatePersistent
		// if !binding.Persistent {
		// 	createType = zk.CreateEphemeral
		// }

		path := KITEQ_SUB + "/" + topic
		//注册对应topic的groupId //注册订阅信息
		succpath, err := self.registePath(path, groupId+"-bind", createType, data)
		if nil != err {
			log.Printf("ZKManager|PublishTopic|Bind|FAIL|%s|%s/%s\n", err, path, binds)
			return err
		} else {
			log.Printf("ZKManager|PublishTopic|Bind|SUCC|%s|%s\n", succpath, binds)
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
			log.Printf("ZKManager|registePath|CREATE CHILD|FAIL|%s|%s\n", err, path+"/"+childpath)
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
			log.Printf("ZKManager|traverseCreatePath|FAIL|%s\n", err)
			return err
		}
		tmppath += "/"

	}

	//对叶子节点创建及添加数据
	return self.innerCreatePath(tmppath, data, createType)
}

//内部创建节点的方法
func (self *ZKManager) innerCreatePath(tmppath string, data []byte, createType zk.CreateType) error {
	exist, _, err := self.session.Exists(tmppath, self.zkwatcher)
	if nil == err && !exist {
		_, err := self.session.Create(tmppath, data, createType, zk.AclOpen)
		if nil != err {
			log.Printf("ZKManager|innerCreatePath|FAIL|%s|%s\n", err, tmppath)
			return err
		}

		//做一下校验等待
		for i := 0; i < 5; i++ {
			exist, _, _ = self.session.Exists(tmppath, self.zkwatcher)
			if !exist {
				time.Sleep(time.Duration(i*100) * time.Millisecond)
			} else {
				break
			}
		}

		return err
	} else if nil != err {
		log.Printf("ZKManager|innerCreatePath|FAIL|%s\n", err)
		return err
	} else if nil != data {
		//存在该节点，推送新数据
		_, err := self.session.Set(tmppath, data, -1)
		if nil != err {
			log.Printf("ZKManager|innerCreatePath|PUSH DATA|FAIL|%s|%s|%s\n", err, tmppath, string(data))
			return err
		}
	}
	return nil
}

//获取QServer并添加watcher
func (self *ZKManager) GetQServerAndWatch(topic string) ([]string, error) {

	path := KITEQ_SERVER + "/" + topic

	exist, _, _ := self.session.Exists(path, self.zkwatcher)
	if !exist {
		return []string{}, nil
	}
	//获取topic下的所有qserver
	children, _, err := self.session.Children(path, self.zkwatcher)
	if nil != err {
		log.Printf("ZKManager|GetQServerAndWatch|FAIL|%s\n", path)
		return nil, err
	}
	return children, nil
}

//获取订阅关系并添加watcher
func (self *ZKManager) GetBindAndWatch(topic string) (map[string][]*Binding, error) {

	path := KITEQ_SUB + "/" + topic

	exist, _, err := self.session.Exists(path, self.zkwatcher)
	if !exist {
		//不存在订阅关系的时候需要创建该topic和
		return make(map[string][]*Binding, 0), err
	}

	//获取topic下的所有qserver
	groupIds, _, err := self.session.Children(path, self.zkwatcher)
	if nil != err {
		log.Printf("ZKManager|GetBindAndWatch|GroupID|FAIL|%s|%s\n", err, path)
		return nil, err
	}

	hps := make(map[string][]*Binding, len(groupIds))
	//获取topic对应的所有groupId下的订阅关系
	for _, groupId := range groupIds {
		tmppath := path + "/" + groupId
		binds, err := self.getBindData(tmppath, self.zkwatcher)
		if nil != err {
			log.Printf("GetBindAndWatch|getBindData|FAIL|%s|%s\n", tmppath, err)
			continue
		}

		//去掉分组后面的-bind
		gid := strings.TrimSuffix(groupId, "-bind")
		hps[gid] = binds
	}

	return hps, nil
}

//获取绑定对象的数据
func (self *ZKManager) getBindData(path string, zkwatcher chan zk.Event) ([]*Binding, error) {

	bindData, _, err := self.session.Get(path, zkwatcher)
	if nil != err {
		log.Printf("ZKManager|getBindData|Binding|FAIL|%s|%s\n", err, path)
		return nil, err
	}

	// log.Printf("ZKManager|getBindData|Binding|SUCC|%s|%s\n", path, string(bindData))
	if nil == bindData || len(bindData) <= 0 {
		return []*Binding{}, nil
	} else {
		binding, err := UmarshalBinds(bindData)
		if nil != err {
			log.Printf("ZKManager|getBindData|UmarshalBind|FAIL|%s|%s|%s\n", err, path, string(bindData))

		}
		return binding, err
	}

}

func (self *ZKManager) addWatch(path string) {
	self.session.Exists(path, self.zkwatcher)
}

func (self *ZKManager) Close() {
	self.session.Close()
	self.isClose = true
}
