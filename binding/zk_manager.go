package binding

import (
	"github.com/blackbeans/zk"
	"log"
	"strings"
	"time"
)

const (
	KITEQ        = "/kiteq/"
	KITEQ_SERVER = KITEQ + "server/" // 临时节点 # /kiteq/server/${topic}/ip:port
	KITEQ_PUB    = KITEQ + "pub/"    // 临时节点 # /kiteq/pub/${topic}/${groupId}/ip:port
	KITEQ_SUB    = KITEQ + "sub/"    // 持久订阅/或者临时订阅 # /kiteq/sub/${topic}/${groupId}/bind/#$data(bind)
)

type ZKManager struct {
	session *zk.Session
}

type ZkEvent zk.EventType

const (
	Created ZkEvent = 1 // From Exists, Get
	Deleted ZkEvent = 2 // From Exists, Get
	Changed ZkEvent = 3 // From Exists, Get
	Child   ZkEvent = 4 // From Children
)

//每个watcher
type IWatcher interface {
	EventNotify(path string, eventType ZkEvent)
	ChildWatcher(path string, childNode []string)
}

type Watcher struct {
	watcher   IWatcher
	zkwatcher chan zk.Event
	path      string
}

//创建一个watcher
func NewWatcher(path string, watcher IWatcher) *Watcher {
	zkwatcher := make(chan zk.Event, 10)
	return &Watcher{path: path, watcher: watcher, zkwatcher: zkwatcher}
}

func NewZKManager(zkhosts string) *ZKManager {
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
			panic("can't create flume root path ! " + err.Error())
		} else {
			log.Println("create flume root path succ ! " + resp)
		}
	}

	return &ZKManager{session: ss}
}

//发布topic对应的server
func (self *ZKManager) PublishQServer(hostport string, topics []string) error {
	for _, topic := range topics {
		path, err := self.registePath(KITEQ_SERVER, topic, zk.CreateEphemeral, nil)
		if nil != err {
			return err
		}

		path, err = self.registePath(path, hostport, zk.CreateEphemeral, nil)
		if nil != err {
			log.Printf("ZKManager|PublishServer|FAIL|%s|%s/%s\n", err, path, hostport)
			return err
		}
	}

	return nil
}

//发布可以使用的topic类型的publisher
func (self *ZKManager) PublishTopic(topics []string, groupId string, hostport string) error {
	for _, topic := range topics {
		path, err := self.registePath(KITEQ_PUB+topic, groupId, zk.CreateEphemeral, nil)
		if nil != err {
			return err
		}

		path, err = self.registePath(path, hostport, zk.CreateEphemeral, nil)
		if nil != err {
			log.Printf("ZKManager|PublishTopic|FAIL|%s|%s/%s\n", err, path, hostport)
			return err
		}
	}
	return nil
}

//订阅消息类型
func (self *ZKManager) SubscribeTopic(groupId string, bindings []*Binding) error {
	for _, binding := range bindings {
		data, err := MarshalBind(binding)
		if nil != err {
			log.Printf("ZKManager|SubscribeTopic|MarshalBind|FAIL|%s|%s|%t\n", err, groupId, binding)
			return err
		}

		//如果为非持久订阅则直接注册临时节点
		createType := zk.CreatePersistent
		if !binding.Persistent {
			createType = zk.CreateEphemeral
		}

		//注册对应topic的groupId
		path, err := self.registePath(KITEQ_SUB+binding.Topic, binding.GroupId, createType, nil)
		if nil != err {
			log.Printf("ZKManager|PublishTopic|GroupId|FAIL|%s|%s/%s\n", err, path, groupId)
			return err
		}

		//注册订阅信息
		path, err = self.registePath(path, "bind", createType, data)
		if nil != err {
			log.Printf("ZKManager|PublishTopic|Bind|FAIL|%s|%s/%s\n", err, path, binding)
			return err
		}
	}
	return nil
}

//注册当前进程节点
func (self *ZKManager) registePath(path string, childpath string, createType zk.CreateType, data []byte) (string, error) {
	err := self.traverseCreatePath(path)
	if nil == err {
		resp, err := self.session.Create(path+"/"+childpath, data, createType, zk.AclOpen)
		if nil != err {
			log.Printf("ZKManager|CREATE NODE|FAIL|%s|%s/%s|%s|%t\n", err, path, childpath, createType, data)
		} else {
			log.Printf("ZKManager|CREATE NODE|SUCC|%s/%s|%s|%t\n", path, childpath, createType, data)
		}
		return resp, err
	}
	return "", err

}

func (self *ZKManager) traverseCreatePath(path string) error {
	split := strings.SplitN(path, "/", 3)
	log.Println(split)
	tmppath := ""
	for _, v := range split {
		tmppath += v
		exist, _, err := self.session.Exists(tmppath, nil)
		if nil == err && !exist {
			self.session.Create(tmppath, nil, zk.CreatePersistent, zk.AclOpen)
			return nil
		} else if nil != err {
			log.Printf("ZKManager|traverseCreatePath|FAIL|%s\n", err.Error())
			return err
		}
		tmppath += "/"
	}

	return nil
}

//获取QServer并添加watcher
func (self *ZKManager) GetQServerAndWatch(topic string, nwatcher *Watcher) ([]string, error) {

	path := KITEQ_SERVER + topic
	//获取topic下的所有qserver
	children, _, err := self.session.Children(path, nwatcher.zkwatcher)
	if nil != err {
		log.Printf("ZKManager|GetQServerAndWatch|FAIL|%s\n", path)
		return nil, err
	}

	//增加监听
	self.addWatch(path, nwatcher)
	return children, nil
}

//获取订阅关系并添加watcher
func (self *ZKManager) GetBindAndWatch(topic string, nwatcher *Watcher) ([]*Binding, error) {

	path := KITEQ_SUB + topic
	//获取topic下的所有qserver
	groupIds, _, err := self.session.Children(path, nwatcher.zkwatcher)
	if nil != err {
		log.Printf("ZKManager|GetBindAndWatch|GroupID|FAIL|%s\n", path)
		return nil, err
	}

	hps := make([]*Binding, 0, len(groupIds))
	//获取topic对应的所有groupId下的订阅关系
	for _, groupId := range groupIds {
		path += "/" + groupId + "/bind"

		bindData, _, err := self.session.Get(path, nwatcher.zkwatcher)
		//增加监听
		self.addWatch(path, nwatcher)
		if nil != err {
			log.Printf("ZKManager|GetBindAndWatch|Binding|FAIL|%s|%s\n", err, path)
			continue
		}

		binding, err := UmarshalBind(bindData)
		if nil != err {
			log.Printf("ZKManager|GetBindAndWatch|UmarshalBind|FAIL|%s|%s|%s\n", err, path, string(bindData))
			continue
		}

		hps = append(hps, binding)
	}

	return hps, nil
}

func (self *ZKManager) addWatch(path string, nwatcher *Watcher) {

	//监听数据变更
	go func() {
		for {
			//根据zk的文档 watcher机制是无法保证可靠的，其次需要在每次处理完watcher后要重新注册watcher
			change := <-nwatcher.zkwatcher
			switch change.Type {
			case zk.Created:
				self.session.Exists(path, nwatcher.zkwatcher)
				nwatcher.watcher.EventNotify(path, Created)
			case zk.Deleted:
				self.session.Exists(path, nwatcher.zkwatcher)
				nwatcher.watcher.EventNotify(path, Deleted)

			case zk.Changed:
				self.session.Exists(path, nwatcher.zkwatcher)
				nwatcher.watcher.EventNotify(path, Changed)

			case zk.Child:
				self.session.Children(path, nwatcher.zkwatcher)
				//子节点发生变更，则获取全新的子节点
				childnodes, _, err := self.session.Children(path, nil)
				if nil != err {
					log.Println("recieve child's changes fail ! [" + path + "]  " + err.Error())
				} else {
					log.Printf("%s|child's changed %s", path, childnodes)
					nwatcher.watcher.ChildWatcher(path, childnodes)
				}
			}
		}
		log.Printf("ZKManager|addWatch|FAIL|out of wacher range ! [%s]\n", path)
	}()
}

func (self *ZKManager) Close() {
	self.session.Close()
}
