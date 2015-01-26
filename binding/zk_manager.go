package binding

import (
	"github.com/blackbeans/zk"
	"log"
	"strconv"
	"strings"
	"time"
)

const (
	FLUME_PATH            = "/kiteQ/"
	FLUME_SOURCE_PATH_PID = "/flume_source"
)

type HostPort struct {
	Host string
	Port int
}

func NewHostPort(hp string) HostPort {
	hparr := strings.Split(hp, ":")
	port, _ := strconv.Atoi(hparr[1])
	return HostPort{Host: hparr[0], Port: port}
}

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
	BusinessWatcher(business string, eventType ZkEvent)
	ChildWatcher(business string, childNode []HostPort)
}

type Watcher struct {
	watcher   IWatcher
	zkwatcher chan zk.Event
	business  string
}

//创建一个watcher
func NewWatcher(business string, watcher IWatcher) *Watcher {
	zkwatcher := make(chan zk.Event, 10)
	return &Watcher{business: business, watcher: watcher, zkwatcher: zkwatcher}
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

	exist, _, err := ss.Exists(FLUME_PATH, nil)
	if nil != err {
		panic("无法创建flume_path " + err.Error())
	}

	if !exist {

		resp, err := ss.Create(FLUME_PATH, nil, zk.CreatePersistent, zk.AclOpen)
		if nil != err {
			panic("can't create flume root path ! " + err.Error())
		} else {
			log.Println("create flume root path succ ! " + resp)
		}
	}

	return &ZKManager{session: ss}
}

//注册当前进程节点
func (self *ZKManager) RegistePath(businesses []string, childpath string) {
	for _, business := range businesses {
		tmppath := FLUME_SOURCE_PATH_PID + "/" + business
		err := self.traverseCreatePath(tmppath)
		if nil == err {
			resp, err := self.session.Create(tmppath+"/"+childpath, nil, zk.CreateEphemeral, zk.AclOpen)
			if nil != err {
				log.Println("can't create flume source path [" + resp + "]," + err.Error())
			} else {
				log.Println("create flume source path :[" + tmppath + "/" + childpath + "]")
			}
		}
	}
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
			log.Println("traverseCreatePath|fail|" + err.Error())
			return err
		}
		tmppath += "/"
	}

	return nil
}

func (self *ZKManager) GetAndWatch(business string, nwatcher *Watcher) []HostPort {

	path := FLUME_PATH + "/" + business
	exist, _, err := self.session.Exists(path, nwatcher.zkwatcher)

	//存在该节点
	if !exist && nil == err {
		resp, err := self.session.Create(path, nil, zk.CreatePersistent, zk.AclOpen)
		if nil != err {
			log.Println("can't create flume path " + path + "," + err.Error())
			return nil
		} else {
			log.Println("create flume path succ ! " + resp)
		}
	} else if nil != err {
		log.Println("can't create path [" + path + "]!")
		return nil
	}

	childnodes, _, err := self.session.Children(path, nwatcher.zkwatcher)
	if nil != err {
		log.Println("get data from [" + path + "] fail! " + err.Error())
		return nil
	}

	//赋值新的Node
	flumenodes := self.DecodeNode(childnodes)

	//监听数据变更
	go func() {
		for {
			//根据zk的文档 watcher机制是无法保证可靠的，其次需要在每次处理完watcher后要重新注册watcher
			change := <-nwatcher.zkwatcher
			switch change.Type {
			case zk.Created:
				self.session.Exists(path, nwatcher.zkwatcher)
				nwatcher.watcher.BusinessWatcher(business, Created)

			case zk.Deleted:
				self.session.Exists(path, nwatcher.zkwatcher)
				nwatcher.watcher.BusinessWatcher(business, Deleted)

			case zk.Changed:
				self.session.Exists(path, nwatcher.zkwatcher)
				nwatcher.watcher.BusinessWatcher(business, Changed)

			case zk.Child:
				self.session.Children(path, nwatcher.zkwatcher)
				//子节点发生变更，则获取全新的子节点
				childnodes, _, err := self.session.Children(path, nil)
				if nil != err {
					log.Println("recieve child's changes fail ! [" + path + "]  " + err.Error())
				} else {
					log.Printf("%s|child's changed %s", path, childnodes)
					nwatcher.watcher.ChildWatcher(business, self.DecodeNode(childnodes))
				}
			}
		}
		log.Printf("out of wacher range ! [%s]\n", path)
	}()

	return flumenodes
}

func (self *ZKManager) DecodeNode(paths []string) []HostPort {

	//由小到大排序
	flumehost := make([]HostPort, 0)
	//选取出一半数量最小的Host作为master
	for _, path := range paths {
		split := strings.Split(path, "_")
		hostport := split[0] + ":" + split[1]
		hp := NewHostPort(hostport)
		flumehost = append(flumehost, hp)
	}

	log.Println("running node :%s", flumehost)
	return flumehost
}

func (self *ZKManager) Close() {
	self.session.Close()
}
