package binding

import (
	"log"
	"sort"
	"strings"
	"sync"
)

//用于管理订阅关系，对接zookeeper的订阅关系变更
type BindExchanger struct {
	exchanger   map[string] /*topic*/ map[string] /*groupId*/ []*Binding //保存的订阅关系
	topics      []string                                                 //当前服务器可投递的topic类型
	lock        sync.RWMutex
	zkmanager   *ZKManager
	kiteqserver string
}

func NewBindExchanger(zkhost string, kiteQServer string) *BindExchanger {

	ex := &BindExchanger{
		exchanger: make(map[string]map[string][]*Binding, 100),
		topics:    make([]string, 0, 50)}
	zkmanager := NewZKManager(zkhost, ex)
	ex.zkmanager = zkmanager
	ex.kiteqserver = kiteQServer
	return ex
}

//推送Qserver到配置中心
func (self *BindExchanger) PushQServer(hostport string, topics []string) bool {
	err := self.zkmanager.PublishQServer(hostport, topics)
	if nil != err {
		log.Printf("BindExchanger|PushQServer|FAIL|%s|%s|%s\n", err, hostport, topics)
		return false
	}
	log.Printf("BindExchanger|PushQServer|SUCC|%s|%s\n", hostport, topics)
	self.topics = append(self.topics, topics...)
	sort.Strings(self.topics)

	//订阅订阅关系变更
	return self.subscribeBinds(self.topics)
}

//监听topics的对应的订阅关系的变更
func (self *BindExchanger) subscribeBinds(topics []string) bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	for _, topic := range topics {
		binds, err := self.zkmanager.GetBindAndWatch(topic)
		if nil != err {
			log.Printf("BindExchanger|SubscribeBinds|FAIL|%s|%s\n", err, topic)
			return false
		} else {
			for groupId, bs := range binds {
				self.onBindChanged(topic, groupId, bs)
				log.Printf("BindExchanger|SubscribeBinds|SUCC|%s\n", binds)
			}
		}
	}

	return true
}

//根据topic和messageType 类型获取订阅关系
func (self *BindExchanger) FindBinds(topic string, messageType string, filter func(b *Binding) bool) []*Binding {
	self.lock.RLock()
	defer self.lock.RUnlock()
	groups, ok := self.exchanger[topic]
	if !ok {
		return []*Binding{}
	}

	//符合规则的binds
	validBinds := make([]*Binding, 0, 10)
	for _, binds := range groups {
		for _, b := range binds {
			//匹配并且不被过滤
			if b.matches(topic, messageType) && !filter(b) {
				validBinds = append(validBinds, b)
			}
		}
	}

	return validBinds
}

//订阅关系topic下的group发生变更
func (self *BindExchanger) NodeChange(path string, eventType ZkEvent, childNode []string) {

	//如果是订阅关系变更则处理
	if strings.HasPrefix(path, KITEQ_SUB) {
		//获取topic
		split := strings.Split(path, "/")
		if len(split) < 4 {
			//不合法的订阅璐姐
			log.Printf("BindExchanger|NodeChange|INVALID SUB PATH |%s|%t\n", path, childNode)
			return
		}
		//获取topic
		topic := split[3]

		self.lock.Lock()
		defer self.lock.Unlock()
		//如果topic下无订阅分组节点，直接删除该topic
		if len(childNode) <= 0 {
			self.onBindChanged(topic, "", nil)
			log.Printf("BindExchanger|NodeChange|无子节点|%s|%s\n", path, childNode)
			return
		}

		// //对当前的topic的分组进行重新设置
		switch eventType {
		case Created, Child:

			bm, err := self.zkmanager.GetBindAndWatch(topic)
			if nil != err {
				log.Printf("BindExchanger|NodeChange|获取订阅关系失败|%s|%s\n", path, childNode)
			}

			for groupId, bs := range bm {
				self.onBindChanged(topic, groupId, bs)
			}
		}

	} else {
		log.Printf("BindExchanger|NodeChange|非SUB节点变更|%s|%s\n", path, childNode)
	}
}

func (self *BindExchanger) DataChange(path string, binds []*Binding) {

	//订阅关系变更才处理
	if strings.HasPrefix(path, KITEQ_SUB) {

		split := strings.Split(path, "/")
		//获取topic
		topic := split[3]
		groupId := split[4]
		self.lock.Lock()
		defer self.lock.Unlock()
		//开始处理变化的订阅关系
		self.onBindChanged(topic, groupId, binds)

	} else {
		log.Printf("BindExchanger|DataChange|非SUB节点变更|%s\n", path)
	}

}

//订阅关系改变
func (self *BindExchanger) onBindChanged(topic, groupId string, newbinds []*Binding) {

	if len(groupId) <= 0 {
		delete(self.exchanger, topic)
		return
	}

	//不是当前服务可以处理的topic则直接丢地啊哦
	if sort.SearchStrings(self.topics, topic) == len(self.topics) {
		log.Printf("BindExchanger|onBindChanged|UnAccept Bindings|%s|%s|%s\n", topic, self.topics, newbinds)
		return
	}

	v, ok := self.exchanger[topic]
	if !ok {
		v = make(map[string][]*Binding, 10)
		self.exchanger[topic] = v
	}
	v[groupId] = newbinds
}

//关闭掉exchanger
func (self *BindExchanger) Shutdown() {
	//删除掉当前的QServer
	self.zkmanager.UnpushlishQServer(self.kiteqserver, self.topics)
	self.zkmanager.Close()
}
