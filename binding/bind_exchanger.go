package binding

import (
	"log"
	"sort"
	"strings"
	"sync"
)

//用于管理订阅关系，对接zookeeper的订阅关系变更
type BindExchanger struct {
	exchanger map[string] /*topic*/ map[string] /*groupId*/ []*Binding //保存的订阅关系
	topics    []string                                                 //当前服务器可投递的topic类型
	lock      sync.Mutex
	zkmanager *ZKManager
}

func NewBindExchanger(zkhost string) *BindExchanger {
	return &BindExchanger{
		exchanger: make(map[string]map[string][]*Binding, 100),
		topics:    make([]string, 0, 50),
		zkmanager: NewZKManager(zkhost)}
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
	for _, topic := range topics {
		binds, err := self.zkmanager.GetBindAndWatch(topic, NewWatcher(self))
		if nil != err {
			log.Printf("BindExchanger|SubscribeBinds|FAIL|%s|%s\n", err, topic)
			return false
		} else {
			self.onBindChanged(topic, binds)
		}
	}

	return true
}

//订阅关系改变
func (self *BindExchanger) onBindChanged(topic string, newbinds []*Binding) {
	self.lock.Lock()
	defer self.lock.Unlock()
	v, ok := self.exchanger[topic]
	if !ok {
		v = make(map[string][]*Binding, 10)
		self.exchanger[topic] = v
	}

outter:
	for _, bind := range newbinds {
		binds, ok := v[bind.GroupId]
		//还没有该组的订阅关系则直接创建
		if !ok {
			binds = make([]*Binding, 0, 10)
		} else {
			//如果已经存在则直接进行排重
			for _, b := range binds {
				if b.conflict(bind) {
					//如果相等则直接略过
					log.Printf("BindExchanger|SubscribeBinds|CONFLICT BIND|%s|%s\n", b, bind)
					continue outter
				}
				//否则后面根据情况添加
			}
		}

		//如果已经满了直接扩大
		if len(binds) >= cap(binds) {
			newbind := make([]*Binding, 0, cap(binds)+10)
			//copy data
			copy(newbind, binds)
			binds = newbind
		}

		//放入到映射关系里
		binds = append(binds, bind)

		//翻入订阅关系
		v[bind.GroupId] = binds
	}

}

//根据topic和messageType 类型获取订阅关系
func (self *BindExchanger) FindBinds(topic string, messageType string) []*Binding {
	self.lock.Lock()
	defer self.lock.Unlock()
	groups, ok := self.exchanger[topic]
	if !ok {
		return []*Binding{}
	}

	//符合规则的binds
	validBinds := make([]*Binding, 0, 10)
	for _, binds := range groups {
		for _, b := range binds {
			if b.matches(topic, messageType) {
				validBinds = append(validBinds, b)
			}
		}
	}

	return validBinds
}

func (self *BindExchanger) EventNotify(path string, eventType ZkEvent, binds []*Binding) {

	//订阅关系变更才处理
	if eventType == Changed && strings.HasPrefix(path, KITEQ_SUB) {
		split := strings.Split(path, "/")
		if len(split) < 5 {
			//不合法的订阅璐姐
			log.Printf("BindExchanger|EventNotify|INVALID SUB DATA|PATH |%s\n", path)
			return
		}
		//获取topic
		topic := split[3]
		//不是当前服务可以处理的topic则直接丢地啊哦
		if sort.SearchStrings(self.topics, topic) < 0 {
			log.Printf("BindExchanger|EventNotify|REFUSE SUB PATH |%s|%t\n", path, binds)
			return
		}
		//开始处理变化的订阅关系
		self.onBindChanged(topic, binds)
	}
}

//订阅关系topic下的group发生变更
func (self *BindExchanger) ChildWatcher(path string, childNode []string) {
	//如果是订阅关系变更则处理
	if strings.HasPrefix(path, KITEQ_SUB) {
		//获取topic
		split := strings.Split(path, "/")
		if len(split) < 4 {
			//不合法的订阅璐姐
			log.Printf("BindExchanger|ChildWatcher|INVALID SUB PATH |%s|%t\n", path, childNode)
			return
		}
		//获取topic
		topic := split[3]
		//不是当前服务可以处理的topic则直接丢地啊哦
		if sort.SearchStrings(self.topics, topic) < 0 {
			log.Printf("BindExchanger|ChildWatcher|REFUSE SUB PATH |%s|%t\n", path, childNode)
			return
		}

		self.lock.Lock()
		self.lock.Unlock()

		groupIds, ok := self.exchanger[topic]
		if !ok {
			groupIds = make(map[string] /*groupId*/ []*Binding, 20)
			self.exchanger[topic] = groupIds
		}

		//如果topic下无订阅分组节点，直接删除该topic
		if len(childNode) <= 0 {
			delete(self.exchanger, topic)
			return
		}

		//重新拷贝赋值
		copyMap := make(map[string] /*groupId*/ []*Binding, len(groupIds))
		//对当前的topic的分组进行重新设置
		for _, child := range childNode {
			groupId := strings.TrimSuffix(child, "-bind")
			if !ok {
				//新增的订阅关系
				groupIds[groupId] = make([]*Binding, 0, 5)
			}
			//直接拷贝赋值
			copyMap[groupId] = groupIds[groupId]
		}

		//直接替换到当前topic中的订阅关系
		self.exchanger[topic] = copyMap

	} else {
		//其他都不处理，因为服务端只有对订阅关系节点的处理
	}
}

//关闭掉exchanger
func (self *BindExchanger) Shutdown() {
	self.zkmanager.Close()
}
