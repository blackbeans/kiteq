package stat

import (
	"github.com/blackbeans/kiteq-common/store"
	"github.com/blackbeans/turbo"
	"sync"
)

type MessageFlow struct {
	Receive *turbo.Flow
	Deliver *turbo.Flow
}

type FlowStat struct {
	Kitestore   store.IKiteStore
	DeliverFlow *turbo.Flow
	RecieveFlow *turbo.Flow
	DeliverGo   *turbo.Flow
	TopicFlows  map[string]*MessageFlow
	lock        sync.RWMutex
	stop        bool
}

func NewFlowStat() *FlowStat {

	f := &FlowStat{
		DeliverFlow: &turbo.Flow{},
		RecieveFlow: &turbo.Flow{},
		DeliverGo:   &turbo.Flow{},
		TopicFlows:  make(map[string]*MessageFlow, 20),
		stop:        false}
	return f
}

func (self *FlowStat) TopicFlowSnapshot() (topicsdeliver, topicsrecieve map[string]int32) {

	self.lock.RLock()
	defer self.lock.RUnlock()
	//统计topic的数量消息
	topicsdeliver = make(map[string]int32, 20)
	topicsrecieve = make(map[string]int32, 20)
	// TopicReceiveFlow
	// TopicDeliverFlows
	for topic, f := range self.TopicFlows {
		topicsdeliver[topic] = f.Deliver.Changes()
		topicsrecieve[topic] = f.Receive.Changes()
	}
	return
}

func (self *FlowStat) IncrTopicReceiveFlow(topic string, count int32) {

	self.lock.RLock()
	//投递消息的统计
	flow, ok := self.TopicFlows[topic]
	if ok {
		flow.Receive.Incr(count)
	}
	self.lock.RUnlock()

	if !ok {
		self.lock.Lock()
		flow, ok := self.TopicFlows[topic]
		if !ok {
			f := &turbo.Flow{}
			f.Incr(count)
			self.TopicFlows[topic] = &MessageFlow{Receive: f, Deliver: &turbo.Flow{}}
		} else {
			flow.Receive.Incr(count)
		}
		self.lock.Unlock()
	}
}

func (self *FlowStat) IncrTopicDeliverFlow(topic string, count int32) {

	self.lock.RLock()
	//投递消息的统计
	flow, ok := self.TopicFlows[topic]
	if ok {
		flow.Deliver.Incr(count)
	}
	self.lock.RUnlock()

	if !ok {
		self.lock.Lock()
		flow, ok := self.TopicFlows[topic]
		if !ok {
			f := &turbo.Flow{}
			f.Incr(count)
			self.TopicFlows[topic] = &MessageFlow{Receive: &turbo.Flow{}, Deliver: f}
			// self.TopicFlows[topic].Receive = &turbo.Flow{}
		} else {
			flow.Deliver.Incr(count)
		}
		self.lock.Unlock()
	}
}
