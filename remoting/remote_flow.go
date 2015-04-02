package remoting

import (
	"fmt"
	"sync/atomic"
)

type RemotingFlow struct {
	Name               string
	OptimzeStatus      bool //当前优化的状态
	ReadFlow           *Flow
	DispatcherWorkPool *Flow //处理
	DispatcherFlow     *Flow
	WriteFlow          *Flow
}

func NewRemotingFlow(name string) *RemotingFlow {
	return &RemotingFlow{
		OptimzeStatus:      true,
		Name:               name,
		ReadFlow:           &Flow{},
		DispatcherWorkPool: &Flow{},
		DispatcherFlow:     &Flow{},
		WriteFlow:          &Flow{}}
}

func (self *RemotingFlow) Monitor() string {

	line := fmt.Sprintf("%s:\tread:%d\tdispatcher:%d\twrite:%d\t", self, self.ReadFlow.Changes(),
		self.DispatcherFlow.Changes(), self.WriteFlow.Changes())
	if nil != self.DispatcherWorkPool {
		line = fmt.Sprintf("%sdispatcher-pool:%d\t", line, self.DispatcherWorkPool.count)
	}
	return line
}

type Flow struct {
	count     int32
	lastcount int32
}

func (self *Flow) Incr(num int32) {
	atomic.AddInt32(&self.count, num)
}

func (self *Flow) Count() int32 {
	return self.count
}

func (self *Flow) Changes() int32 {
	tmpc := self.count
	tmpl := self.lastcount
	c := tmpc - tmpl
	self.lastcount = tmpc
	return c
}
