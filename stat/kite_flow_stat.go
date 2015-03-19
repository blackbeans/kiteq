package stat

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

type FlowStat struct {
	name           string
	ReadFlow       *flow
	DispatcherFlow *flow
	WriteFlow      *flow
	DeliverFlow    *flow
	DeliverPool    *flow
	stop           bool
}

func NewFlowStat(name string) *FlowStat {
	return &FlowStat{
		name:           name,
		ReadFlow:       &flow{},
		DispatcherFlow: &flow{},
		WriteFlow:      &flow{},
		DeliverFlow:    &flow{},
		DeliverPool:    &flow{},
		stop:           false}
}

func (self *FlowStat) Start() {

	go func() {
		t := time.NewTicker(1 * time.Second)
		for !self.stop {
			line := fmt.Sprintf("%s:\tread:%d\tdispatcher:%d\twrite:%d\t", self.name, self.ReadFlow.changes(),
				self.DispatcherFlow.changes(), self.WriteFlow.changes())
			if nil != self.DeliverFlow {
				line = fmt.Sprintf("%s\tdeliver:%d\tdeliver-go:%d\t", line, self.DeliverFlow.changes(), self.DeliverPool.count)
			}

			log.Println(line)
			<-t.C
		}
		t.Stop()
	}()
}

func (self *FlowStat) Stop() {
	self.stop = true
}

type flow struct {
	count     int32
	lastcount int32
}

func (self *flow) Incr(num int32) {
	atomic.AddInt32(&self.count, num)
}

func (self *flow) changes() int32 {
	tmpc := self.count
	tmpl := self.lastcount
	c := tmpc - tmpl
	self.lastcount = tmpc
	return c
}
