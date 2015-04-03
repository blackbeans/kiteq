package stat

import (
	"fmt"
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"
	"time"
)

type FlowStat struct {
	name          string
	OptimzeStatus bool
	DeliverFlow   *turbo.Flow
	DeliverPool   *turbo.Flow
	stop          bool
}

func NewFlowStat(name string) *FlowStat {
	f := &FlowStat{
		name:          name,
		OptimzeStatus: true,
		DeliverFlow:   &turbo.Flow{},
		DeliverPool:   &turbo.Flow{},
		stop:          false}
	return f
}

func (self *FlowStat) Start() {

	go func() {
		t := time.NewTicker(1 * time.Second)
		for !self.stop {
			line := self.Monitor()
			log.Info(line)
			<-t.C
		}
		t.Stop()
	}()
}

func (self *FlowStat) Monitor() string {
	if nil != self.DeliverFlow {
		return fmt.Sprintf("%sdeliver:%d\tdeliver-go:%d\t", self.name, self.DeliverFlow.Changes(), self.DeliverPool.Count())
	}
	return fmt.Sprintf("%sdeliver:%d\tdeliver-go:%d\t", self.name, 0, 0)
}
