package stat

import (
	"fmt"
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"
	"kiteq/store"
	"time"
)

type FlowStat struct {
	name        string
	Kitestore   store.IKiteStore
	DeliverFlow *turbo.Flow
	DeliverPool *turbo.Flow
	stop        bool
	//current deliver count
	DeliverCount int32
}

func NewFlowStat(name string) *FlowStat {
	f := &FlowStat{
		name:        name,
		DeliverFlow: &turbo.Flow{},
		DeliverPool: &turbo.Flow{},
		stop:        false}
	return f
}

func (self *FlowStat) Start() {

	go func() {
		t := time.NewTicker(1 * time.Second)
		for !self.stop {
			self.DeliverCount = self.DeliverFlow.Changes()
			line := fmt.Sprintf("%s\tdeliver:%d\tdeliver-go:%d\t", self.name, self.DeliverCount, self.DeliverPool.Count())
			log.Info(line)
			if nil != self.Kitestore {
				log.Info(self.Kitestore.Monitor())
			}
			<-t.C
		}
		t.Stop()
	}()
}
