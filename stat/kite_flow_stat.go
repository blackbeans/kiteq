package stat

import (
	"fmt"
	log "github.com/blackbeans/log4go"
	"kiteq/remoting"
	"time"
)

type FlowStat struct {
	RemotingFlow  *remoting.RemotingFlow
	OptimzeStatus bool
	DeliverFlow   *remoting.Flow
	DeliverPool   *remoting.Flow
	stop          bool
}

func NewFlowStat(name string) *FlowStat {
	f := &FlowStat{
		OptimzeStatus: true,
		DeliverFlow:   &remoting.Flow{},
		DeliverPool:   &remoting.Flow{},
		stop:          false}
	f.RemotingFlow = remoting.NewRemotingFlow(name)
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

	line := self.RemotingFlow.Monitor()
	if nil != self.DeliverFlow {
		line = fmt.Sprintf("%sdeliver:%d\tdeliver-go:%d\t", line, self.DeliverFlow.Changes(), self.DeliverPool.Count())
	}

	return line
}
