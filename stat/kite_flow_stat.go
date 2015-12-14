package stat

import (
	"github.com/blackbeans/turbo"
	"kiteq/store"
)

type FlowStat struct {
	Kitestore   store.IKiteStore
	DeliverFlow *turbo.Flow
	DeliverGo   *turbo.Flow
	TopicsFlows map[string]*turbo.Flow
	stop        bool
}

func NewFlowStat(name string) *FlowStat {
	f := &FlowStat{
		DeliverFlow: &turbo.Flow{},
		DeliverGo:   &turbo.Flow{},
		TopicsFlows: make(map[string]*turbo.Flow, 20),
		stop:        false}
	return f
}
