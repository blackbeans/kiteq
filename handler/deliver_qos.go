package handler

import (
	"github.com/blackbeans/kiteq-common/stat"
	//	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"
	p "github.com/blackbeans/turbo/pipe"
	"time"
)

const (
	EXPIRED_SECOND = 10 * time.Second
	OVER_FLOW
)

//----------------投递的handler
type DeliverQosHandler struct {
	p.BaseForwardHandler
	flowstat *stat.FlowStat
}

//------创建deliver
func NewDeliverQosHandler(name string, flowstat *stat.FlowStat) *DeliverQosHandler {

	phandler := &DeliverQosHandler{}
	phandler.BaseForwardHandler = p.NewBaseForwardHandler(name, phandler)
	phandler.flowstat = flowstat
	return phandler
}

func (self *DeliverQosHandler) TypeAssert(event p.IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *DeliverQosHandler) cast(event p.IEvent) (val *deliverEvent, ok bool) {
	val, ok = event.(*deliverEvent)
	return
}

func (self *DeliverQosHandler) Process(ctx *p.DefaultPipelineContext, event p.IEvent) error {
	pevent, ok := self.cast(event)
	if !ok {
		return p.ERROR_INVALID_EVENT_TYPE
	}

	//没有投递分组直接投递结果
	if len(pevent.deliverGroups) <= 0 {
		//直接显示投递成功
		resultEvent := newDeliverResultEvent(pevent, nil)
		ctx.SendForward(resultEvent)
		return nil
	}

	groups := make([]string, 0, 10)
	overflow := make(map[string]*turbo.Future, 2)
	//check flows
	for g, limiter := range pevent.limiters {
		succ := limiter.Acquire()
		if succ {
			//allow deliver
			groups = append(groups, g)
		} else {
			//too fast overflow
			overflow[g] = turbo.NewErrFuture(-1, g, turbo.ERROR_OVER_FLOW)
		}
	}

	//赋值投递的分组
	pevent.deliverGroups = groups

	//投递消息的统计
	self.flowstat.IncrTopicDeliverFlow(pevent.header.GetTopic(), int32(len(groups)))

	//增加消息投递的次数
	pevent.deliverCount++
	//创建投递事件
	revent := p.NewRemotingEvent(pevent.packet, nil, groups...)
	revent.AttachEvent(pevent)
	revent.AttachErrFutures(overflow)
	//发起网络请求
	ctx.SendForward(revent)
	return nil

}
