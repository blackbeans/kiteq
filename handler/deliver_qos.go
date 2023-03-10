package handler

import (
	"github.com/blackbeans/kiteq-common/stat"
	"github.com/blackbeans/turbo"
	"sort"
	"time"
)

const (
	EXPIRED_SECOND = 10 * time.Second
)

//----------------投递的handler
type DeliverQosHandler struct {
	turbo.BaseDoubleSidedHandler
	flowstat *stat.FlowStat
}

//------创建deliver
func NewDeliverQosHandler(name string, flowstat *stat.FlowStat) *DeliverQosHandler {

	phandler := &DeliverQosHandler{}
	phandler.BaseDoubleSidedHandler = turbo.NewBaseDoubleSidedHandler(name, phandler)
	phandler.flowstat = flowstat
	return phandler
}

func (self *DeliverQosHandler) TypeAssert(event turbo.IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *DeliverQosHandler) cast(event turbo.IEvent) (val *deliverEvent, ok bool) {
	val, ok = event.(*deliverEvent)
	return
}

func (self *DeliverQosHandler) Process(ctx *turbo.DefaultPipelineContext, event turbo.IEvent) error {
	pevent, ok := self.cast(event)
	if !ok {
		return turbo.ERROR_INVALID_EVENT_TYPE
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
	//sort deliver groups
	sort.Strings(pevent.deliverGroups)
	//check flows
	for g, limiter := range pevent.limiters {
		//matches valid limiter
		idx := sort.SearchStrings(pevent.deliverGroups, g)
		if idx >= len(pevent.deliverGroups) || pevent.deliverGroups[idx] != g {
			//not find
			continue
		}

		succ := limiter.Acquire()
		if succ {
			//allow deliver
			groups = append(groups, g)
		} else {
			//too fast overflow
			overflow[g] = turbo.NewErrFuture(0, g, turbo.ERR_OVER_FLOW, nil)
		}
	}

	//赋值投递的分组
	pevent.deliverGroups = groups

	//投递消息的统计
	self.flowstat.IncrTopicDeliverFlow(pevent.header.GetTopic(), int32(len(groups)))

	//增加消息投递的次数
	pevent.deliverCount++

	//创建投递事件
	revent := turbo.NewRemotingEvent(pevent.packet, nil, groups...)
	revent.AttachEvent(pevent)
	revent.AttachErrFutures(overflow)
	//发起网络请求
	ctx.SendForward(revent)
	return nil

}
