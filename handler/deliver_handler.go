package handler

import (
	. "kiteq/pipe"
	// "log"
)

//----------------投递的handler
type DeliverHandler struct {
	BaseDoubleSidedHandler
}

//------创建deliverpre
func NewDeliverHandler(name string) *DeliverHandler {

	phandler := &DeliverHandler{}
	phandler.BaseDoubleSidedHandler = NewBaseDoubleSidedHandler(name, phandler)

	return phandler
}

func (self *DeliverHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *DeliverHandler) cast(event IEvent) (val *deliverEvent, ok bool) {
	val, ok = event.(*deliverEvent)
	return
}

func (self *DeliverHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {
	pevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	//没有投递分组直接投递结果
	if len(pevent.deliverGroups) <= 0 {
		//直接显示投递成功
		resultEvent := newDeliverResultEvent(pevent, make(map[string]chan interface{}, 0))
		ctx.SendForward(resultEvent)
		return nil
	}

	//减少事件的ttl及消息投递的次数
	pevent.ttl--
	pevent.deliverCount++
	//创建投递事件
	revent := NewRemotingEvent(pevent.packet, nil, pevent.deliverGroups...)
	revent.AttachEvent(pevent)
	//发起网络请求
	go ctx.SendForward(revent)

	// //创建一个投递结果
	resultEvent := newDeliverResultEvent(pevent, revent.Wait())
	ctx.SendForward(resultEvent)
	return nil

}
