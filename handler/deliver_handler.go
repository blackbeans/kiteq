package handler

import (
	. "github.com/blackbeans/turbo/pipe"
	"kiteq/stat"
	"time"
	// 	log "github.com/blackbeans/log4go"
)

const (
	EXPIRED_SECOND = 10 * time.Second
)

//----------------投递的handler
type DeliverHandler struct {
	BaseDoubleSidedHandler
	deliveryRegistry *stat.DeliveryRegistry
}

//------创建deliverpre
func NewDeliverHandler(name string, deliveryRegistry *stat.DeliveryRegistry) *DeliverHandler {

	phandler := &DeliverHandler{}
	phandler.BaseDoubleSidedHandler = NewBaseDoubleSidedHandler(name, phandler)
	phandler.deliveryRegistry = deliveryRegistry
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

	//尝试注册一下当前的投递事件的消息
	//如果失败则放弃本次投递
	//会在 deliverResult里取消该注册事件可以继续投递
	succ := self.deliveryRegistry.Registe(pevent.messageId, EXPIRED_SECOND)
	if !succ {
		return nil
	}

	//没有投递分组直接投递结果
	if len(pevent.deliverGroups) <= 0 {
		//直接显示投递成功
		resultEvent := newDeliverResultEvent(pevent, nil)
		ctx.SendForward(resultEvent)
		return nil
	}

	//增加消息投递的次数
	pevent.deliverCount++
	//创建投递事件
	revent := NewRemotingEvent(pevent.packet, nil, pevent.deliverGroups...)
	revent.AttachEvent(pevent)
	//发起网络请求
	ctx.SendForward(revent)
	return nil

}
