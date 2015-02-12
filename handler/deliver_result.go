package handler

import (
	. "kiteq/pipe"
	"kiteq/store"
	// "log"
	"time"
)

//-------投递结果的handler
type DeliverResultHandler struct {
	BaseForwardHandler
	store          store.IKiteStore
	deliverTimeout time.Duration
}

//------创建投递结果处理器
func NewDeliverResultHandler(name string, store store.IKiteStore, deliverTimeout time.Duration) *DeliverResultHandler {
	dhandler := &DeliverResultHandler{}
	dhandler.BaseForwardHandler = NewBaseForwardHandler(name, dhandler)
	dhandler.store = store
	dhandler.deliverTimeout = deliverTimeout
	return dhandler
}

func (self *DeliverResultHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *DeliverResultHandler) cast(event IEvent) (val *deliverResultEvent, ok bool) {
	val, ok = event.(*deliverResultEvent)
	return
}

func (self *DeliverResultHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	fevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}
	// log.Printf("DeliverResultHandler|Process|%s|%t\n", self.GetName(), fevent.futures)
	//等待结果响应
	fevent.wait(-1)

	//则全部投递成功
	if len(fevent.failGroups) <= 0 {
		self.store.Delete(fevent.messageId)
		return nil
		// log.Printf("DeliverResultHandler|%s|Process|ALL GROUP SEND |SUCC|%s|%s|%s\n", self.GetName(), fevent.deliverEvent.messageId, fevent.succGroups, fevent.failGroups)
	} else {
		//需要将失败的分组重新投递
		// log.Printf("DeliverResultHandler|%s|Process|GROUP SEND |FAIL|%s|%s|%s\n", self.GetName(), fevent.deliverEvent.messageId, fevent.succGroups, fevent.failGroups)

		//检查当前消息的ttl和有效期是否达到最大的，如果达到最大则不允许再次投递
		if fevent.expiredTime >= time.Now().Unix() || fevent.deliverLimit >= fevent.deliverCount {
			//只是记录一下本次发送记录不发起重投策略

		} else if fevent.deliverEvent.ttl > 0 {
			fevent.deliverEvent.packet.ResetOpaque()
			//再次发起重投策略
			ctx.SendBackward(fevent.deliverEvent)
			return nil

		} else {
			//只能等后续的recover线程去处理
		}
	}
	// //向后继续记录投递结果
	go ctx.SendForward(fevent)
	return nil
}
