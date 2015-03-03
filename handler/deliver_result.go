package handler

import (
	. "kiteq/pipe"
	"kiteq/store"
	// "log"
	"time"
)

//-------投递结果的handler
type DeliverResultHandler struct {
	BaseBackwardHandler
	store          store.IKiteStore
	deliverTimeout time.Duration
}

//------创建投递结果处理器
func NewDeliverResultHandler(name string, store store.IKiteStore, deliverTimeout time.Duration) *DeliverResultHandler {
	dhandler := &DeliverResultHandler{}
	dhandler.BaseBackwardHandler = NewBaseBackwardHandler(name, dhandler)
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

	//等待结果响应
	fevent.wait(1 * time.Second)
	// log.Printf("DeliverResultHandler|Process|%s|%t\n", self.GetName(), fevent.futures)
	//则全部投递成功
	if len(fevent.deliveryFailGroups) <= 0 && len(fevent.deliverGroups) == len(fevent.deliverySuccGroups) {

		self.store.Delete(fevent.messageId)
		// log.Printf("DeliverResultHandler|%s|Process|ALL GROUP SEND |SUCC|%s|%s|%s\n", self.GetName(), fevent.deliverEvent.messageId, fevent.succGroups, fevent.failGroups)
	}
	// //向后继续记录投递结果
	ctx.SendBackward(fevent)
	return nil
}
