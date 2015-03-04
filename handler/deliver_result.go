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

	//等待结果响应
	fevent.wait(self.deliverTimeout)
	// log.Printf("DeliverResultHandler|Process|%s|%t\n", self.GetName(), fevent.futures)
	ctx.SendForward(fevent)
	return nil
}
