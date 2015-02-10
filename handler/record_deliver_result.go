package handler

import (
	. "kiteq/pipe"
	"kiteq/store"
	// "log"
)

//-------投递结果记录的handler
type ResultRecordHandler struct {
	BaseForwardHandler
	kitestore store.IKiteStore
}

//------创建投递结果处理器
func NewResultRecordHandler(name string, kitestore store.IKiteStore) *ResultRecordHandler {
	dhandler := &ResultRecordHandler{}
	dhandler.BaseForwardHandler = NewBaseForwardHandler(name, dhandler)
	dhandler.kitestore = kitestore
	return dhandler
}

func (self *ResultRecordHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *ResultRecordHandler) cast(event IEvent) (val *deliverResultEvent, ok bool) {
	val, ok = event.(*deliverResultEvent)
	return
}

func (self *ResultRecordHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	fevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	// log.Printf("ResultRecordHandler|Process|%s|%t\n", self.GetName(), fevent)
	ctx.SendForward(fevent)
	return nil

}
