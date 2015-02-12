package handler

import (
	. "kiteq/pipe"
	// "log"
)

//-------投递结果的handler
type RemoteResultHandler struct {
	BaseForwardHandler
}

//------创建投递结果处理器
func NewRemoteResultHandler(name string) *RemoteResultHandler {
	dhandler := &RemoteResultHandler{}
	dhandler.BaseForwardHandler = NewBaseForwardHandler(name, dhandler)
	return dhandler
}

func (self *RemoteResultHandler) TypeAssert(event IEvent) bool {
	val, ok := self.cast(event)
	if ok && nil != val.Event {
		_, ok := val.Event.(*deliverEvent)
		return ok
	}
	return false
}

func (self *RemoteResultHandler) cast(event IEvent) (val *RemoteFutureEvent, ok bool) {
	val, ok = event.(*RemoteFutureEvent)

	return
}

func (self *RemoteResultHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	// log.Printf("RemoteResultHandler|Process|%s|%t\n", self.GetName(), fevent.Futures)

	fevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	futures := fevent.Wait()

	delverResult := newDeliverResultEvent(fevent.Event.(*deliverEvent), futures)
	//向后继续记录投递结果
	ctx.SendForward(delverResult)
	return nil

}
