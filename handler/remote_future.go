package handler

import (
	. "kiteq/pipe"
	// "log"
)

//网络调用的futurehandler
type RemoteFutureHandler struct {
	BaseForwardHandler
}

//------创建deliverpre
func NewRemotingFutureHandler(name string) *RemoteFutureHandler {
	phandler := &RemoteFutureHandler{}
	phandler.BaseForwardHandler = NewBaseForwardHandler(name, phandler)
	return phandler
}

func (self *RemoteFutureHandler) TypeAssert(event IEvent) bool {
	re, ok := self.cast(event)
	if ok {
		_, ok := re.Event.(*deliverEvent)
		return ok
	}
	return ok
}

func (self *RemoteFutureHandler) cast(event IEvent) (val *RemoteFutureEvent, ok bool) {
	val, ok = event.(*RemoteFutureEvent)
	return val, ok
}

func (self *RemoteFutureHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {
	pevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	futures := pevent.Wait()
	devent := pevent.RemotingEvent.Event.(*deliverEvent)
	// //创建一个投递结果
	resultEvent := newDeliverResultEvent(devent, futures)
	ctx.SendForward(resultEvent)
	return nil
}
