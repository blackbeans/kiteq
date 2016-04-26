package handler

import (
	p "github.com/blackbeans/turbo/pipe"
	// 	log "github.com/blackbeans/log4go"
)

//网络调用的futurehandler
type RemoteFutureHandler struct {
	p.BaseForwardHandler
}

//------创建deliverpre
func NewRemotingFutureHandler(name string) *RemoteFutureHandler {
	phandler := &RemoteFutureHandler{}
	phandler.BaseForwardHandler = p.NewBaseForwardHandler(name, phandler)
	return phandler
}

func (self *RemoteFutureHandler) TypeAssert(event p.IEvent) bool {
	re, ok := self.cast(event)
	if ok {
		_, ok := re.Event.(*deliverEvent)
		return ok
	}
	return ok
}

func (self *RemoteFutureHandler) cast(event p.IEvent) (val *p.RemoteFutureEvent, ok bool) {
	val, ok = event.(*p.RemoteFutureEvent)
	return val, ok
}

func (self *RemoteFutureHandler) Process(ctx *p.DefaultPipelineContext, event p.IEvent) error {
	pevent, ok := self.cast(event)
	if !ok {
		return p.ERROR_INVALID_EVENT_TYPE
	}

	futures := pevent.Wait()
	devent := pevent.RemotingEvent.Event.(*deliverEvent)
	// //创建一个投递结果
	resultEvent := newDeliverResultEvent(devent, futures)
	ctx.SendForward(resultEvent)
	return nil
}
