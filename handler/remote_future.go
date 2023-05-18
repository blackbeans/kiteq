package handler

import (
	"github.com/blackbeans/turbo"
	// 	"github.com/blackbeans/logx"
)

//网络调用的futurehandler
type RemoteFutureHandler struct {
	turbo.BaseForwardHandler
}

//------创建deliverpre
func NewRemotingFutureHandler(name string) *RemoteFutureHandler {
	phandler := &RemoteFutureHandler{}
	phandler.BaseForwardHandler = turbo.NewBaseForwardHandler(name, phandler)
	return phandler
}

func (self *RemoteFutureHandler) TypeAssert(event turbo.IEvent) bool {
	re, ok := self.cast(event)
	if ok {
		_, ok := re.Event.(*deliverEvent)
		return ok
	}
	return ok
}

func (self *RemoteFutureHandler) cast(event turbo.IEvent) (val *turbo.RemoteFutureEvent, ok bool) {
	val, ok = event.(*turbo.RemoteFutureEvent)
	return val, ok
}

func (self *RemoteFutureHandler) Process(ctx *turbo.DefaultPipelineContext, event turbo.IEvent) error {
	pevent, ok := self.cast(event)
	if !ok {
		return turbo.ERROR_INVALID_EVENT_TYPE
	}

	futures := pevent.Wait()
	devent := pevent.RemotingEvent.Event.(*deliverEvent)
	// //创建一个投递结果
	resultEvent := newDeliverResultEvent(devent, futures)
	ctx.SendForward(resultEvent)
	return nil
}
