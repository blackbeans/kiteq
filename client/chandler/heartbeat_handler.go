package chandler

import (
	. "kiteq/pipe"
	"log"
)

type HeartbeatHandler struct {
	BaseForwardHandler
}

//------创建heartbeat
func NewHeartbeatHandler(name string) *HeartbeatHandler {
	phandler := &HeartbeatHandler{}
	phandler.BaseForwardHandler = NewBaseForwardHandler(name, phandler)
	return phandler
}

func (self *HeartbeatHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *HeartbeatHandler) cast(event IEvent) (val *HeartbeatEvent, ok bool) {
	val, ok = event.(*HeartbeatEvent)
	return
}

func (self *HeartbeatHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	hevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	hevent.RemoteClient.Attach(hevent.Opaque, hevent.Version)
	log.Printf("HeartbeatHandler|%s|Process|Recieve|Pong|%s|%d\n", self.GetName(), hevent.RemoteClient.RemoteAddr(), hevent.Version)
	ctx.SendForward(event)
	return nil
}
