package handler

import (
	"kiteq/remoting/session"
)

//----------------持久化的handler
type DeliverHandler struct {
	BaseForwardHandler
	IEventProcessor
	sessionmanager *session.SessionManager
}

//------创建deliverpre
func NewDeliverHandler(name string, sessionmanager *session.SessionManager) *DeliverHandler {
	phandler := &DeliverHandler{}
	phandler.name = name
	phandler.processor = phandler
	phandler.sessionmanager = sessionmanager
	return phandler
}

func (self *DeliverHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *DeliverHandler) cast(event IEvent) (val *DeliverEvent, ok bool) {
	val, ok = event.(*DeliverEvent)
	return
}

func (self *DeliverHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	// log.Printf("DeliverHandler|Process|%t\n", event)

	pevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	//查找分组对应的session
	sessions := self.sessionmanager.FindSessions(pevent.DeliverGroups, func(s *session.Session) bool {
		return false
	})

	//获取投递的消息数据

	//创建投递事件
	revent := newRemotingEvent(nil, sessions...)

	//向后转发
	ctx.SendForward(revent)

	return nil

}
