package handler

import (
	// "github.com/golang/protobuf/proto"
	// "kiteq/protocol"
	"kiteq/remoting/session"
	"kiteq/store"
)

//----------------持久化的handler
type DeliverHandler struct {
	BaseForwardHandler
	IEventProcessor
	sessionmanager *session.SessionManager
	store          store.IKiteStore
}

//------创建deliverpre
func NewDeliverHandler(name string, sessionmanager *session.SessionManager, store store.IKiteStore) *DeliverHandler {
	phandler := &DeliverHandler{}
	phandler.name = name
	phandler.processor = phandler
	phandler.sessionmanager = sessionmanager
	phandler.store = store
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
	// msgEntity := self.store.Query(pevent.MessageId)

	//发起一个请求包

	//创建投递事件
	revent := newRemotingEvent(nil, sessions...)

	//向后转发
	ctx.SendForward(revent)

	return nil

}
