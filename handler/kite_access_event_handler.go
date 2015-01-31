package handler

import (
	"kiteq/protocol"
	"kiteq/remoting/session"
	// "log"
)

//----------------鉴权handler
type AccessHandler struct {
	BaseForwardHandler
	IEventProcessor
	sessionmanager *session.SessionManager
}

//------创建鉴权handler
func NewAccessHandler(name string, sessionmanager *session.SessionManager) *AccessHandler {
	ahandler := &AccessHandler{}
	ahandler.name = name
	ahandler.processor = ahandler
	ahandler.sessionmanager = sessionmanager
	return ahandler
}

func (self *AccessHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *AccessHandler) cast(event IEvent) (val *AccessEvent, ok bool) {
	val, ok = event.(*AccessEvent)
	return
}

func (self *AccessHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	// log.Printf("AccessEvent|Process|%t\n", event)

	aevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	//做权限校验.............
	// 权限验证通过 保存到session
	self.sessionmanager.Add(aevent.GroupId, aevent.session)
	aevent.session.GroupId = aevent.GroupId

	cmd := protocol.MarshalConnAuthAck(true, "授权成功")

	//响应包
	packet := protocol.NewPacket(
		aevent.opaque,
		protocol.CMD_CONN_AUTH,
		cmd)

	//向当前连接写入一个存储成功的response
	remoteEvent := newRemotingEvent(packet, aevent.session)

	//向后走网络传输
	ctx.SendForward(remoteEvent)
	return nil

}
