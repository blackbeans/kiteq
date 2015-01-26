package handler

import (
	"kiteq/protocol"
	// "log"
)

//----------------鉴权handler
type AccessHandler struct {
	BaseForwardHandler
	IEventProcessor
}

//------创建鉴权handler
func NewAccessHandler(name string) *AccessHandler {
	ahandler := &AccessHandler{}
	ahandler.name = name
	ahandler.processor = ahandler
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

	aevent.session.GroupId = aevent.GroupId
	//响应包
	responsePacket := &protocol.ResponsePacket{
		Opaque:     aevent.opaque,
		RemoteAddr: aevent.session.RemotingAddr(),
		CmdType:    protocol.CMD_CONN_META,
		Status:     protocol.RESP_STATUS_SUCC}

	//向当前连接写入一个存储成功的response
	remoteEvent := newRemotingEvent(responsePacket, aevent.session)

	//向后走网络传输
	ctx.SendForward(remoteEvent)
	return nil

}
