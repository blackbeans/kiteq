package handler

import (
	. "kiteq/pipe"
	"kiteq/protocol"
	"kiteq/remoting/client"
	"log"
)

//----------------鉴权handler
type AccessHandler struct {
	BaseForwardHandler
	clientManager *client.ClientManager
}

//------创建鉴权handler
func NewAccessHandler(name string, clientManager *client.ClientManager) *AccessHandler {
	ahandler := &AccessHandler{}
	ahandler.BaseForwardHandler = NewBaseForwardHandler(name, ahandler)
	ahandler.clientManager = clientManager
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

	// log.Printf("AccessEvent|Process|%s|%t\n", self.GetName(), event)

	aevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	//做权限校验.............
	if false {
		log.Printf("AccessEvent|Process|INVALID AUTH|%s|%s\n", aevent.GroupId, aevent.SecretKey)
	}

	// 权限验证通过 保存到session
	self.clientManager.Add(aevent.GroupId, aevent.remoteClient)

	cmd := protocol.MarshalConnAuthAck(true, "授权成功")
	//响应包
	packet := protocol.NewPacket(protocol.CMD_CONN_AUTH, cmd)
	//向当前连接写入一个存储成功的response
	remoteEvent := NewRemotingEvent(packet, []string{aevent.remoteClient.RemoteAddr()})

	//向后走网络传输
	ctx.SendForward(remoteEvent)
	return nil

}
