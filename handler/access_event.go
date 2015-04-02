package handler

import (
	log "github.com/blackbeans/log4go"
	client "github.com/blackbeans/turbo/client"
	packet "github.com/blackbeans/turbo/packet"
	. "github.com/blackbeans/turbo/pipe"
	"kiteq/protocol"
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

func (self *AccessHandler) cast(event IEvent) (val *accessEvent, ok bool) {
	val, ok = event.(*accessEvent)
	return
}

func (self *AccessHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	// log.Debug("accessEvent|Process|%s|%t\n", self.GetName(), event)

	aevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	//做权限校验.............
	if false {
		log.Warn("accessEvent|Process|INVALID AUTH|%s|%s\n", aevent.groupId, aevent.secretKey)
	}

	// 权限验证通过 保存到clientmanager
	self.clientManager.Auth(client.NewGroupAuth(aevent.groupId, aevent.secretKey), aevent.remoteClient)

	// log.Info("accessEvent|Process|NEW CONNECTION|AUTH SUCC|%s|%s|%s\n", aevent.groupId, aevent.secretKey, aevent.remoteClient.RemoteAddr())

	cmd := protocol.MarshalConnAuthAck(true, "授权成功")
	//响应包
	packet := packet.NewRespPacket(aevent.opaque, protocol.CMD_CONN_AUTH, cmd)

	//向当前连接写入当前包
	remoteEvent := NewRemotingEvent(packet, []string{aevent.remoteClient.RemoteAddr()})

	//向后走网络传输
	ctx.SendForward(remoteEvent)
	return nil

}
