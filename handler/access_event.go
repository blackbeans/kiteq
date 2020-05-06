package handler

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/turbo"
)

//----------------鉴权handler
type AccessHandler struct {
	turbo.BaseForwardHandler
	clientManager *turbo.ClientManager
}

//------创建鉴权handler
func NewAccessHandler(name string, clientManager *turbo.ClientManager) *AccessHandler {
	ahandler := &AccessHandler{}
	ahandler.BaseForwardHandler = turbo.NewBaseForwardHandler(name, ahandler)
	ahandler.clientManager = clientManager
	return ahandler
}

func (self *AccessHandler) TypeAssert(event turbo.IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *AccessHandler) cast(event turbo.IEvent) (val *accessEvent, ok bool) {
	val, ok = event.(*accessEvent)
	return
}

func (self *AccessHandler) Process(ctx *turbo.DefaultPipelineContext, event turbo.IEvent) error {

	// log.Debug("accessEvent|Process|%s|%t\n", self.GetName(), event)

	aevent, ok := self.cast(event)
	if !ok {
		return turbo.ERROR_INVALID_EVENT_TYPE
	}

	// 权限验证通过 保存到clientmanager
	auth := turbo.NewGroupAuth(aevent.connMeta.GetGroupId(), aevent.connMeta.GetSecretKey())
	//填写warmingup的时间
	auth.WarmingupSec = int(aevent.connMeta.GetWarmingupSec())
	self.clientManager.Auth(auth, aevent.remoteClient)
	cmd := protocol.MarshalConnAuthAck(true, "授权成功")
	//响应包
	packet := turbo.NewRespPacket(aevent.opaque, protocol.CMD_CONN_AUTH, cmd)

	//向当前连接写入当前包
	remoteEvent := turbo.NewRemotingEvent(packet, []string{aevent.remoteClient.RemoteAddr()})

	//向后走网络传输
	ctx.SendForward(remoteEvent)
	return nil

}
