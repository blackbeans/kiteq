package handler

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/turbo"
	// 	log "github.com/blackbeans/log4go"
)

type HeartbeatHandler struct {
	turbo.BaseForwardHandler
}

//------创建heartbeat
func NewHeartbeatHandler(name string) *HeartbeatHandler {
	phandler := &HeartbeatHandler{}
	phandler.BaseForwardHandler = turbo.NewBaseForwardHandler(name, phandler)
	return phandler
}

func (self *HeartbeatHandler) TypeAssert(event turbo.IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *HeartbeatHandler) cast(event turbo.IEvent) (val *turbo.HeartbeatEvent, ok bool) {
	val, ok = event.(*turbo.HeartbeatEvent)
	return
}

func (self *HeartbeatHandler) Process(ctx *turbo.DefaultPipelineContext, event turbo.IEvent) error {

	hevent, ok := self.cast(event)
	if !ok {
		return turbo.ERROR_INVALID_EVENT_TYPE
	}

	//处理本地的pong
	hevent.RemoteClient.Pong(hevent.Opaque, hevent.Version)

	//发起一个ping对应的响应
	packet := turbo.NewRespPacket(hevent.Opaque, protocol.CMD_HEARTBEAT, protocol.MarshalHeartbeatPacket(hevent.Version))
	//发起一个网络请求
	remoteEvent := turbo.NewRemotingEvent(packet, []string{hevent.RemoteClient.RemoteAddr()})

	// log.InfoLog("kite_handler", "HeartbeatHandler|%s|Process|Recieve|Ping|%s|%d\n", self.GetName(), hevent.RemoteClient.RemoteAddr(), hevent.Version)
	ctx.SendForward(remoteEvent)
	return nil
}
