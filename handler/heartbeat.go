package handler

import (
	packet "github.com/blackbeans/turbo/packet"
	. "github.com/blackbeans/turbo/pipe"
	"kiteq/protocol"
	// 	log "github.com/blackbeans/log4go"
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

	//处理本地的pong
	hevent.RemoteClient.Pong(hevent.Opaque, hevent.Version)

	//发起一个ping对应的响应
	packet := packet.NewRespPacket(hevent.Opaque, protocol.CMD_HEARTBEAT, protocol.MarshalHeartbeatPacket(hevent.Version))
	//发起一个网络请求
	remoteEvent := NewRemotingEvent(packet, []string{hevent.RemoteClient.RemoteAddr()})

	// log.InfoLog("kite_handler", "HeartbeatHandler|%s|Process|Recieve|Ping|%s|%d\n", self.GetName(), hevent.RemoteClient.RemoteAddr(), hevent.Version)
	ctx.SendForward(remoteEvent)
	return nil
}
