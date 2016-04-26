package handler

import (
	"errors"
	"github.com/blackbeans/kiteq-common/protocol"
	p "github.com/blackbeans/turbo/pipe"
	// 	log "github.com/blackbeans/log4go"
)

//远程操作的PacketHandler

type PacketHandler struct {
	p.BaseForwardHandler
}

func NewPacketHandler(name string) *PacketHandler {
	packetHandler := &PacketHandler{}
	packetHandler.BaseForwardHandler = p.NewBaseForwardHandler(name, packetHandler)
	return packetHandler

}

func (self *PacketHandler) TypeAssert(event p.IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *PacketHandler) cast(event p.IEvent) (val *p.PacketEvent, ok bool) {
	val, ok = event.(*p.PacketEvent)
	return
}

var INVALID_PACKET_ERROR = errors.New("INVALID PACKET ERROR")

func (self *PacketHandler) Process(ctx *p.DefaultPipelineContext, event p.IEvent) error {

	// log.DebugLog("kite_handler", "PacketHandler|Process|%s|%t\n", self.GetName(), event)

	pevent, ok := self.cast(event)
	if !ok {
		return p.ERROR_INVALID_EVENT_TYPE
	}

	cevent, err := self.handlePacket(pevent)
	if nil != err {
		return err
	}

	ctx.SendForward(cevent)
	return nil
}

var sunkEvent = &p.SunkEvent{}

//对于请求事件
func (self *PacketHandler) handlePacket(pevent *p.PacketEvent) (p.IEvent, error) {
	var err error
	var event p.IEvent

	packet := pevent.Packet
	//根据类型反解packet
	switch packet.Header.CmdType {
	//连接的元数据
	case protocol.CMD_CONN_META:
		var connMeta protocol.ConnMeta
		err = protocol.UnmarshalPbMessage(packet.Data, &connMeta)
		if nil == err {
			meta := &connMeta
			event = newAccessEvent(meta.GetGroupId(), meta.GetSecretKey(), pevent.RemoteClient, packet.Header.Opaque)
		}

	//心跳
	case protocol.CMD_HEARTBEAT:
		var hearbeat protocol.HeartBeat
		err = protocol.UnmarshalPbMessage(packet.Data, &hearbeat)
		if nil == err {
			event = newAcceptEvent(protocol.CMD_HEARTBEAT, &hearbeat, pevent.RemoteClient, packet.Header.Opaque)
		}
		//投递结果确认
	case protocol.CMD_DELIVER_ACK:
		var delAck protocol.DeliverAck
		err = protocol.UnmarshalPbMessage(packet.Data, &delAck)
		if nil == err {
			event = newAcceptEvent(protocol.CMD_DELIVER_ACK, &delAck, pevent.RemoteClient, packet.Header.Opaque)
		}

	case protocol.CMD_TX_ACK:
		var txAck protocol.TxACKPacket
		err = protocol.UnmarshalPbMessage(packet.Data, &txAck)
		if nil == err {
			event = newTxAckEvent(&txAck, packet.Header.Opaque, pevent.RemoteClient)
		}

	//发送的是bytesmessage
	case protocol.CMD_BYTES_MESSAGE:
		var msg protocol.BytesMessage
		err = protocol.UnmarshalPbMessage(packet.Data, &msg)
		if nil == err {
			event = newAcceptEvent(protocol.CMD_BYTES_MESSAGE, &msg, pevent.RemoteClient, packet.Header.Opaque)
		}
	//发送的是StringMessage
	case protocol.CMD_STRING_MESSAGE:
		var msg protocol.StringMessage
		err = protocol.UnmarshalPbMessage(packet.Data, &msg)
		if nil == err {
			event = newAcceptEvent(protocol.CMD_STRING_MESSAGE, &msg, pevent.RemoteClient, packet.Header.Opaque)
		}
	}

	return event, err

}
