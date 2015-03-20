package handler

import (
	"errors"
	. "kiteq/pipe"
	"kiteq/protocol"
	// "log"
)

//远程操作的PacketHandler

type PacketHandler struct {
	BaseForwardHandler
}

func NewPacketHandler(name string) *PacketHandler {
	packetHandler := &PacketHandler{}
	packetHandler.BaseForwardHandler = NewBaseForwardHandler(name, packetHandler)
	return packetHandler

}

func (self *PacketHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *PacketHandler) cast(event IEvent) (val *PacketEvent, ok bool) {
	val, ok = event.(*PacketEvent)
	return
}

var INVALID_PACKET_ERROR = errors.New("INVALID PACKET ERROR")

func (self *PacketHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	// log.Printf("PacketHandler|Process|%s|%t\n", self.GetName(), event)

	pevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	cevent, err := self.handlePacket(pevent)
	if nil != err {
		return err
	}

	ctx.SendForward(cevent)
	return nil
}

var sunkEvent = &SunkEvent{}

//对于请求事件
func (self *PacketHandler) handlePacket(pevent *PacketEvent) (IEvent, error) {
	var err error
	var event IEvent
	var marshaler protocol.MarshalHelper
	packet := pevent.Packet
	marshaler = protocol.GetMarshaler(&packet.CmdType)
	pevent.RemoteClient.Marshaler = marshaler
	//根据类型反解packet
	switch packet.CmdType {
	//连接的元数据
	case protocol.CMD_CONN_META:
		var connMeta protocol.ConnMeta
		err = marshaler.UnmarshalMessage(packet.Data, &connMeta)

		if nil == err {
			meta := &connMeta
			event = newAccessEvent(meta.GetGroupId(), meta.GetSecretKey(), pevent.RemoteClient, packet.Opaque)
		}

	//心跳
	case protocol.CMD_HEARTBEAT:
		var hearbeat protocol.HeartBeat
		err = marshaler.UnmarshalMessage(packet.Data, &hearbeat)
		if nil == err {
			event = newAcceptEvent(protocol.CMD_HEARTBEAT, &hearbeat, pevent.RemoteClient, packet.Opaque)
		}
		//投递结果确认
	case protocol.CMD_DELIVER_ACK:
		var delAck protocol.DeliverAck
		err = marshaler.UnmarshalMessage(packet.Data, &delAck)

		if nil == err {
			event = newAcceptEvent(protocol.CMD_DELIVER_ACK, &delAck, pevent.RemoteClient, packet.Opaque)
		}

	case protocol.CMD_TX_ACK:
		var txAck protocol.TxACKPacket
		err = marshaler.UnmarshalMessage(packet.Data, &txAck)
		if nil == err {
			event = newTxAckEvent(&txAck, packet.Opaque, pevent.RemoteClient)
		}

	//发送的是bytesmessage
	case protocol.CMD_BYTES_MESSAGE:
		var msg protocol.BytesMessage
		err = marshaler.UnmarshalMessage(packet.Data, &msg)
		if nil == err {
			event = newAcceptEvent(protocol.CMD_BYTES_MESSAGE, &msg, pevent.RemoteClient, packet.Opaque)
		}
	//发送的是StringMessage
	case protocol.CMD_STRING_MESSAGE:
		var msg protocol.StringMessage
		err = marshaler.UnmarshalMessage(packet.Data, &msg)
		if nil == err {
			event = newAcceptEvent(protocol.CMD_STRING_MESSAGE, &msg, pevent.RemoteClient, packet.Opaque)
		}
	}

	return event, err

}
