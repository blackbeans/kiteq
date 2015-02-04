package handler

import (
	"errors"
	. "kiteq/pipe"
	"kiteq/protocol"
	"log"
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
	//decode2requestPacket
	tlv, err := protocol.UnmarshalTLV(pevent.Packet)
	if nil == tlv || nil != err {
		log.Printf("PacketHandler|%s|UnmarshalTLV|FAIL|%s|%s\n", self.GetName(), err, pevent.Packet)
		//如果为空
		return INVALID_PACKET_ERROR
	}

	cevent, err := self.handlePacket(pevent, tlv)
	if nil != err {
		return err
	}
	ctx.SendForward(cevent)
	return nil
}

//对于响应事件

//对于请求事件
func (self *PacketHandler) handlePacket(pevent *PacketEvent, packet *protocol.Packet) (IEvent, error) {
	var err error
	var event IEvent
	//根据类型反解packet
	switch packet.CmdType {
	//连接的元数据
	case protocol.CMD_CONN_META:
		var connMeta protocol.ConnMeta
		err = protocol.UnmarshalPbMessage(packet.Data, &connMeta)
		if nil == err {
			meta := &connMeta
			event = NewAccessEvent(meta.GetGroupId(), meta.GetSecretKey(), pevent.RemoteClient, packet.Opaque)
		}

	//心跳
	case protocol.CMD_HEARTBEAT:
		var hearbeat protocol.HeartBeat
		err = protocol.UnmarshalPbMessage(packet.Data, &hearbeat)
		if nil == err {
			hb := &hearbeat
			event = NewHeartbeatEvent(pevent.RemoteClient, packet.Opaque, hb.GetVersion())
		}
		//投递结果确认
	case protocol.CMD_DELIVERY_ACK:
		var delAck protocol.DeliveryAck
		err = protocol.UnmarshalPbMessage(packet.Data, &delAck)

	case protocol.CMD_TX_ACK:
		var txAck protocol.TxACKPacket
		err = protocol.UnmarshalPbMessage(packet.Data, &txAck)
		if nil == err {
			event = NewTxAckEvent(&txAck, packet.Opaque)
		}

	//发送的是bytesmessage
	case protocol.CMD_BYTES_MESSAGE:
		var msg protocol.BytesMessage
		err = protocol.UnmarshalPbMessage(packet.Data, &msg)
		if nil == err {
			event = NewAcceptEvent(protocol.CMD_BYTES_MESSAGE, &msg, pevent.RemoteClient, packet.Opaque)
		}
	//发送的是StringMessage
	case protocol.CMD_STRING_MESSAGE:
		var msg protocol.StringMessage
		err = protocol.UnmarshalPbMessage(packet.Data, &msg)
		if nil == err {
			event = NewAcceptEvent(protocol.CMD_STRING_MESSAGE, &msg, pevent.RemoteClient, packet.Opaque)
		}
	}

	return event, err

}
