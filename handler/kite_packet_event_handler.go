package handler

import (
	"errors"
	"github.com/golang/protobuf/proto"
	"kiteq/protocol"
	"log"
)

//远程操作的PacketHandler

type PacketHandler struct {
	BaseForwardHandler
	IEventProcessor
}

func NewPacketHandler(name string) *PacketHandler {
	packetHandler := &PacketHandler{}
	packetHandler.name = name
	packetHandler.processor = packetHandler
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

	// log.Printf("PacketHandler|Process|%t\n", event)

	pevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}
	//decode2requestPacket
	tlv, err := protocol.UnmarshalTLV(pevent.packet)
	if nil == tlv || nil != err {
		log.Printf("PacketHandler|UnmarshalTLV|FAIL|%s|%s\n", err, pevent.packet)
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
		connMeta := &protocol.ConnMeta{}
		err = proto.Unmarshal(packet.Data, connMeta)
		if nil == err {
			event = NewAccessEvent(connMeta.GetGroupId(), connMeta.GetSecretKey(), pevent.session, packet.Opaque)
		}
		//连接授权确认
	case protocol.CMD_CONN_AUTH:
		auth := &protocol.ConnAuthAck{}
		err = proto.Unmarshal(packet.Data, auth)

	//心跳
	case protocol.CMD_HEARTBEAT:
		hearbeat := &protocol.HeartBeat{}
		err = proto.Unmarshal(packet.Data, hearbeat)
		//投递结果确认
	case protocol.CMD_DELIVERY_ACK:
		delAck := &protocol.DeliveryAck{}
		err = proto.Unmarshal(packet.Data, delAck)

		//消息持久化
	case protocol.CMD_MESSAGE_STORE_ACK:
		pesisteAck := &protocol.MessageStoreAck{}
		err = proto.Unmarshal(packet.Data, pesisteAck)

	case protocol.CMD_TX_ACK:
		txAck := &protocol.TxACKPacket{}
		err = proto.Unmarshal(packet.Data, txAck)
	//发送的是bytesmessage
	case protocol.CMD_BYTES_MESSAGE:
		msg := &protocol.BytesMessage{}
		err = proto.Unmarshal(packet.Data, msg)
		if nil == err {
			event = NewAcceptEvent(protocol.CMD_BYTES_MESSAGE, msg, pevent.session, packet.Opaque)
		}
	//发送的是StringMessage
	case protocol.CMD_STRING_MESSAGE:
		msg := &protocol.StringMessage{}
		err = proto.Unmarshal(packet.Data, msg)
		if nil == err {
			event = NewAcceptEvent(protocol.CMD_STRING_MESSAGE, msg, pevent.session, packet.Opaque)
		}
	}

	return event, err

}
