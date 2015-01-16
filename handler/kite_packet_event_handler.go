package handler

import (
	"errors"
	"github.com/golang/protobuf/proto"
	"go-kite/protocol"
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
	packet := self.decode(pevent.packet)
	if nil == packet {
		//如果为空
		return INVALID_PACKET_ERROR
	}

	event, err := self.wrapEvent(pevent, packet)
	if nil != err {
		log.Printf("PacketHandler|Process|wrapEvent|FAIL|%t\n", err)
	} else {
		//向后投递
		ctx.SendForward(event)
	}
	return err

}

func (self *PacketHandler) decode(packet []byte) *protocol.RequestPacket {

	//解析packet的数据位requestPacket
	reqPacket := &protocol.RequestPacket{}
	err := reqPacket.Unmarshal(packet)
	if nil != err {
		//ignore
		log.Printf("PacketHandler|decode|INALID PACKET|%t\n", packet)
		return nil
	} else {
		return reqPacket
	}
}

func (self *PacketHandler) wrapEvent(pevent *PacketEvent, packet *protocol.RequestPacket) (IEvent, error) {
	var err error
	var event IEvent
	//根据类型反解packet
	switch packet.CmdType {
	//连接的元数据
	case protocol.CMD_CONN_META:
		connMeta := &protocol.ConnectioMetaPacket{}
		err = proto.Unmarshal(packet.Data, connMeta)
		if nil == err {
			event = NewAccessEvent(connMeta.GetGroupId(), connMeta.GetSecretKey(), pevent.session, packet.Opaque)
		}
	//心跳
	case protocol.CMD_TYPE_HEARTBEAT:
		hearbeat := &protocol.HeartBeatACKPacket{}
		err = proto.Unmarshal(packet.Data, hearbeat)

	case protocol.CMD_TX_ACK:
		txAck := &protocol.TranscationACKPacket{}
		err = proto.Unmarshal(packet.Data, txAck)
	//发送的是bytesmessage
	case protocol.CMD_TYPE_BYTES_MESSAGE:
		msg := &protocol.BytesMessage{}
		err = proto.Unmarshal(packet.Data, msg)
		if nil == err {
			event = NewAcceptEvent(protocol.CMD_TYPE_BYTES_MESSAGE, msg, pevent.session, packet.Opaque)
		}
	//发送的是StringMessage
	case protocol.CMD_TYPE_STRING_MESSAGE:
		msg := &protocol.StringMessage{}
		err = proto.Unmarshal(packet.Data, msg)
		if nil == err {
			event = NewAcceptEvent(protocol.CMD_TYPE_STRING_MESSAGE, msg, pevent.session, packet.Opaque)
		}
	}

	return event, err

}
