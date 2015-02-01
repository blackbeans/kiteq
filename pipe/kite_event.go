package pipe

import (
	"kiteq/protocol"
	rclient "kiteq/remoting/client"
)

type IEvent interface {
}

//返回的事件
type IBackwardEvent interface {
	IEvent
}

//向前的事件
type IForwardEvent interface {
	IEvent
}

type PacketEvent struct {
	IForwardEvent
	Packet       []byte //本次的数据包
	RemoteClient *rclient.RemotingClient
}

func NewPacketEvent(remoteClient *rclient.RemotingClient, packet []byte) *PacketEvent {
	return &PacketEvent{Packet: packet, RemoteClient: remoteClient}
}

//接受消息事件
type AcceptEvent struct {
	IForwardEvent
	MsgType      uint8
	Msg          interface{} //attach的数据message
	RemoteClient *rclient.RemotingClient
	Opaque       int32
}

func NewAcceptEvent(msgType uint8, msg interface{}, remoteClient *rclient.RemotingClient, opaque int32) *AcceptEvent {
	return &AcceptEvent{
		MsgType:      msgType,
		Msg:          msg,
		Opaque:       opaque,
		RemoteClient: remoteClient}
}

//远程操作事件
type RemotingEvent struct {
	TargetHost []string         //发送的特定hostport
	GroupIds   []string         //本次发送的分组
	Packet     *protocol.Packet //tlv的packet数据
}

func NewRemotingEvent(packet *protocol.Packet, targetHost []string, groupIds ...string) *RemotingEvent {
	revent := &RemotingEvent{
		TargetHost: targetHost,
		GroupIds:   groupIds,
		Packet:     packet}
	return revent
}

//到头的事件
type SunkEvent struct {
	IForwardEvent
}
