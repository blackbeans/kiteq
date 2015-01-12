package handler

import (
	"go-kite/protocol"
	"go-kite/remoting/session"
	"go-kite/store"
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
	packet  []byte           //本次的数据包
	session *session.Session //填充session
}

func NewPacketEvent(session *session.Session, packet []byte) *PacketEvent {
	return &PacketEvent{packet: packet, session: session}
}

type AccessEvent struct {
	IForwardEvent
	GroupId   string
	SecretKey string
	session   *session.Session
}

func NewAccessEvent(groupId, secretKey string, session *session.Session) *AccessEvent {
	return &AccessEvent{
		GroupId:   groupId,
		SecretKey: secretKey,
		session:   session}

}

//接受消息事件
type AcceptEvent struct {
	IForwardEvent
	msgType uint8
	msg     interface{} //attach的数据message
	session *session.Session
}

func NewAcceptEvent(msgType uint8, msg interface{}, session *session.Session) *AcceptEvent {
	return &AcceptEvent{msgType: msgType, msg: msg, session: session}
}

//消息持久化操作
type PersistentEvent struct {
	IForwardEvent
	entity  *store.MessageEntity
	session *session.Session
}

func NewPersistentEvent(entity *store.MessageEntity, session *session.Session) *PersistentEvent {
	return &PersistentEvent{entity: entity, session: session}

}

//投递事件
type DeliverEvent struct {
	IForwardEvent
	MessageId     string   //消息的messageId用于查询
	Topic         string   //消息的topic
	MessageType   string   //消息的messageType
	DeliverGroups []string //需要投递的群组
	ExpiredTime   int64    //消息过期时间
}

//远程操作事件
type RemotingEvent struct {
	sessions []*session.Session  //本次发送的session信息
	packet   protocol.ITLVPacket //tlv的packet数据
}

//统计投递结果的事件，决定不决定重发
type DeliverResultEvent struct {
	DeliverEvent
	FailGroupds []string
	SuccGroupds []string
}
