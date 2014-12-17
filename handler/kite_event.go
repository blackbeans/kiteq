package handler

import (
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

//接受消息事件
type AcceptEvent struct {
	IForwardEvent
	msgType uint8
	msg     interface{} //attach的数据message
}

func NewAcceptEvent(msgType uint8, msg interface{}) *AcceptEvent {
	return &AcceptEvent{msgType: msgType, msg: msg}
}

//消息持久化操作
type PersistentEvent struct {
	IForwardEvent
	entity *store.MessageEntity
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

//统计投递结果的事件，决定不决定重发
type DeliverResultEvent struct {
	DeliverEvent
	FailGroupds []string
	SuccGroupds []string
}
