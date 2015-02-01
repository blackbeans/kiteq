package handler

import (
	. "kiteq/pipe"
	rclient "kiteq/remoting/client"
	"kiteq/store"
)

type AccessEvent struct {
	IForwardEvent
	GroupId      string
	SecretKey    string
	remoteClient *rclient.RemotingClient
	opaque       int32
}

func NewAccessEvent(groupId, secretKey string, remoteClient *rclient.RemotingClient, opaque int32) *AccessEvent {
	return &AccessEvent{
		GroupId:      groupId,
		SecretKey:    secretKey,
		remoteClient: remoteClient,
		opaque:       opaque}

}

//消息持久化操作
type PersistentEvent struct {
	IForwardEvent
	entity       *store.MessageEntity
	remoteClient *rclient.RemotingClient
	opaque       int32
}

func NewPersistentEvent(entity *store.MessageEntity, remoteClient *rclient.RemotingClient, opaque int32) *PersistentEvent {
	return &PersistentEvent{entity: entity, remoteClient: remoteClient, opaque: opaque}

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
