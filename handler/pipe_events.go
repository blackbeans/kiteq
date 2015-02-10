package handler

import (
	. "kiteq/pipe"
	"kiteq/protocol"
	rclient "kiteq/remoting/client"
	"kiteq/store"
	"time"
)

type accessEvent struct {
	IForwardEvent
	groupId      string
	secretKey    string
	remoteClient *rclient.RemotingClient
	opaque       int32
}

func newAccessEvent(groupId, secretKey string, remoteClient *rclient.RemotingClient, opaque int32) *accessEvent {
	return &accessEvent{
		groupId:      groupId,
		secretKey:    secretKey,
		remoteClient: remoteClient,
		opaque:       opaque}
}

//接受消息事件
type acceptEvent struct {
	IForwardEvent
	msgType      uint8
	msg          interface{} //attach的数据message
	remoteClient *rclient.RemotingClient
	opaque       int32
}

func newAcceptEvent(msgType uint8, msg interface{}, remoteClient *rclient.RemotingClient, opaque int32) *acceptEvent {
	return &acceptEvent{
		msgType:      msgType,
		msg:          msg,
		opaque:       opaque,
		remoteClient: remoteClient}
}

type txAckEvent struct {
	IForwardEvent
	txPacket *protocol.TxACKPacket
	opaque   int32
}

func newTxAckEvent(txPacket *protocol.TxACKPacket, opaque int32) *txAckEvent {
	return &txAckEvent{txPacket: txPacket, opaque: opaque}

}

//消息持久化操作
type persistentEvent struct {
	IForwardEvent
	entity       *store.MessageEntity
	remoteClient *rclient.RemotingClient
	opaque       int32
}

func newPersistentEvent(entity *store.MessageEntity, remoteClient *rclient.RemotingClient, opaque int32) *persistentEvent {
	return &persistentEvent{entity: entity, remoteClient: remoteClient, opaque: opaque}

}

//投递事件
type deliverEvent struct {
	IForwardEvent
	ttl           int32 //ttl
	messageId     string
	topic         string
	messageType   string
	expiredTime   int64
	deliverLimit  int32
	packet        *protocol.Packet //消息包
	deliverGroups []string         //需要投递的群组
	deliverCount  int32            //已经投递的次数
}

//统计投递结果的事件，决定不决定重发
type deliverResultEvent struct {
	*deliverEvent
	futures    map[string]chan interface{}
	failGroups []string
	succGroups []string
}

func newDeliverResultEvent(deliverEvent *deliverEvent, futures map[string]chan interface{}) *deliverResultEvent {
	re := &deliverResultEvent{}
	re.deliverEvent = deliverEvent
	re.futures = futures
	re.succGroups = make([]string, 0, 5)
	re.failGroups = make([]string, 0, 5)
	return re
}

//等待响应
func (self *deliverResultEvent) wait(timeout time.Duration) {
	//统计回调结果
	for g, f := range self.futures {
		select {
		case resp := <-f:
			ack := resp.(*protocol.DeliverAck)
			//投递成功
			if ack.GetStatus() {
				self.succGroups = append(self.succGroups, ack.GetGroupId())
			} else {
				self.failGroups = append(self.failGroups, ack.GetGroupId())
			}
		case <-time.After(timeout):
			//等待结果超时
			self.failGroups = append(self.failGroups, g)
		}
	}
}
