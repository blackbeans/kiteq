package handler

import (
	. "kiteq/pipe"
	"kiteq/protocol"
	rclient "kiteq/remoting/client"
	"kiteq/store"
	"time"
)

type iauth interface {
	IForwardEvent
	getClient() *rclient.RemotingClient
}

type accessEvent struct {
	iauth
	groupId      string
	secretKey    string
	opaque       int32
	remoteClient *rclient.RemotingClient
}

func (self *accessEvent) getClient() *rclient.RemotingClient {
	return self.remoteClient
}

func newAccessEvent(groupId, secretKey string, remoteClient *rclient.RemotingClient, opaque int32) *accessEvent {
	access := &accessEvent{
		groupId:      groupId,
		secretKey:    secretKey,
		opaque:       opaque,
		remoteClient: remoteClient}
	return access
}

//接受消息事件
type acceptEvent struct {
	iauth
	msgType      uint8
	msg          interface{} //attach的数据message
	opaque       int32
	remoteClient *rclient.RemotingClient
}

func (self *acceptEvent) getClient() *rclient.RemotingClient {
	return self.remoteClient
}

func newAcceptEvent(msgType uint8, msg interface{}, remoteClient *rclient.RemotingClient, opaque int32) *acceptEvent {
	ae := &acceptEvent{
		msgType:      msgType,
		msg:          msg,
		opaque:       opaque,
		remoteClient: remoteClient}
	return ae
}

type txAckEvent struct {
	iauth
	txPacket     *protocol.TxACKPacket
	opaque       int32
	remoteClient *rclient.RemotingClient
}

func (self *txAckEvent) getClient() *rclient.RemotingClient {
	return self.remoteClient
}

func newTxAckEvent(txPacket *protocol.TxACKPacket, opaque int32, remoteClient *rclient.RemotingClient) *txAckEvent {
	tx := &txAckEvent{
		txPacket:     txPacket,
		opaque:       opaque,
		remoteClient: remoteClient}
	return tx
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
	IBackwardEvent
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

	if timeout > 0 {
		//统计回调结果
		for g, f := range self.futures {
			select {
			case <-time.After(timeout):
				//等待结果超时
				self.failGroups = append(self.failGroups, g)
			case resp := <-f:
				ack, ok := resp.(*protocol.DeliverAck)
				if !ok || !ack.GetStatus() {
					self.failGroups = append(self.failGroups, ack.GetGroupId())
				} else {
					self.succGroups = append(self.succGroups, ack.GetGroupId())
				}

			}
		}
	} else {
		//统计回调结果
		for _, f := range self.futures {
			select {
			case resp := <-f:
				ack, ok := resp.(*protocol.DeliverAck)
				if !ok || !ack.GetStatus() {
					self.failGroups = append(self.failGroups, ack.GetGroupId())
				} else {
					self.succGroups = append(self.succGroups, ack.GetGroupId())
				}
			}
		}
	}

}
