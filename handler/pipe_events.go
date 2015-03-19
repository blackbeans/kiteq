package handler

import (
	. "kiteq/pipe"
	"kiteq/protocol"
	rclient "kiteq/remoting/client"
	"kiteq/store"
	// "log"
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

//投递策略
type persistentEvent struct {
	IForwardEvent
	entity       *store.MessageEntity
	remoteClient *rclient.RemotingClient
	opaque       int32
}

func newPersistentEvent(entity *store.MessageEntity, remoteClient *rclient.RemotingClient, opaque int32) *persistentEvent {
	return &persistentEvent{entity: entity, remoteClient: remoteClient, opaque: opaque}

}

//投递准备事件
type deliverPreEvent struct {
	IForwardEvent
	messageId string
	header    *protocol.Header
	entity    *store.MessageEntity
}

func NewDeliverPreEvent(messageId string, header *protocol.Header,
	entity *store.MessageEntity) *deliverPreEvent {
	return &deliverPreEvent{
		messageId: messageId,
		header:    header,
		entity:    entity}
}

//投递事件
type deliverEvent struct {
	IForwardEvent
	messageId     string
	topic         string
	messageType   string
	expiredTime   int64
	publishtime   int64            //消息发布时间
	fly           bool             //是否为fly模式的消息
	packet        *protocol.Packet //消息包
	succGroups    []string         //已经投递成功的分组
	deliverGroups []string         //需要投递的群组
	deliverLimit  int32
	deliverCount  int32 //已经投递的次数
}

//创建投递事件
func newDeliverEvent(messageId string, topic string, messageType string, publishtime int64) *deliverEvent {
	return &deliverEvent{
		messageId:   messageId,
		topic:       topic,
		messageType: messageType,
		publishtime: publishtime}
}

//统计投递结果的事件，决定不决定重发
type deliverResultEvent struct {
	*deliverEvent
	IBackwardEvent
	futures            map[string]chan interface{}
	deliveryFailGroups []string
	deliverySuccGroups []string
}

func newDeliverResultEvent(deliverEvent *deliverEvent, futures map[string]chan interface{}) *deliverResultEvent {
	re := &deliverResultEvent{}
	re.deliverEvent = deliverEvent
	re.futures = futures
	re.deliverySuccGroups = make([]string, 0, 5)
	re.deliveryFailGroups = make([]string, 0, 5)
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
				self.deliveryFailGroups = append(self.deliveryFailGroups, g)
				// log.Printf("deliverResultEvent|wait|timeout\n")
			case resp := <-f:
				// log.Printf("deliverResultEvent|wait|%s\n", resp)
				ack, ok := resp.(*protocol.DeliverAck)
				if !ok || !ack.GetStatus() {
					self.deliveryFailGroups = append(self.deliveryFailGroups, g)
				} else {
					self.deliverySuccGroups = append(self.deliverySuccGroups, g)
				}
			}
		}
	} else {
		//统计回调结果
		for g, f := range self.futures {
			select {
			case resp := <-f:
				ack, ok := resp.(*protocol.DeliverAck)
				if !ok || !ack.GetStatus() {
					self.deliveryFailGroups = append(self.deliveryFailGroups, g)
				} else {
					self.deliverySuccGroups = append(self.deliverySuccGroups, g)
				}
			}
		}
	}

}
