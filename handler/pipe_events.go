package handler

import (
	client "github.com/blackbeans/turbo/client"
	packet "github.com/blackbeans/turbo/packet"
	. "github.com/blackbeans/turbo/pipe"
	"kiteq/protocol"
	"kiteq/store"

	// 	log "github.com/blackbeans/log4go"
)

type iauth interface {
	IForwardEvent
	getClient() *client.RemotingClient
}

type accessEvent struct {
	iauth
	groupId      string
	secretKey    string
	opaque       int32
	remoteClient *client.RemotingClient
}

func (self *accessEvent) getClient() *client.RemotingClient {
	return self.remoteClient
}

func newAccessEvent(groupId, secretKey string, remoteClient *client.RemotingClient, opaque int32) *accessEvent {
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
	remoteClient *client.RemotingClient
}

func (self *acceptEvent) getClient() *client.RemotingClient {
	return self.remoteClient
}

func newAcceptEvent(msgType uint8, msg interface{}, remoteClient *client.RemotingClient, opaque int32) *acceptEvent {
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
	remoteClient *client.RemotingClient
}

func (self *txAckEvent) getClient() *client.RemotingClient {
	return self.remoteClient
}

func newTxAckEvent(txPacket *protocol.TxACKPacket, opaque int32, remoteClient *client.RemotingClient) *txAckEvent {
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
	remoteClient *client.RemotingClient
	opaque       int32
}

func newPersistentEvent(entity *store.MessageEntity, remoteClient *client.RemotingClient, opaque int32) *persistentEvent {
	return &persistentEvent{entity: entity, remoteClient: remoteClient, opaque: opaque}

}

//投递准备事件
type deliverPreEvent struct {
	IForwardEvent
	messageId      string
	header         *protocol.Header
	entity         *store.MessageEntity
	attemptDeliver chan []string
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
	messageId      string
	topic          string
	messageType    string
	expiredTime    int64
	publishtime    int64          //消息发布时间
	fly            bool           //是否为fly模式的消息
	packet         *packet.Packet //消息包
	succGroups     []string       //已经投递成功的分组
	deliverGroups  []string       //需要投递的群组
	deliverLimit   int32
	deliverCount   int32 //已经投递的次数
	attemptDeliver chan []string
}

//创建投递事件
func newDeliverEvent(messageId string, topic string, messageType string,
	publishtime int64, attemptDeliver chan []string) *deliverEvent {
	return &deliverEvent{
		messageId:      messageId,
		topic:          topic,
		messageType:    messageType,
		publishtime:    publishtime,
		attemptDeliver: attemptDeliver}
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
func (self *deliverResultEvent) wait(ch <-chan bool) {

	do := func() chan interface{} {
		var lastchan chan interface{}
		//统计回调结果
		for g, f := range self.futures {

			resp := <-f
			// log.Printf("deliverResultEvent|wait|%s\n", resp)
			ack, ok := resp.(*protocol.DeliverAck)
			if !ok || !ack.GetStatus() {
				self.deliveryFailGroups = append(self.deliveryFailGroups, g)
			} else {
				self.deliverySuccGroups = append(self.deliverySuccGroups, g)
			}

			lastchan = f
		}
		return lastchan

	}

	//wait
	select {
	//timeout
	case <-ch:
	outter:
		for g, _ := range self.futures {
			for _, sg := range self.deliverySuccGroups {
				if g == sg {
					continue outter
				}
			}
			//等待结果超时
			self.deliveryFailGroups = append(self.deliveryFailGroups, g)
		}
		//do
	case <-do():
	}

}
