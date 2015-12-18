package handler

import (
	"fmt"
	"github.com/blackbeans/turbo"
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

func newAcceptEvent(msgType uint8, msg interface{},
	remoteClient *client.RemotingClient, opaque int32) *acceptEvent {
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

type GroupFuture struct {
	*turbo.Future
	resp    interface{}
	groupId string
}

func (self GroupFuture) String() string {

	return fmt.Sprintf("groupId:%s[%s],%s,err:%s", self.groupId, self.TargetHost, self.resp, self.Err)
}

//统计投递结果的事件，决定不决定重发
type deliverResultEvent struct {
	*deliverEvent
	IBackwardEvent
	futures           map[string]*turbo.Future
	failGroupFuture   []GroupFuture
	succGroupFuture   []GroupFuture
	deliverFailGroups []string
	deliverSuccGroups []string
}

func newDeliverResultEvent(deliverEvent *deliverEvent, futures map[string]*turbo.Future) *deliverResultEvent {
	re := &deliverResultEvent{}
	re.deliverEvent = deliverEvent
	re.futures = futures
	re.succGroupFuture = make([]GroupFuture, 0, 5)
	re.failGroupFuture = make([]GroupFuture, 0, 5)

	return re
}

//等待响应
func (self *deliverResultEvent) wait(ch chan bool) bool {
	timeout := false
	//等待回调结果
	for g, f := range self.futures {
		resp, err := f.Get(ch)
		if err == turbo.TIMEOUT_ERROR {
			timeout = true
			gf := GroupFuture{f, resp, g}
			gf.Err = err
			self.failGroupFuture = append(self.failGroupFuture, gf)
		} else if nil != resp {
			ack, ok := resp.(*protocol.DeliverAck)
			if !ok || !ack.GetStatus() {
				self.failGroupFuture = append(self.failGroupFuture, GroupFuture{f, resp, g})
			} else {
				self.succGroupFuture = append(self.succGroupFuture, GroupFuture{f, resp, g})
			}
		}
	}

	fg := make([]string, 0, len(self.failGroupFuture))
	for _, g := range self.failGroupFuture {
		fg = append(fg, g.groupId)
	}
	self.deliverFailGroups = fg

	sg := make([]string, 0, len(self.succGroupFuture))
	for _, g := range self.succGroupFuture {
		sg = append(sg, g.groupId)
	}
	self.deliverSuccGroups = sg
	return timeout
}
