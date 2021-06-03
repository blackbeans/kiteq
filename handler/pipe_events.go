package handler

import (
	"fmt"
	"kiteq/store"
	"time"

	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/registry"
	"github.com/blackbeans/turbo"
)

type iauth interface {
	turbo.IForwardEvent
	getClient() *turbo.TClient
}

type accessEvent struct {
	iauth
	connMeta     protocol.ConnMeta
	opaque       uint32
	remoteClient *turbo.TClient
}

func (self *accessEvent) getClient() *turbo.TClient {
	return self.remoteClient
}

func newAccessEvent(connMeta protocol.ConnMeta, remoteClient *turbo.TClient, opaque uint32) *accessEvent {
	access := &accessEvent{
		connMeta:     connMeta,
		opaque:       opaque,
		remoteClient: remoteClient}
	return access
}

//接受消息事件
type acceptEvent struct {
	iauth
	msgType uint8
	msg     interface{} //attach的数据message
	opaque  uint32
	client  *turbo.TClient
}

func (self *acceptEvent) getClient() *turbo.TClient {
	return self.client
}

func newAcceptEvent(msgType uint8, msg interface{},
	remoteClient *turbo.TClient, opaque uint32) *acceptEvent {
	ae := &acceptEvent{
		msgType: msgType,
		msg:     msg,
		opaque:  opaque,
		client:  remoteClient}
	return ae
}

type txAckEvent struct {
	iauth
	txPacket     *protocol.TxACKPacket
	opaque       uint32
	remoteClient *turbo.TClient
}

func (self *txAckEvent) getClient() *turbo.TClient {
	return self.remoteClient
}

func newTxAckEvent(txPacket *protocol.TxACKPacket, opaque uint32, remoteClient *turbo.TClient) *txAckEvent {
	tx := &txAckEvent{
		txPacket:     txPacket,
		opaque:       opaque,
		remoteClient: remoteClient}
	return tx
}

//投递策略
type persistentEvent struct {
	turbo.IForwardEvent
	entity       *store.MessageEntity
	remoteClient *turbo.TClient
	opaque       uint32
}

func newPersistentEvent(entity *store.MessageEntity, remoteClient *turbo.TClient, opaque uint32) *persistentEvent {
	return &persistentEvent{entity: entity, remoteClient: remoteClient, opaque: opaque}

}

//投递准备事件
type deliverPreEvent struct {
	turbo.IForwardEvent
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
	turbo.IForwardEvent
	turbo.IBackwardEvent
	header *protocol.Header
	// fly            bool           //是否为fly模式的消息
	packet         *turbo.Packet //消息包
	succGroups     []string      //已经投递成功的分组
	deliverGroups  []string      //需要投递的群组
	deliverLimit   int32
	deliverCount   int32 //已经投递的次数
	attemptDeliver chan []string
	limiters       map[string]*turbo.BurstyLimiter
	groupBinds     map[string]registry.Binding //本次投递的订阅关系
}

//创建投递事件
func newDeliverEvent(header *protocol.Header, attemptDeliver chan []string) *deliverEvent {
	return &deliverEvent{
		header:         header,
		attemptDeliver: attemptDeliver}
}

type GroupFuture struct {
	*turbo.Future
	resp    interface{}
	groupId string
}

func (self GroupFuture) String() string {
	ack, ok := self.resp.(*protocol.DeliverAck)
	if ok {
		return fmt.Sprintf("[%s@%s,resp:(status:%v,feedback:%s),err:%v]", self.TargetHost, self.groupId, ack.GetStatus(), ack.GetFeedback(), self.Err)
	}
	return fmt.Sprintf("[%s@%s,resp:%v,err:%v]", self.TargetHost, self.groupId, self.resp, self.Err)
}

//统计投递结果的事件，决定不决定重发
type deliverResultEvent struct {
	*deliverEvent
	turbo.IBackwardEvent
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
func (self *deliverResultEvent) wait(timeout time.Duration, groupBinds map[string]registry.Binding) bool {
	istimeout := false
	latch := make(chan time.Time, 1)
	t := time.AfterFunc(timeout, func() {
		close(latch)
	})

	defer t.Stop()
	tch := (<-chan time.Time)(latch)

	//等待回调结果
	for g, f := range self.futures {

		resp, err := f.Get(tch)

		if err == turbo.ERR_TIMEOUT {
			istimeout = true
		} else if nil != resp {
			ack, ok := resp.(*protocol.DeliverAck)
			if !ok || !ack.GetStatus() {
				self.failGroupFuture = append(self.failGroupFuture, GroupFuture{f, resp, g})
			} else {
				self.succGroupFuture = append(self.succGroupFuture, GroupFuture{f, resp, g})
			}
		}

		if nil != err {
			//如果没有存在存活机器并且当前分组的订阅关系
			//是一个非持久订阅那么就认为成功的
			if err == turbo.ERR_NO_HOSTS {
				b, ok := groupBinds[g]
				if ok && !b.Persistent {
					f.Err = fmt.Errorf("All Clients Offline ! Bind[%v]", b)
					self.succGroupFuture = append(self.succGroupFuture, GroupFuture{f, resp, g})
					continue
				}
			}
			//投递失败的情况
			{
				gf := GroupFuture{f, resp, g}
				gf.Err = err
				self.failGroupFuture = append(self.failGroupFuture, gf)
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
	return istimeout
}
