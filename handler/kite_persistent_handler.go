package handler

import (
	"errors"
	"kiteq/protocol"
	"kiteq/store"
	// "log"
)

var ERROR_PERSISTENT = errors.New("persistent msg error!")

//----------------持久化的handler
type PersistentHandler struct {
	BaseForwardHandler
	IEventProcessor
	kitestore store.IKiteStore
}

//------创建persitehandler
func NewPersistentHandler(name string, kitestore store.IKiteStore) *PersistentHandler {
	phandler := &PersistentHandler{}
	phandler.name = name
	phandler.kitestore = kitestore
	phandler.processor = phandler
	return phandler
}

func (self *PersistentHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *PersistentHandler) cast(event IEvent) (val *PersistentEvent, ok bool) {
	val, ok = event.(*PersistentEvent)
	return
}

func (self *PersistentHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	// log.Printf("PersistentHandler|Process|%t\n", event)

	pevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	//写入到持久化存储里面
	succ := self.kitestore.Save(pevent.entity)
	if succ && pevent.entity.Header.GetCommited() {
		//启动异步协程处理分发逻辑
		go func() {
			deliver := &DeliverEvent{}
			deliver.MessageId = pevent.entity.Header.GetMessageId()
			deliver.Topic = pevent.entity.Header.GetTopic()
			deliver.MessageType = pevent.entity.Header.GetMessageType()
			deliver.ExpiredTime = pevent.entity.Header.GetExpiredTime()
			ctx.SendForward(event)

		}()
	} else if succ {
		//如果是成功存储的、并且为未提交的消息，则需要发起一个ack的命令
		go func() {
			remoteEvent := newRemotingEvent(self.tXAck(pevent.opaque,
				pevent.entity.Header.GetMessageId()))
			ctx.SendForward(remoteEvent)
		}()
	}

	//发送存储结果ack
	remoteEvent := newRemotingEvent(self.storeAck(pevent.opaque,
		pevent.entity.Header.GetMessageId(), succ), pevent.session)
	ctx.SendForward(remoteEvent)
	return nil
}

func (self *PersistentHandler) storeAck(opaque int32, messageid string, succ bool) *protocol.Packet {

	storeAck := protocol.MarshalMessageStoreAck(messageid, succ, "0:SUCC|1:FAIL")
	//响应包
	packet := &protocol.Packet{
		Opaque:  opaque,
		Data:    storeAck,
		CmdType: protocol.CMD_MESSAGE_STORE_ACK}
	return packet
}

//发送事务ack信息
func (self PersistentHandler) tXAck(opaque int32,
	messageid string) *protocol.Packet {

	txack := protocol.MarshalTxACKPacket(messageid, protocol.TX_UNKNOW)
	//响应包
	packet := &protocol.Packet{
		Opaque:  opaque,
		Data:    txack,
		CmdType: protocol.CMD_TX_ACK}

	return packet
}
