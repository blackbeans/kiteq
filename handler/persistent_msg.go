package handler

import (
	"errors"
	. "kiteq/pipe"
	"kiteq/protocol"
	"kiteq/store"
	"log"
)

var ERROR_PERSISTENT = errors.New("persistent msg error!")

//----------------持久化的handler
type PersistentHandler struct {
	BaseForwardHandler
	kitestore store.IKiteStore
}

//------创建persitehandler
func NewPersistentHandler(name string, kitestore store.IKiteStore) *PersistentHandler {
	phandler := &PersistentHandler{}
	phandler.BaseForwardHandler = NewBaseForwardHandler(name, phandler)
	phandler.kitestore = kitestore
	return phandler
}

func (self *PersistentHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *PersistentHandler) cast(event IEvent) (val *persistentEvent, ok bool) {
	val, ok = event.(*persistentEvent)
	return
}

func (self *PersistentHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	pevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}
	// log.Printf("PersistentHandler|Process|%s|%t\n", self.GetName(), event)

	//写入到持久化存储里面
	succ := self.kitestore.Save(pevent.entity)
	if succ && pevent.entity.Header.GetCommit() {
		//启动异步协程处理分发逻辑
		go func() {
			deliver := &deliverEvent{
				messageId:   pevent.entity.Header.GetMessageId(),
				topic:       pevent.entity.Header.GetTopic(),
				messageType: pevent.entity.Header.GetMessageType(),
				ttl:         2}
			ctx.SendForward(deliver)

		}()
	} else if succ {
		//如果是成功存储的、并且为未提交的消息，则需要发起一个ack的命令
		go func() {
			remoteEvent := NewRemotingEvent(self.tXAck(
				pevent.entity.Header), []string{pevent.remoteClient.RemoteAddr()})
			ctx.SendForward(remoteEvent)
		}()
	} else {
		log.Printf("PersistentHandler|Process|SAVE|FAIL|%t\n", pevent.entity)
	}

	//发送存储结果ack
	remoteEvent := NewRemotingEvent(self.storeAck(pevent.opaque,
		pevent.entity.Header.GetMessageId(), succ), []string{pevent.remoteClient.RemoteAddr()})
	ctx.SendForward(remoteEvent)
	return nil
}

func (self *PersistentHandler) storeAck(opaque int32, messageid string, succ bool) *protocol.Packet {

	storeAck := protocol.MarshalMessageStoreAck(messageid, succ, "0:SUCC|1:FAIL")
	//响应包
	return protocol.NewRespPacket(opaque, protocol.CMD_MESSAGE_STORE_ACK, storeAck)
}

//发送事务ack信息
func (self PersistentHandler) tXAck(
	header *protocol.Header) *protocol.Packet {

	txack := protocol.MarshalTxACKPacket(header, protocol.TX_UNKNOWN, "Server Check")
	//响应包
	return protocol.NewPacket(protocol.CMD_TX_ACK, txack)
}
