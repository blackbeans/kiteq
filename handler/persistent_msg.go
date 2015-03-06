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
	kitestore     store.IKiteStore
	maxDeliverNum chan byte
}

//------创建persitehandler
func NewPersistentHandler(name string, maxDeliverWorker int, kitestore store.IKiteStore) *PersistentHandler {
	phandler := &PersistentHandler{}
	phandler.BaseForwardHandler = NewBaseForwardHandler(name, phandler)
	phandler.kitestore = kitestore
	phandler.maxDeliverNum = make(chan byte, maxDeliverWorker)
	for {
		if len(phandler.maxDeliverNum) >= maxDeliverWorker {
			break
		}
		phandler.maxDeliverNum <- 1
	}
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

	//如果是fly模式不做持久化
	if nil != pevent.entity && pevent.entity.Header.GetFly() {
		//如果是成功存储的、并且为未提交的消息，则需要发起一个ack的命令
		//发送存储结果ack
		remoteEvent := NewRemotingEvent(self.storeAck(pevent.opaque,
			pevent.entity.Header.GetMessageId(), true), []string{pevent.remoteClient.RemoteAddr()})
		ctx.SendForward(remoteEvent)

		self.send(ctx, pevent)
	} else if nil != pevent.entity {
		self.sendUnFlyMessage(ctx, pevent)
	}

	return nil
}

//发送非flymessage
func (self *PersistentHandler) sendUnFlyMessage(ctx *DefaultPipelineContext, pevent *persistentEvent) {
	//写入到持久化存储里面
	succ := self.kitestore.Save(pevent.entity)

	//发送存储结果ack
	remoteEvent := NewRemotingEvent(self.storeAck(pevent.opaque,
		pevent.entity.Header.GetMessageId(), succ), []string{pevent.remoteClient.RemoteAddr()})
	ctx.SendForward(remoteEvent)

	if succ && pevent.entity.Header.GetCommit() {
		self.send(ctx, pevent)
	} else if succ {
		//如果是成功存储的、并且为未提交的消息，则需要发起一个ack的命令
		remoteEvent := NewRemotingEvent(self.tXAck(
			pevent.entity.Header), []string{pevent.remoteClient.RemoteAddr()})
		ctx.SendForward(remoteEvent)
	} else if !succ {
		log.Printf("PersistentHandler|Process|SAVE|FAIL|%s\n", pevent.entity.Header)
	}

}

func (self *PersistentHandler) send(ctx *DefaultPipelineContext, pevent *persistentEvent) {

	f := func() {
		//启动投递当然会重投3次
		deliver := NewDeliverPreEvent(
			pevent.entity.Header.GetMessageId(),
			pevent.entity.Header,
			pevent.entity)
		ctx.SendForward(deliver)
	}
	select {
	case <-self.maxDeliverNum:
		go func() {
			defer func() {
				self.maxDeliverNum <- 1
			}()
			//启动投递
			f()
		}()
	default:
		log.Println("PersistentHandler|send|FULL|TRY SEND BY CURRENT GO ....")
		f()
	}

}

func (self *PersistentHandler) storeAck(opaque int32, messageid string, succ bool) *protocol.Packet {

	storeAck := protocol.MarshalMessageStoreAck(messageid, succ, "0:SUCC|1:FAIL")
	//响应包
	return protocol.NewRespPacket(opaque, protocol.CMD_MESSAGE_STORE_ACK, storeAck)
}

//发送事务ack信息
func (self *PersistentHandler) tXAck(
	header *protocol.Header) *protocol.Packet {

	txack := protocol.MarshalTxACKPacket(header, protocol.TX_UNKNOWN, "Server Check")
	//响应包
	return protocol.NewPacket(protocol.CMD_TX_ACK, txack)
}
