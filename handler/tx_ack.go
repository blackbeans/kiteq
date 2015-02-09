package handler

import (
	. "kiteq/pipe"
	"kiteq/protocol"
	"kiteq/store"
	"log"
)

//----------------持久化的handler
type TxAckHandler struct {
	BaseForwardHandler
	kitestore store.IKiteStore
}

//------创建persitehandler
func NewTxAckHandler(name string, kitestore store.IKiteStore) *TxAckHandler {
	phandler := &TxAckHandler{}
	phandler.BaseForwardHandler = NewBaseForwardHandler(name, phandler)
	phandler.kitestore = kitestore
	return phandler
}

func (self *TxAckHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *TxAckHandler) cast(event IEvent) (val *txAckEvent, ok bool) {
	val, ok = event.(*txAckEvent)
	return
}

func (self *TxAckHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	// log.Printf("TxAckHandler|Process|%s|%t\n", self.GetName(), event)

	pevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	//提交或者回滚
	if pevent.txPacket.GetStatus() == int32(protocol.TX_COMMIT) {
		succ := self.kitestore.Commit(pevent.txPacket.GetMessageId())
		log.Printf("TxAckHandler|%s|Process|Commit|%s|%s\n", self.GetName(), pevent.txPacket.GetMessageId(), succ)

		if succ {
			//发起投递事件
			//启动异步协程处理分发逻辑
			deliver := &deliverEvent{}
			deliver.messageId = pevent.txPacket.GetMessageId()
			deliver.topic = pevent.txPacket.GetTopic()
			deliver.messageType = pevent.txPacket.GetMessageType()
			ctx.SendForward(deliver)

		}

	} else if pevent.txPacket.GetStatus() == int32(protocol.TX_ROLLBACK) {
		succ := self.kitestore.Rollback(pevent.txPacket.GetMessageId())
		log.Printf("TxAckHandler|%s|Process|Rollback|%s|%s|%s\n", self.GetName(), pevent.txPacket.GetMessageId(), pevent.txPacket.GetFeedback(), succ)
	} else {
		//UNKNOWN其他的不处理

	}
	ctx.SendForward(&SunkEvent{})
	return nil
}
