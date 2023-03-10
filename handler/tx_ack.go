package handler

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/turbo"
	log "github.com/sirupsen/logrus"
	"kiteq/store"
)

//----------------持久化的handler
type TxAckHandler struct {
	turbo.BaseForwardHandler
	kitestore store.IKiteStore
}

//------创建persitehandler
func NewTxAckHandler(name string, kitestore store.IKiteStore) *TxAckHandler {
	phandler := &TxAckHandler{}
	phandler.BaseForwardHandler = turbo.NewBaseForwardHandler(name, phandler)
	phandler.kitestore = kitestore
	return phandler
}

func (self *TxAckHandler) TypeAssert(event turbo.IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *TxAckHandler) cast(event turbo.IEvent) (val *txAckEvent, ok bool) {
	val, ok = event.(*txAckEvent)
	return
}

func (self *TxAckHandler) Process(ctx *turbo.DefaultPipelineContext, event turbo.IEvent) error {

	// log.Debugf( "TxAckHandler|Process|%s|%t", self.GetName(), event)

	pevent, ok := self.cast(event)
	if !ok {
		return turbo.ERROR_INVALID_EVENT_TYPE
	}

	h := pevent.txPacket.GetHeader()
	//提交或者回滚
	if pevent.txPacket.GetStatus() == int32(protocol.TX_COMMIT) {

		succ := self.kitestore.Commit(h.GetTopic(), h.GetMessageId())

		if succ {
			//发起投递事件
			//启动异步协程处理分发逻辑
			preevent := NewDeliverPreEvent(h.GetMessageId(), h, nil)
			ctx.SendForward(preevent)

		} else {
			//失败了等待下次recover询问
			// log.Debugf(  "TxAckHandler|%s|Process|Commit|FAIL|%s|%s", self.GetName(), h.GetMessageId(), succ)
		}

	} else if pevent.txPacket.GetStatus() == int32(protocol.TX_ROLLBACK) {
		succ := self.kitestore.Rollback(h.GetTopic(), h.GetMessageId())
		if !succ {
			log.Warnf("TxAckHandler|%s|Process|Rollback|FAIL|%s|%s|%s",
				self.GetName(), h.GetMessageId(), pevent.txPacket.GetFeedback(), succ)
		}

	} else {
		//UNKNOWN其他的不处理

	}
	ctx.SendForward(&turbo.SunkEvent{})
	return nil
}
