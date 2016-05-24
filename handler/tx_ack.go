package handler

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/store"
	log "github.com/blackbeans/log4go"
	p "github.com/blackbeans/turbo/pipe"
)

//----------------持久化的handler
type TxAckHandler struct {
	p.BaseForwardHandler
	kitestore store.IKiteStore
}

//------创建persitehandler
func NewTxAckHandler(name string, kitestore store.IKiteStore) *TxAckHandler {
	phandler := &TxAckHandler{}
	phandler.BaseForwardHandler = p.NewBaseForwardHandler(name, phandler)
	phandler.kitestore = kitestore
	return phandler
}

func (self *TxAckHandler) TypeAssert(event p.IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *TxAckHandler) cast(event p.IEvent) (val *txAckEvent, ok bool) {
	val, ok = event.(*txAckEvent)
	return
}

func (self *TxAckHandler) Process(ctx *p.DefaultPipelineContext, event p.IEvent) error {

	// log.DebugLog("kite_handler", "TxAckHandler|Process|%s|%t\n", self.GetName(), event)

	pevent, ok := self.cast(event)
	if !ok {
		return p.ERROR_INVALID_EVENT_TYPE
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
			// log.DebugLog("kite_handler",  "TxAckHandler|%s|Process|Commit|FAIL|%s|%s\n", self.GetName(), h.GetMessageId(), succ)
		}

	} else if pevent.txPacket.GetStatus() == int32(protocol.TX_ROLLBACK) {
		succ := self.kitestore.Rollback(h.GetTopic(), h.GetMessageId())
		if !succ {
			log.WarnLog("kite_handler", "TxAckHandler|%s|Process|Rollback|FAIL|%s|%s|%s",
				self.GetName(), h.GetMessageId(), pevent.txPacket.GetFeedback(), succ)
		}

	} else {
		//UNKNOWN其他的不处理

	}
	ctx.SendForward(&p.SunkEvent{})
	return nil
}
