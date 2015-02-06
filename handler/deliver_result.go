package handler

import (
	. "kiteq/pipe"
	"kiteq/protocol"
	"kiteq/store"
	"log"
	"time"
)

//-------投递结果的handler
type DeliverResultHandler struct {
	BaseForwardHandler
	store store.IKiteStore
}

//------创建投递结果处理器
func NewDeliverResultHandler(name string, store store.IKiteStore) *DeliverResultHandler {
	dhandler := &DeliverResultHandler{}
	dhandler.BaseForwardHandler = NewBaseForwardHandler(name, dhandler)
	dhandler.store = store
	return dhandler
}

func (self *DeliverResultHandler) TypeAssert(event IEvent) bool {
	fevent, ok := self.cast(event)

	if ok {
		return fevent.Packet.CmdType == protocol.CMD_BYTES_MESSAGE ||
			fevent.Packet.CmdType == protocol.CMD_STRING_MESSAGE
	}
	return ok
}

func (self *DeliverResultHandler) cast(event IEvent) (val *RemoteFutureEvent, ok bool) {
	val, ok = event.(*RemoteFutureEvent)
	return
}

func (self *DeliverResultHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	fevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	// log.Printf("DeliverResultHandler|Process|%s|%t\n", self.GetName(), fevent.Futures)

	succGroup := make([]string, 0, 5)
	failGroup := make([]string, 0, 5)
	//统计回调结果
	for g, f := range fevent.Futures {
		select {
		case resp := <-f:
			ack := resp.(*protocol.DeliverAck)
			//投递成功
			if ack.GetStatus() {
				succGroup = append(succGroup, ack.GetGroupId())
			} else {
				failGroup = append(failGroup, ack.GetGroupId())
			}
		case <-time.After(100 * time.Millisecond):
			//等待结果超时
			failGroup = append(failGroup, g)
		}
	}

	//则全部投递成功
	if len(failGroup) <= 0 {
		log.Printf("DeliverResultHandler|%s|Process|ALL GROUP SEND |SUCC|%s|%s\n", self.GetName(), succGroup, failGroup)
	} else {
		//需要将失败的分组重新投递
		log.Printf("DeliverResultHandler|%s|Process|GROUP SEND |FAIL|%s|%s\n", self.GetName(), succGroup, failGroup)
	}

	ctx.SendForward(fevent)
	return nil

}
