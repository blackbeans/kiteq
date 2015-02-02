package chandler

import (
	"errors"
	. "kiteq/pipe"
	"kiteq/protocol"
	"log"
)

//--------------------如下为具体的处理Handler
type AcceptHandler struct {
	BaseForwardHandler
}

func NewAcceptHandler(name string) *AcceptHandler {
	ahandler := &AcceptHandler{}
	ahandler.BaseForwardHandler = NewBaseForwardHandler(name, ahandler)
	return ahandler
}

func (self *AcceptHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *AcceptHandler) cast(event IEvent) (val *AcceptEvent, ok bool) {
	val, ok = event.(*AcceptEvent)
	return
}

var INVALID_MSG_TYPE_ERROR = errors.New("INVALID MSG TYPE !")

func (self *AcceptHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {
	// log.Printf("AcceptHandler|Process|%s|%t\n", self.GetName(), event)

	acceptEvent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	switch acceptEvent.MsgType {
	case protocol.CMD_CHECK_MESSAGE:
		//回调事务完成的监听器
		log.Printf("AcceptHandler|Check Message|%s\n", acceptEvent.Msg)
	case protocol.CMD_STRING_MESSAGE, protocol.CMD_BYTES_MESSAGE:
		//这里应该回调消息监听器然后发送处理结果
		log.Printf("AcceptHandler|Recieve Message|%s\n", acceptEvent.Msg)
	default:
		return INVALID_MSG_TYPE_ERROR
	}

	return nil

}
