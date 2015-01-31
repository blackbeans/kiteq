package handler

import (
	"errors"
	"kiteq/protocol"
	"kiteq/store"
	// "log"
)

//--------------------如下为具体的处理Handler
type AcceptHandler struct {
	BaseForwardHandler
	IEventProcessor
}

func NewAcceptHandler(name string) *AcceptHandler {
	ahandler := &AcceptHandler{}
	ahandler.name = name
	ahandler.processor = ahandler
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
	// log.Printf("AcceptHandler|Process|%t\n", event)

	acceptEvent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}
	//这里处理一下acceptEvent,做一下校验
	var msg *store.MessageEntity
	switch acceptEvent.msgType {
	case protocol.CMD_BYTES_MESSAGE:
		msg = store.NewBytesMessageEntity(acceptEvent.msg.(*protocol.BytesMessage))
	case protocol.CMD_STRING_MESSAGE:
		msg = store.NewStringMessageEntity(acceptEvent.msg.(*protocol.StringMessage))
	default:
		//这只是一个bug不支持的数据类型能给你
	}

	if nil != msg {
		pevent := NewPersistentEvent(msg, acceptEvent.session, acceptEvent.opaque)
		ctx.SendForward(pevent)
		return nil
	}

	return INVALID_MSG_TYPE_ERROR
}
