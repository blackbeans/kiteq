package handler

import (
	"go-kite/remoting/protocol"
	"go-kite/store"
)

//--------------------如下为具体的处理Handler
type AcceptHandler struct {
	IForwardHandler
	name string
}

func NewAcceptHandler(name string) *AcceptHandler {
	return &AcceptHandler{
		name: name}
}

func (self *AcceptHandler) GetName() string {
	return self.name
}

func (self *AcceptHandler) AcceptEvent(event IEvent) bool {
	//是否可以处理当前按的event，再去判断具体的可处理事件类型
	_, ok := event.(IForwardEvent)
	if !ok {
		return false
	} else {
		_, ok := event.(AcceptEvent)
		return ok
	}
}

func (self *AcceptHandler) innerHandle(ctx *DefaultPipelineContext, event IForwardEvent) (*store.MessageEntity, error) {
	acceptEvent, ok := event.(AcceptEvent)
	if !ok {
		return nil, ERROR_INVALID_EVENT_TYPE
	}
	//这里处理一下acceptEvent,做一下校验
	var msg *store.MessageEntity
	switch acceptEvent.msgType {
	case protocol.CMD_TYPE_BYTES_MESSAGE:
		msg = store.NewBytesMessageEntity(acceptEvent.msg.(*protocol.BytesMessage))
	case protocol.CMD_TYPE_STRING_MESSAGE:
		msg = store.NewStringMessageEntity(acceptEvent.msg.(*protocol.StringMessage))
	}
	return msg, nil
}

func (self *AcceptHandler) HandleEvent(ctx *DefaultPipelineContext, event IEvent) error {
	result, err := self.innerHandle(ctx, event)
	if nil == err {
		//创建一个持久化的事件
		persistentEvent := &PersistentEvent{}
		persistentEvent.entity = result

		//向后发送
		ctx.SendForward(persistentEvent)
	}
	return err
}

func (self *AcceptHandler) HandleForward(ctx *DefaultPipelineContext, event IForwardEvent) error {
	//处理逻辑成功则向后传递
	if !self.AcceptEvent(event) {
		ctx.SendForward(event)
		return nil
	} else {
		return self.HandleEvent(ctx, event)
	}
}
