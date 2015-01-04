package handler

import (
	"errors"
	"go-kite/store"
	"log"
)

var ERROR_PERSISTENT = errors.New("persistent msg error!")

//----------------持久化的handler
type PersistentHandler struct {
	IForwardHandler
	name      string
	kitestore store.IKiteStore
}

//------创建persitehandler
func NewPersistentHandler(name string, kitestore store.IKiteStore) *PersistentHandler {
	return &PersistentHandler{name: name,
		kitestore: kitestore}
}

func (self *PersistentHandler) GetName() string {
	return self.name
}

func (self *PersistentHandler) AcceptEvent(event IEvent) bool {
	//是否可以处理当前按的event，再去判断具体的可处理事件类型
	_, ok := event.(IForwardEvent)
	if !ok {
		return false
	} else {
		_, ok := self.typeAssert(event)
		return ok
	}
}

func (self *PersistentHandler) typeAssert(event IEvent) (*PersistentEvent, bool) {
	val, ok := event.(*PersistentEvent)
	return val, ok
}

func (self *PersistentHandler) innerHandle(ctx *DefaultPipelineContext, event IForwardEvent) (bool, *DeliverEvent) {
	pevent, ok := self.typeAssert(event)
	if !ok {
		return false, nil
	}

	//写入到持久化存储里面
	succ := self.kitestore.Save(pevent.entity)
	if succ {
		deliver := &DeliverEvent{}
		deliver.MessageId = pevent.entity.Header.GetMessageId()
		deliver.Topic = pevent.entity.Header.GetTopic()
		deliver.MessageType = pevent.entity.Header.GetMessageType()
		deliver.ExpiredTime = pevent.entity.Header.GetExpiredTime()
		return true, deliver
	}
	return false, nil
}

func (self *PersistentHandler) HandleEvent(ctx *DefaultPipelineContext, event IEvent) error {
	succ, devent := self.innerHandle(ctx, event)
	log.Printf("PersistentHandler|HandleEvent|%t|%t|%s\n", event, succ, devent)
	if succ {
		log.Printf("PersistentHandler|SendForward|%s\n", devent)
		//成功向后发送
		ctx.SendForward(devent)

	} else {
		return ERROR_PERSISTENT
	}
	return nil
}

func (self *PersistentHandler) HandleForward(ctx *DefaultPipelineContext, event IForwardEvent) error {
	//处理逻辑成功则向后传递
	if !self.AcceptEvent(event) {
		ctx.SendForward(event)
		return nil
	} else {
		return self.HandleEvent(ctx, event)
	}
}
