package handler

import (
	"errors"
	"go-kite/store"
	"log"
)

var ERROR_PERSISTENT = errors.New("persistent msg error!")

//----------------持久化的handler
type PersistentHandler struct {
	BaseForwardHandler
	IEventProcessor
	kitestore store.IKiteStore
}

//------创建persitehandler
func NewPersistentHandler(name string, kitestore store.IKiteStore) *PersistentHandler {
	phandler := &PersistentHandler{}
	phandler.name = name
	phandler.kitestore = kitestore
	phandler.processor = phandler
	return phandler
}

func (self *PersistentHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *PersistentHandler) cast(event IEvent) (val *PersistentEvent, ok bool) {
	val, ok = event.(*PersistentEvent)
	return
}

func (self *PersistentHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	log.Printf("PersistentHandler|Process|%t\n", event)

	pevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	//写入到持久化存储里面
	succ := self.kitestore.Save(pevent.entity)
	if succ {
		deliver := &DeliverEvent{}
		deliver.MessageId = pevent.entity.Header.GetMessageId()
		deliver.Topic = pevent.entity.Header.GetTopic()
		deliver.MessageType = pevent.entity.Header.GetMessageType()
		deliver.ExpiredTime = pevent.entity.Header.GetExpiredTime()
		ctx.SendForward(event)
	} else {
		//发送一个保存失败的时间

	}
	return nil
}
