package handler

import (
	"errors"
	"kiteq/protocol"
	"kiteq/store"
	// "log"
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

	// log.Printf("PersistentHandler|Process|%t\n", event)

	pevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	//响应包
	responsePacket := &protocol.ResponsePacket{
		Opaque:     pevent.opaque,
		RemoteAddr: pevent.session.RemotingAddr(),
		CmdType:    protocol.CMD_TYPE_MESSAGE_STORE}

	//向当前连接写入一个存储成功的response
	remoteEvent := newRemotingEvent(responsePacket, pevent.session)

	//写入到持久化存储里面
	succ := self.kitestore.Save(pevent.entity)
	if succ {
		//启动异步协程处理分发逻辑
		go func() {
			deliver := &DeliverEvent{}
			deliver.MessageId = pevent.entity.Header.GetMessageId()
			deliver.Topic = pevent.entity.Header.GetTopic()
			deliver.MessageType = pevent.entity.Header.GetMessageType()
			deliver.ExpiredTime = pevent.entity.Header.GetExpiredTime()
			ctx.SendForward(event)

		}()
		responsePacket.Status = protocol.RESP_STATUS_SUCC
	} else {
		responsePacket.Status = protocol.RESP_STATUS_FAIL
	}

	//向后走网络传输
	ctx.SendForward(remoteEvent)
	return nil

}
