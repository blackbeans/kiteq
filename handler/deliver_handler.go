package handler

import (
	"github.com/golang/protobuf/proto"
	. "kiteq/pipe"
	"kiteq/protocol"
	"kiteq/store"
	"log"
)

//----------------投递的handler
type DeliverHandler struct {
	BaseForwardHandler
	kitestore store.IKiteStore
}

//------创建deliverpre
func NewDeliverHandler(name string, kitestore store.IKiteStore) *DeliverHandler {

	phandler := &DeliverHandler{}
	phandler.BaseForwardHandler = NewBaseForwardHandler(name, phandler)
	phandler.kitestore = kitestore
	return phandler
}

func (self *DeliverHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *DeliverHandler) cast(event IEvent) (val *DeliverEvent, ok bool) {
	val, ok = event.(*DeliverEvent)
	return
}

func (self *DeliverHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	// log.Printf("DeliverHandler|Process|%s|%t\n", self.GetName(), event)

	pevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	//获取投递的消息数据

	//发起一个请求包

	entity := self.kitestore.Query(pevent.MessageId)
	// @todo 判断类型创建string或者byte message
	message := &protocol.StringMessage{}
	message.Header = entity.Header
	message.Body = proto.String(string(entity.GetBody()))
	data, err := proto.Marshal(message)
	if nil != err {
		log.Printf("DeliverHandler|Process|Query Message|FAIL|%s|%s\n", err, pevent.MessageId)
		return err
	}

	wpacket := protocol.NewPacket(protocol.CMD_STRING_MESSAGE, data)
	//创建投递事件
	revent := NewRemotingEvent(wpacket, nil, pevent.DeliverGroups...)

	//向后转发
	ctx.SendForward(revent)

	return nil

}
