package handler

import (
	"kiteq/binding"
	. "kiteq/pipe"
	"kiteq/protocol"
	"kiteq/store"
	// "log"
)

//----------------持久化的handler
type DeliverPreHandler struct {
	BaseForwardHandler
	kitestore store.IKiteStore
	exchanger *binding.BindExchanger
}

//------创建deliverpre
func NewDeliverPreHandler(name string, kitestore store.IKiteStore,
	exchanger *binding.BindExchanger) *DeliverPreHandler {
	phandler := &DeliverPreHandler{}
	phandler.BaseForwardHandler = NewBaseForwardHandler(name, phandler)
	phandler.kitestore = kitestore
	phandler.exchanger = exchanger
	return phandler
}

func (self *DeliverPreHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *DeliverPreHandler) cast(event IEvent) (val *deliverEvent, ok bool) {
	val, ok = event.(*deliverEvent)
	return
}

func (self *DeliverPreHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	pevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	//查询消息
	entity := self.kitestore.Query(pevent.messageId)
	data := protocol.MarshalMessage(entity.Header, entity.MsgType, entity.GetBody())

	//创建不同的packet
	switch entity.MsgType {
	case protocol.CMD_BYTES_MESSAGE:
		pevent.packet = protocol.NewPacket(protocol.CMD_BYTES_MESSAGE, data)
	case protocol.CMD_STRING_MESSAGE:
		pevent.packet = protocol.NewPacket(protocol.CMD_STRING_MESSAGE, data)
	}

	//填充订阅分组
	self.fillGroupIds(pevent)
	ctx.SendForward(pevent)
	return nil

}

//填充订阅分组
func (self *DeliverPreHandler) fillGroupIds(pevent *deliverEvent) {
	binds := self.exchanger.FindBinds(pevent.topic, pevent.messageType, func(b *binding.Binding) bool {
		//过滤掉已经投递成功的分组
		// log.Printf("DeliverPreHandler|fillGroupIds|Filter Bind |%s|\n", b)
		return false
	})

	groupIds := make([]string, 0, 10)
	//按groupid归并
outter:
	for _, bind := range binds {
		for _, g := range groupIds {
			if g == bind.GroupId {
				continue outter
			}
		}
		groupIds = append(groupIds, bind.GroupId)
	}

	// //如果没有可用的分组则直接跳过
	// if len(groupIds) <= 0 {
	// 	log.Printf("DeliverPreHandler|Process|NO GROUPID TO DELIVERY |%s|%s|%s|%s\n", pevent.messageId, pevent.topic, pevent.messageType, binds)
	// } else {
	// 	log.Printf("DeliverPreHandler|Process|GROUPIDS TO DELIVERY |%s|%s|%s,%s\n", pevent.messageId, pevent.topic, pevent.messageType, groupIds)
	// }

	pevent.deliverGroups = groupIds
}
