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

func (self *DeliverPreHandler) cast(event IEvent) (val *deliverPreEvent, ok bool) {
	val, ok = event.(*deliverPreEvent)
	return
}

func (self *DeliverPreHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	pevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	//如果没有entity则直接查询一下db
	entity := pevent.entity
	if nil == entity {
		//查询消息
		entity = self.kitestore.Query(pevent.messageId)
		if nil == entity {
			return nil
		}
	}

	data := protocol.MarshalMessage(entity.Header, entity.MsgType, entity.GetBody())

	//构造deliverEvent
	deliverEvent := newDeliverEvent(pevent.messageId, pevent.header.GetTopic(), pevent.header.GetMessageType())

	//创建不同的packet
	switch entity.MsgType {
	case protocol.CMD_BYTES_MESSAGE:
		deliverEvent.packet = protocol.NewPacket(protocol.CMD_BYTES_MESSAGE, data)
	case protocol.CMD_STRING_MESSAGE:
		deliverEvent.packet = protocol.NewPacket(protocol.CMD_STRING_MESSAGE, data)
	}

	//填充订阅分组
	self.fillGroupIds(deliverEvent, entity)
	self.fillDeliverExt(deliverEvent, entity)
	//向后投递发送
	ctx.SendForward(deliverEvent)
	return nil

}

//填充订阅分组
func (self *DeliverPreHandler) fillGroupIds(pevent *deliverEvent, entity *store.MessageEntity) {
	binds := self.exchanger.FindBinds(entity.Header.GetTopic(), entity.Header.GetMessageType(), func(b *binding.Binding) bool {
		// log.Printf("DeliverPreHandler|fillGroupIds|Filter Bind |%s|\n", b)
		//过滤掉已经投递成功的分组
		for _, sg := range entity.SuccGroups {
			if sg == b.GroupId {
				return true
			}
		}
		return false
	})

	//合并本次需要投递的分组
	groupIds := make([]string, 0, 10)
	//按groupid归并
	for _, bind := range binds {
		groupIds = append(groupIds, bind.GroupId)
		// hashGroups[bind.GroupId] = nil
	}

outter:
	//加入投递失败的分组
	for _, fg := range entity.FailGroups {

		for _, g := range groupIds {
			//如果已经存在则不添加进去
			if g == fg {
				continue outter
			}
		}
		groupIds = append(groupIds, fg)
	}

	// //如果没有可用的分组则直接跳过
	// if len(groupIds) <= 0 {
	// 	log.Printf("DeliverPreHandler|Process|NO GROUPID TO DELIVERY |%s|%s|%s|%s\n", pevent.messageId, pevent.topic, pevent.messageType, binds)
	// } else {
	// 	log.Printf("DeliverPreHandler|Process|GROUPIDS TO DELIVERY |%s|%s|%s,%s\n", pevent.messageId, pevent.topic, pevent.messageType, groupIds)
	// }
	pevent.deliverGroups = groupIds
}

//填充投递的额外信息
func (self *DeliverPreHandler) fillDeliverExt(pevent *deliverEvent, entity *store.MessageEntity) {
	pevent.messageId = entity.Header.GetMessageId()
	pevent.topic = entity.Header.GetTopic()
	pevent.messageType = entity.Header.GetMessageType()
	pevent.expiredTime = entity.Header.GetExpiredTime()
	pevent.succGroups = entity.SuccGroups
	pevent.deliverLimit = entity.DeliverLimit
	pevent.deliverCount = entity.DeliverCount
}
