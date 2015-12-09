package handler

import (
	// log "github.com/blackbeans/log4go"
	packet "github.com/blackbeans/turbo/packet"
	. "github.com/blackbeans/turbo/pipe"
	"kiteq/binding"
	"kiteq/protocol"
	"kiteq/stat"
	"kiteq/store"
	"time"
)

//----------------持久化的handler
type DeliverPreHandler struct {
	BaseForwardHandler
	kitestore      store.IKiteStore
	exchanger      *binding.BindExchanger
	maxDeliverNum  chan byte
	deliverTimeout time.Duration
	flowstat       *stat.FlowStat
}

//------创建deliverpre
func NewDeliverPreHandler(name string, kitestore store.IKiteStore,
	exchanger *binding.BindExchanger, flowstat *stat.FlowStat,
	maxDeliverWorker int) *DeliverPreHandler {
	phandler := &DeliverPreHandler{}
	phandler.BaseForwardHandler = NewBaseForwardHandler(name, phandler)
	phandler.kitestore = kitestore
	phandler.exchanger = exchanger
	phandler.maxDeliverNum = make(chan byte, maxDeliverWorker)
	phandler.flowstat = flowstat
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

	self.maxDeliverNum <- 1
	self.flowstat.DeliverPool.Incr(1)
	go func() {
		defer func() {
			<-self.maxDeliverNum
			self.flowstat.DeliverPool.Incr(-1)
		}()
		//启动投递
		self.send0(ctx, pevent)
		self.flowstat.DeliverFlow.Incr(1)
	}()

	return nil
}

//check entity need to deliver
func (self *DeliverPreHandler) checkValid(entity *store.MessageEntity) bool {
	//判断个当前的header和投递次数消息有效时间是否过期
	return entity.DeliverCount < entity.Header.GetDeliverLimit() &&
		entity.ExpiredTime > time.Now().Unix()
}

//内部处理
func (self *DeliverPreHandler) send0(ctx *DefaultPipelineContext, pevent *deliverPreEvent) {
	//如果没有entity则直接查询一下db
	entity := pevent.entity
	if nil == entity {
		//查询消息
		entity = self.kitestore.Query(pevent.messageId)
		if nil == entity {
			self.kitestore.Expired(pevent.messageId)
			// log.Error("DeliverPreHandler|send0|Query|FAIL|%s\n", pevent.messageId)
			return
		}
	}

	//check entity need to deliver
	if !self.checkValid(entity) {
		self.kitestore.Expired(entity.MessageId)
		return
	}
	// log.Debug("DeliverPreHandler|send0|Query|%s", entity.Header)
	data := protocol.MarshalMessage(entity.Header, entity.MsgType, entity.GetBody())

	//构造deliverEvent
	deliverEvent := newDeliverEvent(pevent.messageId, pevent.header.GetTopic(),
		pevent.header.GetMessageType(), entity.PublishTime, pevent.attemptDeliver)

	//创建不同的packet
	switch entity.MsgType {
	case protocol.CMD_BYTES_MESSAGE:
		deliverEvent.packet = packet.NewPacket(protocol.CMD_BYTES_MESSAGE, data)
	case protocol.CMD_STRING_MESSAGE:
		deliverEvent.packet = packet.NewPacket(protocol.CMD_STRING_MESSAGE, data)
	}

	//填充订阅分组
	self.fillGroupIds(deliverEvent, entity)
	self.fillDeliverExt(deliverEvent, entity)

	//向后投递发送
	ctx.SendForward(deliverEvent)
}

//填充订阅分组
func (self *DeliverPreHandler) fillGroupIds(pevent *deliverEvent, entity *store.MessageEntity) {
	binds := self.exchanger.FindBinds(entity.Header.GetTopic(), entity.Header.GetMessageType(),
		func(b *binding.Binding) bool {
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

	pevent.deliverGroups = groupIds
}

//填充投递的额外信息
func (self *DeliverPreHandler) fillDeliverExt(pevent *deliverEvent, entity *store.MessageEntity) {
	pevent.messageId = entity.Header.GetMessageId()
	pevent.topic = entity.Header.GetTopic()
	pevent.messageType = entity.Header.GetMessageType()
	pevent.expiredTime = entity.Header.GetExpiredTime()
	pevent.fly = entity.Header.GetFly()
	pevent.succGroups = entity.SuccGroups
	pevent.deliverLimit = entity.DeliverLimit
	pevent.deliverCount = entity.DeliverCount
}
