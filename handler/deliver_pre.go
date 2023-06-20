package handler

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/registry"
	"github.com/blackbeans/kiteq-common/stat"
	"github.com/blackbeans/turbo"
	"kiteq/exchange"
	"kiteq/store"
	"time"
)

//----------------持久化的handler
type DeliverPreHandler struct {
	turbo.BaseForwardHandler
	kitestore        store.IKiteStore
	exchanger        *exchange.BindExchanger
	maxDeliverNum    chan interface{}
	deliverTimeout   time.Duration
	flowstat         *stat.FlowStat
	deliveryRegistry *DeliveryRegistry
}

//------创建deliverpre
func NewDeliverPreHandler(name string, kitestore store.IKiteStore,
	exchanger *exchange.BindExchanger, flowstat *stat.FlowStat,
	maxDeliverWorker int, deliveryRegistry *DeliveryRegistry) *DeliverPreHandler {
	phandler := &DeliverPreHandler{}
	phandler.BaseForwardHandler = turbo.NewBaseForwardHandler(name, phandler)
	phandler.kitestore = kitestore
	phandler.exchanger = exchanger
	phandler.maxDeliverNum = make(chan interface{}, maxDeliverWorker)
	phandler.flowstat = flowstat
	phandler.deliveryRegistry = deliveryRegistry

	return phandler
}

func (self *DeliverPreHandler) TypeAssert(event turbo.IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *DeliverPreHandler) cast(event turbo.IEvent) (val *deliverPreEvent, ok bool) {
	val, ok = event.(*deliverPreEvent)
	return
}

func (self *DeliverPreHandler) Process(ctx *turbo.DefaultPipelineContext, event turbo.IEvent) error {

	pevent, ok := self.cast(event)
	if !ok {
		return turbo.ERROR_INVALID_EVENT_TYPE
	}

	//尝试注册一下当前的投递事件的消息
	//如果失败则放弃本次投递
	//会在 deliverResult里取消该注册事件可以继续投递
	succ := self.deliveryRegistry.Register(pevent.messageId, ExpiredSecond)
	if !succ {
		return nil
	}

	self.maxDeliverNum <- nil
	self.flowstat.DeliverGo.Incr(1)
	go func() {
		defer func() {
			<-self.maxDeliverNum
			self.flowstat.DeliverGo.Incr(-1)
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
func (self *DeliverPreHandler) send0(ctx *turbo.DefaultPipelineContext, pevent *deliverPreEvent) {

	//如果没有entity则直接查询一下db
	entity := pevent.entity
	if nil == entity {
		//查询消息
		entity = self.kitestore.Query(pevent.header.GetTopic(), pevent.messageId)
		if nil == entity {
			self.kitestore.Expired(pevent.header.GetTopic(), pevent.messageId)
			//log.Error("DeliverPreHandler|send0|Query|FAIL|%s", pevent.messageId)
			return
		}
	}

	//check entity need to deliver
	if !self.checkValid(entity) {
		self.kitestore.Expired(pevent.header.GetTopic(), entity.MessageId)
		return
	}

	// log.Debug("DeliverPreHandler|send0|Query|%s", entity.Header)
	data := protocol.MarshalMessage(entity.Header, entity.MsgType, entity.GetBody())

	//构造deliverEvent
	deliverEvent := newDeliverEvent(pevent.header, pevent.attemptDeliver)

	//创建不同的packet
	switch entity.MsgType {
	case protocol.CMD_BYTES_MESSAGE:
		deliverEvent.packet = turbo.NewPacket(protocol.CMD_BYTES_MESSAGE, data)
	case protocol.CMD_STRING_MESSAGE:
		deliverEvent.packet = turbo.NewPacket(protocol.CMD_STRING_MESSAGE, data)
	}

	//填充订阅分组
	self.fillGroupIds(deliverEvent, entity)
	self.fillDeliverExt(deliverEvent, entity)

	//向后投递发送
	ctx.SendForward(deliverEvent)
}

//填充订阅分组
func (self *DeliverPreHandler) fillGroupIds(pevent *deliverEvent, entity *store.MessageEntity) {
	binds, limiters := self.exchanger.FindBinds(entity.Header.GetTopic(), entity.Header.GetMessageType(),
		func(b *registry.Binding) bool {
			// log.Printf("DeliverPreHandler|fillGroupIds|Filter Bind |%s|", b)
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
	groupBinds := make(map[string]registry.Binding, 10)
	//按groupid归并
	for _, bind := range binds {
		//获取group对应的limiter
		groupIds = append(groupIds, bind.GroupId)

		_, ok := groupBinds[bind.GroupId]
		if !ok {
			groupBinds[bind.GroupId] = *bind
		}
	}
	pevent.groupBinds = groupBinds
	pevent.limiters = limiters
	pevent.deliverGroups = groupIds
}

//填充投递的额外信息
func (self *DeliverPreHandler) fillDeliverExt(pevent *deliverEvent, entity *store.MessageEntity) {
	pevent.header = entity.Header
	pevent.deliverLimit = entity.DeliverLimit
	pevent.deliverCount = entity.DeliverCount
	pevent.succGroups = entity.SuccGroups
}
