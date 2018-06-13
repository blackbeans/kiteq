package handler

import (
	"fmt"
	"github.com/blackbeans/kiteq-common/stat"
	"github.com/blackbeans/kiteq-common/store"
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"
	"sort"
	"time"
)

type redeliveryWindows []RedeliveryWindow

//redelivery的窗口，根据投递次数决定延迟投递的时间
type RedeliveryWindow struct {
	minDeliveryCount int32
	maxDeliveryCount int32
	delaySeconds     int64 //延迟的秒数
}

func NewRedeliveryWindow(minDeliveryCount, maxDeliveryCount int32, delaySeconds int32) RedeliveryWindow {
	return RedeliveryWindow{
		minDeliveryCount: minDeliveryCount,
		maxDeliveryCount: maxDeliveryCount,
		delaySeconds:     int64(delaySeconds)}
}

func (self redeliveryWindows) Len() int { return len(self) }
func (self redeliveryWindows) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}
func (self redeliveryWindows) Less(i, j int) bool {
	return (self[i].maxDeliveryCount <= self[j].minDeliveryCount &&
		self[i].maxDeliveryCount < self[j].maxDeliveryCount) &&
		self[i].maxDeliveryCount >= 0
}

func (self redeliveryWindows) String() string {
	str := ""
	for _, v := range self {
		str += fmt.Sprintf("[min:%d,max:%d,sec:%d]->", v.minDeliveryCount, v.maxDeliveryCount, v.delaySeconds)
	}
	return str
}

//-------投递结果记录的handler
type DeliverResultHandler struct {
	turbo.BaseForwardHandler
	kitestore        store.IKiteStore
	rw               redeliveryWindows //多个恢复的windows
	deliverTimeout   time.Duration
	updateChan       chan store.MessageEntity
	deleteChan       chan string
	deliveryRegistry *stat.DeliveryRegistry
}

//------创建投递结果处理器
func NewDeliverResultHandler(name string, deliverTimeout time.Duration, kitestore store.IKiteStore,
	rw []RedeliveryWindow, deliveryRegistry *stat.DeliveryRegistry) *DeliverResultHandler {
	dhandler := &DeliverResultHandler{}
	dhandler.BaseForwardHandler = turbo.NewBaseForwardHandler(name, dhandler)
	dhandler.kitestore = kitestore
	dhandler.deliverTimeout = deliverTimeout
	dhandler.rw = redeliveryWindows(rw)
	dhandler.deliveryRegistry = deliveryRegistry
	//排好序
	sort.Sort(dhandler.rw)
	log.InfoLog("kite_handler", "RedeliveryWindows|%s\n ", dhandler.rw)
	return dhandler
}

func (self *DeliverResultHandler) TypeAssert(event turbo.IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *DeliverResultHandler) cast(event turbo.IEvent) (val *deliverResultEvent, ok bool) {
	val, ok = event.(*deliverResultEvent)
	return
}

func (self *DeliverResultHandler) Process(ctx *turbo.DefaultPipelineContext, event turbo.IEvent) error {

	fevent, ok := self.cast(event)
	if !ok {
		return turbo.ERROR_INVALID_EVENT_TYPE
	}

	if len(fevent.futures) > 0 {
		//等待回调结果
		fevent.wait(self.deliverTimeout, fevent.deliverEvent.groupBinds)
	}

	//增加投递成功的分组
	if len(fevent.deliverSuccGroups) > 0 {
		//剔除掉投递成功的分组
		for _, g := range fevent.deliverSuccGroups {
			delete(fevent.limiters, g)
		}
		fevent.succGroups = append(fevent.succGroups, fevent.deliverSuccGroups...)

	}

	attemptDeliver := nil != fevent.attemptDeliver
	//第一次尝试投递失败了立即通知
	if attemptDeliver {
		fevent.attemptDeliver <- fevent.deliverFailGroups
		close(fevent.attemptDeliver)
	}

	log.InfoLog("kite_handler", "%s|Process|SEND RESULT:\n"+
		"MessageId:%s\nTopic:%s\nMessageType:%s\nPublishGroupId:%s\nDeliverCount:%d\n"+
		"CreateTime:%d\nproperties:%v\n"+
		"AttemptDeliver:%v\nFly:%v\n"+
		"NextDeliverTime:%d\n"+
		"DeliverGroups:%v\nSUCCGROUPS:%v\nDeliverSUCCGROUPS:%v\nDeliverFAILGROUPS:%v",
		self.GetName(),
		fevent.header.GetMessageId(), fevent.header.GetTopic(),
		fevent.header.GetMessageType(), fevent.header.GetGroupId(),
		fevent.deliverCount,
		fevent.header.GetCreateTime(),
		fevent.header.GetProperties(),
		attemptDeliver, fevent.header.GetFly(),
		self.nextDeliveryTime(fevent.deliverCount),
		fevent.deliverGroups,
		fevent.succGroups, fevent.succGroupFuture, fevent.failGroupFuture)
	//都投递成功
	if len(fevent.deliverFailGroups) <= 0 {
		if !fevent.header.GetFly() && !attemptDeliver {
			//async batch remove
			self.kitestore.AsyncDelete(fevent.header.GetTopic(), fevent.header.GetMessageId())
		}
	} else {
		//重投策略
		//不是尝试投递也就是第一次投递并且也是满足重投条件
		if !attemptDeliver && self.checkRedelivery(fevent) {
			//去掉当前消息的投递事件
			self.deliveryRegistry.UnRegiste(fevent.header.GetMessageId())
			//再次发起重投策略
			ctx.SendBackward(fevent.deliverEvent)
		}
	}

	return nil

}

func (self *DeliverResultHandler) checkRedelivery(fevent *deliverResultEvent) bool {

	//检查当前消息的ttl和有效期是否达到最大的，如果达到最大则不允许再次投递
	if fevent.header.GetExpiredTime() <= time.Now().Unix() || (fevent.deliverLimit <= fevent.deliverCount &&
		fevent.deliverLimit > 0) {
		//只是记录一下本次发送记录不发起重投策略

	} else if fevent.deliverCount < 3 {
		//只有在消息前三次投递才会失败立即重投
		fevent.deliverGroups = fevent.deliverFailGroups
		fevent.packet.Reset()
		return true
	} else {
		//如果投递次数大于3次并且失败了，那么需要持久化一下然后只能等待后续的recover重投了
		//log deliver fail
		// log.DebugLog("kite_handler", "DeliverResultHandler|checkRedelivery|messageId:%s|Topic:%s|MessageType:%s|DeliverCount:%d|SUCCGROUPS:%s|FAILGROUPS:%s|",
		// 	fevent.deliverEvent.messageId, fevent.deliverEvent.topic, fevent.deliverEvent.messageType,
		// 	fevent.deliverCount, fevent.deliverEvent.succGroups, fevent.deliverFailGroups)
	}

	//如果不为fly消息那么需要存储投递结果
	//并且消息的投递次数大于等于3那么就应该持久化投递结果
	if !fevent.header.GetFly() && fevent.deliverCount >= 3 {
		//存储投递结果
		self.saveDeliverResult(fevent.header.GetMessageId(), fevent.deliverCount,
			fevent.succGroups, fevent.deliverFailGroups)
	}

	return false
}

//存储投递结果
func (self *DeliverResultHandler) saveDeliverResult(messageId string, deliverCount int32, succGroups []string, failGroups []string) {

	entity := &store.MessageEntity{
		MessageId:    messageId,
		DeliverCount: deliverCount,
		SuccGroups:   succGroups,
		FailGroups:   failGroups,
		//设置一下下一次投递时间
		NextDeliverTime: self.nextDeliveryTime(deliverCount)}
	//异步更新当前消息的数据
	self.kitestore.AsyncUpdate(entity)
}

func (self *DeliverResultHandler) nextDeliveryTime(deliverCount int32) int64 {
	delayTime := self.rw[0].delaySeconds
	for _, w := range self.rw {
		if deliverCount >= w.minDeliveryCount &&
			w.maxDeliveryCount > deliverCount ||
			(w.maxDeliveryCount < 0 && deliverCount >= w.minDeliveryCount) {
			delayTime = w.delaySeconds
		}
	}

	// log.InfoLog("kite_handler", "DeliverResultHandler|nextDeliveryTime|%d|%d\n", deliverCount, delayTime)
	//总是返回一个区间的不然是个bug

	//设置一下下次投递时间为当前时间+延时时间
	return time.Now().Unix() + delayTime
}
