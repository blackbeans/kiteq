package handler

import (
	. "kiteq/pipe"
	"kiteq/store"
	"log"
	"sort"
	"time"
)

type redeliveryWindows []RedeliveryWindow

//redelivery的窗口，根据投递次数决定延迟投递的时间
type RedeliveryWindow struct {
	minDeliveryCount int32
	maxDeliveryCount int32
	delaySeconds     int32 //延迟的秒数
}

func NewRedeliveryWindow(minDeliveryCount, maxDeliveryCount, delaySeconds int32) RedeliveryWindow {
	return RedeliveryWindow{
		minDeliveryCount: minDeliveryCount,
		maxDeliveryCount: maxDeliveryCount,
		delaySeconds:     delaySeconds}
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

//-------投递结果记录的handler
type ResultRecordHandler struct {
	BaseForwardHandler
	kitestore store.IKiteStore
	rw        redeliveryWindows //多个恢复的windows
}

//------创建投递结果处理器
func NewResultRecordHandler(name string, kitestore store.IKiteStore, rw []RedeliveryWindow) *ResultRecordHandler {
	dhandler := &ResultRecordHandler{}
	dhandler.BaseForwardHandler = NewBaseForwardHandler(name, dhandler)
	dhandler.kitestore = kitestore

	dhandler.rw = redeliveryWindows(rw)
	//排好序
	sort.Sort(dhandler.rw)
	log.Printf("ResultRecordHandler|SORT RedeliveryWindows|%s\n ", dhandler.rw)
	return dhandler
}

func (self *ResultRecordHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *ResultRecordHandler) cast(event IEvent) (val *deliverResultEvent, ok bool) {
	val, ok = event.(*deliverResultEvent)
	return
}

func (self *ResultRecordHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	fevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	//增加投递成功的分组
	if len(fevent.deliverySuccGroups) > 0 {
		fevent.succGroups = append(fevent.succGroups, fevent.deliverySuccGroups...)
	}

	//存储投递结果
	self.saveDevlierResult(fevent.messageId, fevent.deliverCount, fevent.succGroups, fevent.deliveryFailGroups)

	//检查当前消息的ttl和有效期是否达到最大的，如果达到最大则不允许再次投递
	if fevent.expiredTime >= time.Now().Unix() || fevent.deliverLimit >= fevent.deliverCount {
		//只是记录一下本次发送记录不发起重投策略

	} else if fevent.deliverCount <= 3 {
		//只有在消息前三次投递才会失败立即重投
		fevent.deliverGroups = fevent.deliveryFailGroups
		fevent.packet.Reset()
		//再次发起重投策略
		ctx.SendBackward(fevent.deliverEvent)
	} else {
		//只能等待后续的recover重投了
	}
	return nil

}

//存储投递结果
func (self *ResultRecordHandler) saveDevlierResult(messageId string, deliverCount int32,
	succGroups []string, failGroups []string) {

	entity := &store.MessageEntity{
		MessageId:    messageId,
		DeliverCount: deliverCount,
		SuccGroups:   succGroups,
		FailGroups:   failGroups,
		//设置一下下一次投递时间
		NextDeliverTime: self.nextDeliveryTime(deliverCount)}
	//更新当前消息的数据
	self.kitestore.UpdateEntity(entity)
}

func (self *ResultRecordHandler) nextDeliveryTime(deliverCount int32) int64 {
	delayTime := self.rw[0].delaySeconds
	for _, w := range self.rw {
		if deliverCount >= w.minDeliveryCount &&
			w.maxDeliveryCount > deliverCount ||
			(w.maxDeliveryCount < 0 && deliverCount >= w.minDeliveryCount) {
			delayTime = w.delaySeconds
		}
	}

	// log.Printf("ResultRecordHandler|nextDeliveryTime|%d|%d\n", deliverCount, delayTime)
	//总是返回一个区间的不然是个bug

	//设置一下下次投递时间为当前时间+延时时间
	return time.Now().Add(time.Duration(delayTime) * time.Second).Unix()
}
