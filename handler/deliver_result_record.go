package handler

import (
	. "kiteq/pipe"
	"kiteq/store"
	// "log"
	"time"
)

//-------投递结果记录的handler
type ResultRecordHandler struct {
	BaseBackwardHandler
	kitestore store.IKiteStore
}

//------创建投递结果处理器
func NewResultRecordHandler(name string, kitestore store.IKiteStore) *ResultRecordHandler {
	dhandler := &ResultRecordHandler{}
	dhandler.BaseBackwardHandler = NewBaseBackwardHandler(name, dhandler)
	dhandler.kitestore = kitestore
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
	if len(fevent.succGroups) > 0 {
		fevent.deliverGroups = append(fevent.deliverGroups, fevent.succGroups...)
	}

	//存储投递结果
	self.saveDevlierResult(fevent.messageId, fevent.deliverCount, fevent.deliverGroups, fevent.failGroups)

	//检查当前消息的ttl和有效期是否达到最大的，如果达到最大则不允许再次投递
	if fevent.expiredTime >= time.Now().Unix() || fevent.deliverLimit >= fevent.deliverCount {
		//只是记录一下本次发送记录不发起重投策略

	} else if fevent.deliverCount <= 3 {
		//只有在消息前三次投递才会失败立即重投
		fevent.deliverGroups = fevent.failGroups
		//再次发起重投策略
		ctx.SendBackward(fevent.deliverEvent)
	} else {
		//只能等后续的recover线程去处理
	}
	return nil

}

//存储投递结果
func (self *ResultRecordHandler) saveDevlierResult(messageId string, deliverCount int32,
	deliverGroupIds []string, failGroups []string) {

}
