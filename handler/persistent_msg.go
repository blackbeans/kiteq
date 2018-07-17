package handler

import (
	"errors"
	"fmt"
	"github.com/blackbeans/kiteq-common/store"
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"
	"time"
)

var ERROR_PERSISTENT = errors.New("persistent msg error!")

//----------------持久化的handler
type PersistentHandler struct {
	turbo.BaseForwardHandler
	kitestore      store.IKiteStore
	deliverTimeout time.Duration
	deliveryFirst  bool //是否优先投递
}

//------创建persitehandler
func NewPersistentHandler(name string, deliverTimeout time.Duration,
	kitestore store.IKiteStore, deliveryFirst bool) *PersistentHandler {
	phandler := &PersistentHandler{}
	phandler.BaseForwardHandler = turbo.NewBaseForwardHandler(name, phandler)
	phandler.kitestore = kitestore
	phandler.deliverTimeout = deliverTimeout
	phandler.deliveryFirst = deliveryFirst
	return phandler
}

func (self *PersistentHandler) TypeAssert(event turbo.IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *PersistentHandler) cast(event turbo.IEvent) (val *persistentEvent, ok bool) {
	val, ok = event.(*persistentEvent)
	return
}

func (self *PersistentHandler) Process(ctx *turbo.DefaultPipelineContext, event turbo.IEvent) error {

	pevent, ok := self.cast(event)
	if !ok {
		return turbo.ERROR_INVALID_EVENT_TYPE
	}

	if nil != pevent.entity {
		//如果是fly模式不做持久化
		if pevent.entity.Header.GetFly() {
			if pevent.entity.Header.GetCommit() {
				//如果是成功存储的、并且为未提交的消息，则需要发起一个ack的命令
				//发送存储结果ack
				remoteEvent := turbo.NewRemotingEvent(storeAck(pevent.opaque,
					pevent.entity.Header.GetMessageId(), true, "FLY NO NEED SAVE"), []string{pevent.remoteClient.RemoteAddr()})
				ctx.SendForward(remoteEvent)

				self.send(ctx, pevent, nil)
			} else {
				remoteEvent := turbo.NewRemotingEvent(storeAck(pevent.opaque,
					pevent.entity.Header.GetMessageId(), false, "FLY MUST BE COMMITTED !"), []string{pevent.remoteClient.RemoteAddr()})
				ctx.SendForward(remoteEvent)
			}

		} else {
			self.sendUnFlyMessage(ctx, pevent)
		}

	}

	return nil
}

//发送非flymessage
func (self *PersistentHandler) sendUnFlyMessage(ctx *turbo.DefaultPipelineContext, pevent *persistentEvent) {
	saveSucc := false

	// log.DebugLog("kite_handler", "PersistentHandler|sendUnFlyMessage|%s", pevent.entity)
	var storeCostMs int64
	//提交并且开启优化
	if self.deliveryFirst &&
		pevent.entity.Header.GetCommit() {
		//先投递尝试投递一次再去根据结果写存储
		ch := make(chan []string, 1) //用于返回尝试投递结果
		self.send(ctx, pevent, ch)
		/*如果是成功的则直接返回处理存储成功的
		 *如果失败了，则需要持久化
		 */
		failGroups := <-ch
		//失败或者超时的持久化
		if len(failGroups) > 0 {
			pevent.entity.DeliverCount += 1
			if nil != failGroups {
				pevent.entity.FailGroups = failGroups
			}

			//写入到持久化存储里面
			now := time.Now().UnixNano()
			//写入到持久化存储里面,再投递
			saveSucc = self.kitestore.Save(pevent.entity)
			storeCostMs = (time.Now().UnixNano() - now) / (1000 * 1000)
			if storeCostMs >= 200 {
				log.WarnLog("kite_store", "PersistentHandler|Save Too Long|cost:%d ms|%v", storeCostMs, pevent.entity.Header.String())
			}

			//再投递
			self.send(ctx, pevent, nil)
		} else {
			log.DebugLog("kite_handler", "PersistentHandler|sendUnFlyMessage|FLY|%s", pevent.entity)
		}

	} else {
		now := time.Now().UnixNano()
		//写入到持久化存储里面,再投递
		saveSucc = self.kitestore.Save(pevent.entity)
		storeCostMs = (time.Now().UnixNano() - now) / (1000 * 1000)
		if storeCostMs >= 200 {
			log.WarnLog("kite_store", "PersistentHandler|Save Too Long|cost:%d ms|%v", storeCostMs, pevent.entity.Header.String())
		}

		if saveSucc && pevent.entity.Commit {
			self.send(ctx, pevent, nil)
		}

	}

	//发送存储结果ack
	remoteEvent := turbo.NewRemotingEvent(storeAck(pevent.opaque,
		pevent.entity.Header.GetMessageId(), saveSucc, fmt.Sprintf("Store Result %t Cost:%d", saveSucc, storeCostMs)),
		[]string{pevent.remoteClient.RemoteAddr()})
	ctx.SendForward(remoteEvent)

}

func (self *PersistentHandler) send(ctx *turbo.DefaultPipelineContext, pevent *persistentEvent, ch chan []string) {

	//启动投递当然会重投3次
	preDeliver := NewDeliverPreEvent(
		pevent.entity.Header.GetMessageId(),
		pevent.entity.Header,
		pevent.entity)
	preDeliver.attemptDeliver = ch
	ctx.SendForward(preDeliver)

	// log.DebugLog("kite_handler", "PersistentHandler|send|FULL|TRY SEND BY CURRENT GO ....")
}
