package handler

import (
	"errors"
	"fmt"
	"github.com/blackbeans/kiteq-common/stat"
	"github.com/blackbeans/kiteq-common/store"
	log "github.com/blackbeans/log4go"
	p "github.com/blackbeans/turbo/pipe"
	"time"
)

var ERROR_PERSISTENT = errors.New("persistent msg error!")

//----------------持久化的handler
type PersistentHandler struct {
	p.BaseForwardHandler
	kitestore      store.IKiteStore
	deliverTimeout time.Duration
	flowstat       *stat.FlowStat //当前优化是否开启 true为开启，false为关闭
	deliveryFirst  bool           //是否优先投递
}

//------创建persitehandler
func NewPersistentHandler(name string, deliverTimeout time.Duration,
	kitestore store.IKiteStore, deliveryFirst bool, flowstat *stat.FlowStat) *PersistentHandler {
	phandler := &PersistentHandler{}
	phandler.BaseForwardHandler = p.NewBaseForwardHandler(name, phandler)
	phandler.kitestore = kitestore
	phandler.deliverTimeout = deliverTimeout
	phandler.flowstat = flowstat
	phandler.deliveryFirst = deliveryFirst
	return phandler
}

func (self *PersistentHandler) TypeAssert(event p.IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *PersistentHandler) cast(event p.IEvent) (val *persistentEvent, ok bool) {
	val, ok = event.(*persistentEvent)
	return
}

func (self *PersistentHandler) Process(ctx *p.DefaultPipelineContext, event p.IEvent) error {

	pevent, ok := self.cast(event)
	if !ok {
		return p.ERROR_INVALID_EVENT_TYPE
	}

	if nil != pevent.entity {
		//如果是fly模式不做持久化
		if pevent.entity.Header.GetFly() {
			if pevent.entity.Header.GetCommit() {
				//如果是成功存储的、并且为未提交的消息，则需要发起一个ack的命令
				//发送存储结果ack
				remoteEvent := p.NewRemotingEvent(storeAck(pevent.opaque,
					pevent.entity.Header.GetMessageId(), true, "FLY NO NEED SAVE"), []string{pevent.remoteClient.RemoteAddr()})
				ctx.SendForward(remoteEvent)

				self.send(ctx, pevent, nil)
			} else {
				remoteEvent := p.NewRemotingEvent(storeAck(pevent.opaque,
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
func (self *PersistentHandler) sendUnFlyMessage(ctx *p.DefaultPipelineContext, pevent *persistentEvent) {
	saveSucc := true

	// log.DebugLog("kite_handler", "PersistentHandler|sendUnFlyMessage|%s", pevent.entity)

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
			saveSucc = self.kitestore.Save(pevent.entity)
			//再投递
			self.send(ctx, pevent, nil)
		} else {
			log.DebugLog("kite_handler", "PersistentHandler|sendUnFlyMessage|FLY|%s", pevent.entity)
		}

	} else {
		// now := time.Now().UnixNano()
		//写入到持久化存储里面,再投递
		saveSucc = self.kitestore.Save(pevent.entity)
		// log.DebugLog("kite_handler", "PersistentHandler|sendUnFlyMessage|cost:%d", time.Now().UnixNano()-now)
		if saveSucc && pevent.entity.Commit {
			self.send(ctx, pevent, nil)
		}

	}

	//发送存储结果ack
	remoteEvent := p.NewRemotingEvent(storeAck(pevent.opaque,
		pevent.entity.Header.GetMessageId(), saveSucc, fmt.Sprintf("Store Result %t", saveSucc)),
		[]string{pevent.remoteClient.RemoteAddr()})
	ctx.SendForward(remoteEvent)

}

func (self *PersistentHandler) send(ctx *p.DefaultPipelineContext, pevent *persistentEvent, ch chan []string) {

	//启动投递当然会重投3次
	preDeliver := NewDeliverPreEvent(
		pevent.entity.Header.GetMessageId(),
		pevent.entity.Header,
		pevent.entity)
	preDeliver.attemptDeliver = ch
	ctx.SendForward(preDeliver)

	// log.DebugLog("kite_handler", "PersistentHandler|send|FULL|TRY SEND BY CURRENT GO ....")
}
