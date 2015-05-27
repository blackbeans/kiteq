package handler

import (
	"errors"
	// log "github.com/blackbeans/log4go"
	. "github.com/blackbeans/turbo/pipe"
	"kiteq/stat"
	"kiteq/store"
	"time"
)

var ERROR_PERSISTENT = errors.New("persistent msg error!")

//----------------持久化的handler
type PersistentHandler struct {
	BaseForwardHandler
	kitestore      store.IKiteStore
	deliverTimeout time.Duration
	flowstat       *stat.FlowStat //当前优化是否开启 true为开启，false为关闭
	fly            bool           //是否开启飞行模式
}

//------创建persitehandler
func NewPersistentHandler(name string, deliverTimeout time.Duration,
	kitestore store.IKiteStore, fly bool, flowstat *stat.FlowStat) *PersistentHandler {
	phandler := &PersistentHandler{}
	phandler.BaseForwardHandler = NewBaseForwardHandler(name, phandler)
	phandler.kitestore = kitestore
	phandler.deliverTimeout = deliverTimeout
	phandler.flowstat = flowstat
	phandler.fly = fly
	return phandler
}

func (self *PersistentHandler) TypeAssert(event IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *PersistentHandler) cast(event IEvent) (val *persistentEvent, ok bool) {
	val, ok = event.(*persistentEvent)
	return
}

func (self *PersistentHandler) Process(ctx *DefaultPipelineContext, event IEvent) error {

	pevent, ok := self.cast(event)
	if !ok {
		return ERROR_INVALID_EVENT_TYPE
	}

	if nil != pevent.entity {
		//如果是fly模式不做持久化
		if pevent.entity.Header.GetFly() {
			if pevent.entity.Header.GetCommit() {
				//如果是成功存储的、并且为未提交的消息，则需要发起一个ack的命令
				//发送存储结果ack
				remoteEvent := NewRemotingEvent(storeAck(pevent.opaque,
					pevent.entity.Header.GetMessageId(), true, "FLY NO NEED SAVE"), []string{pevent.remoteClient.RemoteAddr()})
				ctx.SendForward(remoteEvent)

				self.send(ctx, pevent, nil)
			} else {
				remoteEvent := NewRemotingEvent(storeAck(pevent.opaque,
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
func (self *PersistentHandler) sendUnFlyMessage(ctx *DefaultPipelineContext, pevent *persistentEvent) {
	saveSucc := true

	// log.Info("PersistentHandler|sendUnFlyMessage|%s", pevent.entity)

	//提交并且开启优化
	if self.fly &&
		pevent.entity.Commit && self.flowstat.OptimzeStatus {
		//先投递再去根据结果写存储
		ch := make(chan []string, 3) //用于返回尝试投递结果
		self.send(ctx, pevent, ch)
		/*如果是成功的则直接返回处理存储成功的
		 *如果失败了，则需要持久化
		 */
		var failGroups *[]string
		select {
		case fg := <-ch:
			failGroups = &fg
		case <-time.After(self.deliverTimeout):

		}
		//失败或者超时的持久化
		if nil == failGroups || len(*failGroups) > 0 {
			pevent.entity.DeliverCount = 1
			if nil != failGroups {
				pevent.entity.FailGroups = *failGroups
			}

			//写入到持久化存储里面
			saveSucc = self.kitestore.Save(pevent.entity)
		}

	} else {
		//写入到持久化存储里面,再投递
		saveSucc = self.kitestore.Save(pevent.entity)
		if pevent.entity.Commit {
			self.send(ctx, pevent, nil)
		}
	}

	//发送存储结果ack
	remoteEvent := NewRemotingEvent(storeAck(pevent.opaque,
		pevent.entity.Header.GetMessageId(), saveSucc, "Store SUCC"), []string{pevent.remoteClient.RemoteAddr()})
	ctx.SendForward(remoteEvent)

}

func (self *PersistentHandler) send(ctx *DefaultPipelineContext, pevent *persistentEvent, ch chan []string) {

	//启动投递当然会重投3次
	preDeliver := NewDeliverPreEvent(
		pevent.entity.Header.GetMessageId(),
		pevent.entity.Header,
		pevent.entity)
	preDeliver.attemptDeliver = ch
	ctx.SendForward(preDeliver)

	// log.Println("PersistentHandler|send|FULL|TRY SEND BY CURRENT GO ....")
}
