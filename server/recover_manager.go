package server

import (
	// "encoding/json"
	"fmt"
	"kiteq/handler"
	"time"

	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/store"
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"
	"github.com/blackbeans/turbo/packet"
	. "github.com/blackbeans/turbo/pipe"
)

//-----------recover的handler
type RecoverManager struct {
	pipeline      *DefaultPipeline
	serverName    string
	isClose       bool
	kitestore     store.IKiteStore
	recoverPeriod time.Duration
	recoveLimiter *turbo.BurstyLimiter
}

//------创建persitehandler
func NewRecoverManager(serverName string, recoverPeriod time.Duration,
	pipeline *DefaultPipeline, kitestore store.IKiteStore, tw *turbo.TimerWheel) *RecoverManager {

	limter, _ := turbo.NewBurstyLimiter(2000, 2000)
	rm := &RecoverManager{
		serverName:    serverName,
		kitestore:     kitestore,
		isClose:       false,
		pipeline:      pipeline,
		recoverPeriod: recoverPeriod,
		recoveLimiter: limter}
	return rm
}

//开始启动恢复程序
func (self *RecoverManager) Start() {
	for i := 0; i < self.kitestore.RecoverNum(); i++ {
		go self.startRecoverTask(fmt.Sprintf("%x", i))
	}

	log.InfoLog("kite_recover", "RecoverManager|Start|SUCC....")
}

func (self *RecoverManager) startRecoverTask(hashKey string) {
	defer func() {

		log.ErrorLog("kite_recover", "RecoverManager|startRecoverTask|STOPPED|%s|%v", hashKey)

	}()
	// log.Info("RecoverManager|startRecoverTask|SUCC|%s....", hashKey)
	for !self.isClose {
		//开始
		count := self.redeliverMsg(hashKey, time.Now())
		log.InfoLog("kite_recover", "RecoverManager|endRecoverTask|%s|count:%d....", hashKey, count)
		time.Sleep(self.recoverPeriod)
	}

}

func (self *RecoverManager) Stop() {
	self.isClose = true
}

func (self *RecoverManager) redeliverMsg(hashKey string, now time.Time) int {
	startIdx := 0
	preTimestamp := now.Unix()
	//开始分页查询未过期的消息实体
	for !self.isClose {
		_, entities := self.kitestore.PageQueryEntity(hashKey, self.serverName,
			preTimestamp, startIdx, 200)

		if len(entities) <= 0 {
			break
		}
		// d, _ := json.Marshal(entities[0].Header)
		// log.InfoLog("kite_recover", "RecoverManager|redeliverMsg|%d|%s", now.Unix(), string(d))
		succ := self.recoveLimiter.AcquireCount(len(entities))
		if succ {
			//开始发起重投
			for _, entity := range entities {
				//如果为未提交的消息则需要发送一个事务检查的消息
				if !entity.Commit {
					self.txAck(entity)
				} else {
					//发起投递事件
					self.delivery(entity)
				}
			}
		}
		startIdx += len(entities)
		// hasMore = more
		preTimestamp = entities[len(entities)-1].NextDeliverTime
	}
	return startIdx
}

//发起投递事件
func (self *RecoverManager) delivery(entity *store.MessageEntity) {
	deliver := handler.NewDeliverPreEvent(
		entity.MessageId,
		entity.Header,
		nil)
	//会先经过pre处理器填充额外信息
	self.pipeline.FireWork(deliver)
}

//发送事务ack信息
func (self *RecoverManager) txAck(entity *store.MessageEntity) {

	txack := protocol.MarshalTxACKPacket(entity.Header, protocol.TX_UNKNOWN, "Server Check")
	p := packet.NewPacket(protocol.CMD_TX_ACK, txack)
	//向头部的发送分组发送txack消息
	groupId := entity.PublishGroup
	event := NewRemotingEvent(p, nil, groupId)
	self.pipeline.FireWork(event)
}
