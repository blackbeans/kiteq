package server

import (
	"fmt"
	"kiteq/handler"
	. "kiteq/pipe"
	"kiteq/protocol"
	"kiteq/store"
	"log"
	"time"
)

//-----------recover的handler
type RecoverManager struct {
	pipeline       *DefaultPipeline
	serverName     string
	isClose        bool
	kitestore      store.IKiteStore
	recoverPeriod  time.Duration
	recoverWorkers chan byte
}

//------创建persitehandler
func NewRecoverManager(serverName string, recoverPeriod time.Duration, pipeline *DefaultPipeline, kitestore store.IKiteStore) *RecoverManager {

	rm := &RecoverManager{
		serverName:     serverName,
		kitestore:      kitestore,
		isClose:        false,
		pipeline:       pipeline,
		recoverPeriod:  recoverPeriod,
		recoverWorkers: make(chan byte, 100)}
	return rm
}

//开始启动恢复程序
func (self *RecoverManager) Start() {
	for i := 0; i < 16; i++ {
		go self.startRecoverTask(fmt.Sprintf("%x", i))
	}

	log.Println("RecoverManager|Start|SUCC....")
}

func (self *RecoverManager) startRecoverTask(hashKey string) {
	ticker := time.NewTicker(self.recoverPeriod)
	defer ticker.Stop()
	for !self.isClose {
		now := <-ticker.C
		//开始
		self.redeliverMsg(hashKey, now)
	}

}

func (self *RecoverManager) Stop() {
	self.isClose = true
}

func (self *RecoverManager) redeliverMsg(hashKey string, now time.Time) {
	var hasMore bool = true

	startIdx := 0
	//开始分页查询未过期的消息实体
	for !self.isClose && hasMore {
		more, entities := self.kitestore.PageQueryEntity(hashKey, self.serverName,
			now.Unix(), startIdx, 100)
		// log.Printf("RecoverManager|redeliverMsg|%d|%d\n", now.Unix(), len(entities))
		if len(entities) <= 0 {
			break
		}

		//开始发起重投
		for _, entity := range entities {
			self.recoverWorkers <- 1
			go func() {
				defer func() {
					<-self.recoverWorkers
				}()
				//如果为未提交的消息则需要发送一个事务检查的消息
				if !entity.Commit {
					self.txAck(entity)
				} else {

					//发起投递事件
					self.delivery(entity)
				}
			}()
		}
		startIdx += len(entities)
		hasMore = more
	}
}

//发起投递事件
func (self *RecoverManager) delivery(entity *store.MessageEntity) {
	deliver := handler.NewDeliverPreEvent(
		entity.Header.GetMessageId(),
		entity.Header,
		nil)
	//会先经过pre处理器填充额外信息
	self.pipeline.FireWork(deliver)
}

//发送事务ack信息
func (self *RecoverManager) txAck(entity *store.MessageEntity) {

	txack := protocol.MarshalTxACKPacket(entity.Header, protocol.TX_UNKNOWN, "Server Check")
	packet := protocol.NewPacket(protocol.CMD_TX_ACK, txack)
	//向头部的发送分组发送txack消息
	groupId := entity.PublishGroup
	event := NewRemotingEvent(packet, nil, groupId)
	self.pipeline.FireWork(event)
}
