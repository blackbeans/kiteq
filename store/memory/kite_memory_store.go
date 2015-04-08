package memory

import (
	"container/list"
	"fmt"
	log "github.com/blackbeans/log4go"
	. "kiteq/store"
	"sync"
	"time"
)

type KiteMemoryStore struct {
	datalink *list.List                              //用于LRU
	idx      map[string] /*messageId*/ *list.Element //用于LRU
	lock     sync.RWMutex
	maxcap   int
}

func NewKiteMemoryStore(initcap, maxcap int) *KiteMemoryStore {
	return &KiteMemoryStore{
		datalink: list.New(),
		idx:      make(map[string]*list.Element, initcap),
		maxcap:   maxcap}
}

func (self *KiteMemoryStore) Start() {}
func (self *KiteMemoryStore) Stop()  {}

func (self *KiteMemoryStore) Monitor() string {
	return fmt.Sprintf("mmap-msg-length:%d\n", self.datalink.Len())
}

func (self *KiteMemoryStore) AsyncUpdate(entity *MessageEntity) bool { return self.UpdateEntity(entity) }
func (self *KiteMemoryStore) AsyncDelete(messageId string) bool      { return self.Delete(messageId) }
func (self *KiteMemoryStore) AsyncCommit(messageId string) bool      { return self.Commit(messageId) }

func (self *KiteMemoryStore) Query(messageId string) *MessageEntity {
	self.lock.RLock()
	defer self.lock.RUnlock()
	e, ok := self.idx[messageId]
	if !ok {
		return nil
	}
	//将当前节点放到最前面
	return e.Value.(*MessageEntity)
}

func (self *KiteMemoryStore) Save(entity *MessageEntity) bool {
	self.lock.Lock()
	defer self.lock.Unlock()

	//没有空闲node，则判断当前的datalinke中是否达到容量上限
	cl := self.datalink.Len()
	if cl >= self.maxcap {
		log.Warn("KiteMemoryStore|SAVE|OVERFLOW|%d/%d\n", cl, self.maxcap)
		return false

	} else {
		front := self.datalink.PushFront(entity)
		self.idx[entity.MessageId] = front
	}

	return true
}
func (self *KiteMemoryStore) Commit(messageId string) bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	e, ok := self.idx[messageId]
	if !ok {
		return false
	}
	entity := e.Value.(*MessageEntity)
	entity.Commit = true
	return true
}
func (self *KiteMemoryStore) Rollback(messageId string) bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	e, ok := self.idx[messageId]
	if !ok {
		return true
	}
	delete(self.idx, messageId)
	self.datalink.Remove(e)
	return true
}
func (self *KiteMemoryStore) UpdateEntity(entity *MessageEntity) bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	v, ok := self.idx[entity.MessageId]
	if !ok {
		return true
	}

	e := v.Value.(*MessageEntity)
	e.DeliverCount = entity.DeliverCount
	e.NextDeliverTime = entity.NextDeliverTime
	e.SuccGroups = entity.SuccGroups
	e.FailGroups = entity.FailGroups
	return true
}
func (self *KiteMemoryStore) Delete(messageId string) bool {
	return self.Rollback(messageId)

}

//根据kiteServer名称查询需要重投的消息 返回值为 是否还有更多、和本次返回的数据结果
func (self *KiteMemoryStore) PageQueryEntity(hashKey string, kiteServer string, nextDeliveryTime int64, startIdx, limit int) (bool, []*MessageEntity) {
	pe := make([]*MessageEntity, 0, limit+1)
	var delMessage []string

	self.lock.RLock()

	now := time.Now().Unix()
	for e := self.datalink.Back(); nil != e; e = e.Prev() {
		entity := e.Value.(*MessageEntity)
		if entity.NextDeliverTime <= nextDeliveryTime &&
			entity.DeliverCount < entity.Header.GetDeliverLimit() &&
			entity.ExpiredTime <= now {
			pe = append(pe, entity)
			if len(pe) > limit {
				return true, pe[:limit]
			}
		} else if entity.DeliverCount >= entity.Header.GetDeliverLimit() ||
			entity.ExpiredTime > now {
			if nil == delMessage {
				delMessage = make([]string, 0, 10)
			}
			delMessage = append(delMessage, entity.MessageId)
		}
	}

	self.lock.RUnlock()
	//删除过期的message
	if nil != delMessage {
		for _, v := range delMessage {
			self.Delete(v)
		}
	}

	return false, pe
}
