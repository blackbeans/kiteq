package memory

import (
	"container/list"
	"fmt"
	log "github.com/blackbeans/log4go"
	. "kiteq/store"
	"strconv"
	"sync"
)

const (
	CONCURRENT_LEVEL = 16
)

type KiteMemoryStore struct {
	datalinks []*list.List                              //用于LRU
	stores    []map[string] /*messageId*/ *list.Element //用于LRU
	locks     []*sync.RWMutex
	maxcap    int
}

func NewKiteMemoryStore(initcap, maxcap int) *KiteMemoryStore {

	//定义holder
	datalinks := make([]*list.List, 0, CONCURRENT_LEVEL)
	stores := make([]map[string] /*messageId*/ *list.Element, 0, CONCURRENT_LEVEL)
	locks := make([]*sync.RWMutex, 0, CONCURRENT_LEVEL)
	for i := 0; i < CONCURRENT_LEVEL; i++ {
		splitMap := make(map[string] /*messageId*/ *list.Element, maxcap/CONCURRENT_LEVEL)
		stores = append(stores, splitMap)
		locks = append(locks, &sync.RWMutex{})
		datalinks = append(datalinks, list.New())
	}

	return &KiteMemoryStore{
		datalinks: datalinks,
		stores:    stores,
		locks:     locks,
		maxcap:    maxcap / CONCURRENT_LEVEL}
}

func (self *KiteMemoryStore) Start() {}
func (self *KiteMemoryStore) Stop()  {}

func (self *KiteMemoryStore) RecoverNum() int {
	return CONCURRENT_LEVEL
}

func (self *KiteMemoryStore) Monitor() string {
	l := 0
	for i := 0; i < CONCURRENT_LEVEL; i++ {
		lock, _, dl := self.hash(fmt.Sprintf("%x", i))
		lock.RLock()
		l += dl.Len()
		lock.RUnlock()
	}
	return fmt.Sprintf("memory-length:%d\n", l)
}

func (self *KiteMemoryStore) AsyncUpdate(entity *MessageEntity) bool { return self.UpdateEntity(entity) }
func (self *KiteMemoryStore) AsyncDelete(messageId string) bool      { return self.Delete(messageId) }
func (self *KiteMemoryStore) AsyncCommit(messageId string) bool      { return self.Commit(messageId) }

//hash get elelment
func (self *KiteMemoryStore) hash(messageid string) (l *sync.RWMutex, e map[string]*list.Element, lt *list.List) {
	id := string(messageid[len(messageid)-1])
	i, err := strconv.ParseInt(id, CONCURRENT_LEVEL, 8)
	hashId := int(i)
	if nil != err {
		log.Error("KiteMemoryStore|hash|INVALID MESSAGEID|%s\n", messageid)
		hashId = 0
	} else {
		hashId = hashId % CONCURRENT_LEVEL
	}

	// log.Debug("KiteMemoryStore|hash|%s|%d\n", messageid, hashId)

	//hash part
	l = self.locks[hashId]
	e = self.stores[hashId]
	lt = self.datalinks[hashId]
	return
}

func (self *KiteMemoryStore) Query(messageId string) *MessageEntity {
	lock, el, _ := self.hash(messageId)
	lock.RLock()
	defer lock.RUnlock()
	e, ok := el[messageId]
	if !ok {
		return nil
	}
	//将当前节点放到最前面
	return e.Value.(*MessageEntity)
}

func (self *KiteMemoryStore) Save(entity *MessageEntity) bool {
	lock, el, dl := self.hash(entity.MessageId)
	lock.Lock()
	defer lock.Unlock()

	//没有空闲node，则判断当前的datalinke中是否达到容量上限
	cl := dl.Len()
	if cl >= self.maxcap {
		log.Warn("KiteMemoryStore|SAVE|OVERFLOW|%d/%d\n", cl, self.maxcap)
		return false

	} else {
		front := dl.PushFront(entity)
		el[entity.MessageId] = front
	}

	return true
}
func (self *KiteMemoryStore) Commit(messageId string) bool {
	lock, el, _ := self.hash(messageId)
	lock.Lock()
	defer lock.Unlock()
	e, ok := el[messageId]
	if !ok {
		return false
	}
	entity := e.Value.(*MessageEntity)
	entity.Commit = true
	return true
}
func (self *KiteMemoryStore) Rollback(messageId string) bool {
	return self.Delete(messageId)
}
func (self *KiteMemoryStore) UpdateEntity(entity *MessageEntity) bool {
	lock, el, _ := self.hash(entity.MessageId)
	lock.Lock()
	defer lock.Unlock()
	v, ok := el[entity.MessageId]
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
	lock, el, dl := self.hash(messageId)
	lock.Lock()
	defer lock.Unlock()
	self.innerDelete(messageId, el, dl)
	return true

}

func (self *KiteMemoryStore) innerDelete(messageId string,
	el map[string]*list.Element, dl *list.List) {
	e, ok := el[messageId]
	if !ok {
		return
	}
	delete(el, messageId)
	dl.Remove(e)
	e = nil
	// log.Info("KiteMemoryStore|innerDelete|%s\n", messageId)
}

func (self *KiteMemoryStore) Expired(messageId string) bool {
	succ := self.Delete(messageId)
	return succ

}

//根据kiteServer名称查询需要重投的消息 返回值为 是否还有更多、和本次返回的数据结果
func (self *KiteMemoryStore) PageQueryEntity(hashKey string, kiteServer string, nextDeliveryTime int64, startIdx, limit int) (bool, []*MessageEntity) {

	pe := make([]*MessageEntity, 0, limit+1)
	var delMessage []string

	lock, el, dl := self.hash(hashKey)
	lock.RLock()

	i := 0
	for e := dl.Back(); nil != e; e = e.Prev() {
		entity := e.Value.(*MessageEntity)
		if entity.NextDeliverTime <= nextDeliveryTime &&
			entity.DeliverCount < entity.Header.GetDeliverLimit() &&
			entity.ExpiredTime > nextDeliveryTime {
			if startIdx <= i {
				pe = append(pe, entity)
			}

			i++
			if len(pe) > limit {
				break
			}
		} else if entity.DeliverCount >= entity.Header.GetDeliverLimit() ||
			entity.ExpiredTime <= nextDeliveryTime {
			if nil == delMessage {
				delMessage = make([]string, 0, 10)
			}
			delMessage = append(delMessage, entity.MessageId)
		}
	}

	lock.RUnlock()

	//删除过期的message
	if nil != delMessage {
		lock.Lock()
		for _, v := range delMessage {
			self.innerDelete(v, el, dl)
		}
		lock.Unlock()
	}

	if len(pe) > limit {
		return true, pe[:limit]
	} else {
		return false, pe
	}

}
