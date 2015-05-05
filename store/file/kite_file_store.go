package file

import (
	"container/list"
	"encoding/json"
	"fmt"
	log "github.com/blackbeans/log4go"
	. "kiteq/store"
	"strconv"
	"sync"
)

//delvier tags
type opBody struct {
	Id              int64    `json:"id"`
	MessageId       string   `json:"mid"`
	Commit          bool     `json:"commit",omitempty`
	FailGroups      []string `json:"fg",omitempty`
	SuccGroups      []string `json:"sg",omitempty`
	NextDeliverTime int64    `json:"ndt",omitempty`
}

const (
	CONCURRENT_LEVEL = 16
)

type FileStore struct {
	oplogs []map[string] /*messageId*/ *opBody //用于oplog的replay

	locks      []*sync.RWMutex
	maxcap     int
	currentSid int64 // 当前segment的id
	snapshot   *MessageStore
	sync.RWMutex
}

func NewFileStore(initcap, maxcap int) *FileStore {

	oplogs := make([]map[string]*opBody, 0, CONCURRENT_LEVEL)
	locks := make([]*sync.RWMutex, 0, CONCURRENT_LEVEL)
	for i := 0; i < CONCURRENT_LEVEL; i++ {
		splitMap := make(map[string] /*messageId*/ *opBody, maxcap/CONCURRENT_LEVEL)
		locks = append(locks, &sync.RWMutex{})
		oplogs = append(oplogs, splitMap)
	}

	kms := &FileStore{
		oplogs: oplogs,
		locks:  locks,
		maxcap: maxcap / CONCURRENT_LEVEL}

	kms.snapshot =
		NewMessageStore("./snapshot/", 100, 10, func(ol *oplog) {
			kms.replay(ol)
		})
	return kms
}

//重放当前data的操作日志还原消息状态
func (self *FileStore) replay(ol *oplog) {

	var body opBody
	err := json.Unmarshal([]byte(ol.Body), &body)
	if nil != err {
		log.Error("FileStore|replay|FAIL|%s|%s", err, ol.Body)
		return
	}

	ob := &body
	l, tol := self.hash(ob.MessageId)
	l.Lock()
	defer l.Unlock()
	//如果是更新或者创建，则直接反序列化
	if ol.Op == OP_U || ol.Op == OP_C {
		//直接覆盖
		tol[ob.MessageId] = ob

	} else if ol.Op == OP_D {
		//如果为删除操作直接删除已经保存的op_log
		delete(tol, ob.MessageId)
	}

}

func (self *FileStore) Start() {

	//start snapshost
	self.snapshot.Start()

}
func (self *FileStore) Stop() {

	//save queue message into snapshot
	// self.syncToFile()
	self.snapshot.Destory()
	log.Info("FileStore|Stop...")

}

func (self *FileStore) RecoverNum() int {
	return CONCURRENT_LEVEL
}

func (self *FileStore) Monitor() string {
	l := 0
	for i := 0; i < CONCURRENT_LEVEL; i++ {
		lock, dl := self.hash(fmt.Sprintf("%x", i))
		lock.RLock()
		l += len(dl)
		lock.RUnlock()
	}
	return fmt.Sprintf("memory-length:%d\n", l)
}

func (self *FileStore) AsyncUpdate(entity *MessageEntity) bool { return self.UpdateEntity(entity) }
func (self *FileStore) AsyncDelete(messageId string) bool      { return self.Delete(messageId) }
func (self *FileStore) AsyncCommit(messageId string) bool      { return self.Commit(messageId) }

//hash get elelment
func (self *FileStore) hash(messageid string) (l *sync.RWMutex, ol map[string]*opBody) {
	id := string(messageid[len(messageid)-1])
	i, err := strconv.ParseInt(id, CONCURRENT_LEVEL, 8)
	hashId := int(i)
	if nil != err {
		log.Error("FileStore|hash|INVALID MESSAGEID|%s\n", messageid)
		hashId = 0
	} else {
		hashId = hashId % CONCURRENT_LEVEL
	}

	// log.Debug("FileStore|hash|%s|%d\n", messageid, hashId)

	//hash part
	l = self.locks[hashId]
	ol = self.oplogs[hashId]
	return
}

func (self *FileStore) Query(messageId string) *MessageEntity {
	return nil
}

func (self *FileStore) Save(entity *MessageEntity) bool {

	data, err := json.Marshal(entity)
	if nil != err {
		log.Error("FileStore|Save|FAIL|%s", err)
		return false
	}

	//create oplog
	ob := &opBody{
		MessageId:       entity.MessageId,
		Commit:          entity.Commit,
		FailGroups:      entity.FailGroups,
		SuccGroups:      entity.SuccGroups,
		NextDeliverTime: entity.NextDeliverTime}

	obd, _ := json.Marshal(ob)
	cmd := NewCommand(entity.MessageId, data, obd)
	//get lock
	lock, ol := self.hash(entity.MessageId)
	lock.Lock()

	//append oplog into file
	id := self.snapshot.Append(cmd)
	ob.Id = id
	ol[entity.MessageId] = ob

	lock.Unlock()

	return true
}
func (self *FileStore) Commit(messageId string) bool {
	lock, ol := self.hash(messageId)
	lock.Lock()
	defer lock.Unlock()
	e, ok := ol[messageId]
	if !ok {
		return false
	}
	e.Commit = true

	obd, _ := json.Marshal(e)
	cmd := NewCommand(messageId, nil, obd)
	self.snapshot.Update(cmd)
	return true
}
func (self *FileStore) Rollback(messageId string) bool {
	return self.Delete(messageId)
}
func (self *FileStore) UpdateEntity(entity *MessageEntity) bool {
	// lock, el, _ := self.hash(entity.MessageId)
	// lock.Lock()
	// defer lock.Unlock()
	// v, ok := el[entity.MessageId]
	// if !ok {
	// 	return true
	// }

	// e := v.Value.(*MessageEntity)
	// e.DeliverCount = entity.DeliverCount
	// e.NextDeliverTime = entity.NextDeliverTime
	// e.SuccGroups = entity.SuccGroups
	// e.FailGroups = entity.FailGroups
	return true
}
func (self *FileStore) Delete(messageId string) bool {
	// lock, el, dl := self.hash(messageId)
	// lock.Lock()
	// defer lock.Unlock()
	// self.innerDelete(messageId, el, dl)
	return true

}

func (self *FileStore) innerDelete(messageId string,
	el map[string]*list.Element, dl *list.List) {
	// e, ok := el[messageId]
	// if !ok {
	// 	return
	// }
	// delete(el, messageId)
	// dl.Remove(e)
	// e = nil
	// log.Info("FileStore|innerDelete|%s\n", messageId)
}

//根据kiteServer名称查询需要重投的消息 返回值为 是否还有更多、和本次返回的数据结果
func (self *FileStore) PageQueryEntity(hashKey string, kiteServer string, nextDeliveryTime int64, startIdx, limit int) (bool, []*MessageEntity) {

	// var pe []*MessageEntity
	// var delMessage []string

	// lock, el, dl := self.hash(hashKey)
	// lock.RLock()
	// i := 0
	// for e := dl.Back(); nil != e; e = e.Prev() {
	// 	entity := e.Value.(*MessageEntity)
	// 	if entity.NextDeliverTime <= nextDeliveryTime &&
	// 		entity.DeliverCount < entity.Header.GetDeliverLimit() &&
	// 		entity.ExpiredTime > nextDeliveryTime {
	// 		if startIdx <= i {
	// 			if nil == pe {
	// 				pe = make([]*MessageEntity, 0, limit+1)
	// 			}
	// 			pe = append(pe, entity)
	// 		}

	// 		i++
	// 		if len(pe) > limit {
	// 			break
	// 		}
	// 	} else if entity.DeliverCount >= entity.Header.GetDeliverLimit() ||
	// 		entity.ExpiredTime <= nextDeliveryTime {
	// 		if nil == delMessage {
	// 			delMessage = make([]string, 0, 10)
	// 		}
	// 		delMessage = append(delMessage, entity.MessageId)
	// 	}
	// }

	// lock.RUnlock()

	// //删除过期的message
	// if nil != delMessage {
	// 	lock.Lock()
	// 	for _, v := range delMessage {
	// 		self.innerDelete(v, el, dl)
	// 	}
	// 	lock.Unlock()
	// }

	// //no data notify load segment
	// if len(pe) <= 0 {
	// 	self.snapshotNotify <- true
	// }

	// if len(pe) > limit {
	// 	return true, pe[:limit]
	// } else {
	// 	return false, pe
	// }

	return false, nil

}
