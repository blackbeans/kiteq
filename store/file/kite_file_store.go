package file

import (
	"container/list"
	_ "container/list"
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
	Commit          bool     `json:"commit"`
	FailGroups      []string `json:"fg",omitempty`
	SuccGroups      []string `json:"sg",omitempty`
	NextDeliverTime int64    `json:"ndt"`
	DeliverCount    int32    `json:"dc"`
}

const (
	CONCURRENT_LEVEL = 16
)

type FileStore struct {
	oplogs     []map[string] /*messageId*/ *list.Element //用于oplog的replay
	loglink    []*list.List                              //*opBody
	locks      []*sync.RWMutex
	maxcap     int
	currentSid int64 // 当前segment的id
	snapshot   *MessageStore
	sync.RWMutex
}

func NewFileStore(dir string, maxcap int) *FileStore {

	loglink := make([]*list.List, 0, CONCURRENT_LEVEL)
	oplogs := make([]map[string]*list.Element, 0, CONCURRENT_LEVEL)
	locks := make([]*sync.RWMutex, 0, CONCURRENT_LEVEL)
	for i := 0; i < CONCURRENT_LEVEL; i++ {
		splitMap := make(map[string] /*messageId*/ *list.Element, maxcap/CONCURRENT_LEVEL)
		locks = append(locks, &sync.RWMutex{})
		oplogs = append(oplogs, splitMap)
		loglink = append(loglink, list.New())
	}

	kms := &FileStore{
		loglink: loglink,
		oplogs:  oplogs,
		locks:   locks,
		maxcap:  maxcap / CONCURRENT_LEVEL}

	kms.snapshot =
		NewMessageStore(dir+"/snapshot/", 100, 10, func(ol *oplog) {
			kms.replay(ol)
		})
	return kms
}

//重放当前data的操作日志还原消息状态
func (self *FileStore) replay(ol *oplog) {

	var body opBody
	err := json.Unmarshal(ol.Body, &body)
	if nil != err {
		log.Error("FileStore|replay|FAIL|%s|%s", err, ol.Body)
		return
	}

	ob := &body
	l, link, tol := self.hash(ob.MessageId)

	//如果是更新或者创建，则直接反序列化
	if ol.Op == OP_U || ol.Op == OP_C {
		l.Lock()
		//直接覆盖
		e, ok := tol[ob.MessageId]
		if ok {
			e.Value = ob
		} else {
			e = link.PushFront(ob)
			tol[ob.MessageId] = e
		}
		link.MoveToFront(e)

		l.Unlock()

	} else if ol.Op == OP_D {
		//如果为删除操作直接删除已经保存的op_log
		l.Lock()
		e, ok := tol[ob.MessageId]
		if ok {
			delete(tol, ob.MessageId)
			link.Remove(e)
		}
		l.Unlock()
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
		lock, link, _ := self.hash(fmt.Sprintf("%x", i))
		lock.RLock()
		l += link.Len()
		lock.RUnlock()
	}
	return fmt.Sprintf("memory-length:%d\n", l)
}

func (self *FileStore) AsyncUpdate(entity *MessageEntity) bool { return self.UpdateEntity(entity) }
func (self *FileStore) AsyncDelete(messageId string) bool      { return self.Delete(messageId) }
func (self *FileStore) AsyncCommit(messageId string) bool      { return self.Commit(messageId) }

//hash get elelment
func (self *FileStore) hash(messageid string) (l *sync.RWMutex, link *list.List, ol map[string]*list.Element) {
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
	link = self.loglink[hashId]
	return
}

func (self *FileStore) Query(messageId string) *MessageEntity {

	lock, _, el := self.hash(messageId)
	lock.Lock()
	defer lock.Unlock()
	e, ok := el[messageId]
	if !ok {
		return nil
	}

	v := e.Value.(*opBody)
	var entity MessageEntity
	err := self.snapshot.Query(v.Id, &entity)
	if nil != err {
		log.Error("MessageStore|Query|FAIL|%s", err)
		return nil
	}

	//merge data
	entity.Commit = v.Commit
	entity.FailGroups = v.FailGroups
	entity.SuccGroups = v.SuccGroups
	entity.NextDeliverTime = v.NextDeliverTime
	entity.DeliverCount = v.DeliverCount

	return &entity
}

func (self *FileStore) Save(entity *MessageEntity) bool {

	lock, link, ol := self.hash(entity.MessageId)
	if len(ol) >= self.maxcap {
		//overflow
		return false
	}

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
		NextDeliverTime: entity.NextDeliverTime,
		DeliverCount:    0}

	obd, _ := json.Marshal(ob)
	cmd := NewCommand(-1, entity.MessageId, data, obd)
	//get lock

	lock.Lock()

	//append oplog into file
	id := self.snapshot.Append(cmd)
	ob.Id = id

	//push
	e := link.PushFront(ob)
	ol[entity.MessageId] = e

	lock.Unlock()

	return true
}
func (self *FileStore) Commit(messageId string) bool {
	lock, _, ol := self.hash(messageId)
	lock.Lock()
	defer lock.Unlock()
	e, ok := ol[messageId]
	if !ok {
		return false
	}

	//modify commit
	v := e.Value.(*opBody)
	v.Commit = true

	//write oplog
	obd, _ := json.Marshal(v)
	cmd := NewCommand(v.Id, messageId, nil, obd)
	self.snapshot.Update(cmd)
	return true
}
func (self *FileStore) Rollback(messageId string) bool {
	return self.Delete(messageId)
}
func (self *FileStore) UpdateEntity(entity *MessageEntity) bool {
	lock, _, el := self.hash(entity.MessageId)
	lock.Lock()
	defer lock.Unlock()
	e, ok := el[entity.MessageId]
	if !ok {
		return true
	}
	//modify opbody value
	v := e.Value.(*opBody)
	v.DeliverCount = entity.DeliverCount
	v.NextDeliverTime = entity.NextDeliverTime
	v.SuccGroups = entity.SuccGroups
	v.FailGroups = entity.FailGroups
	//append log
	obd, _ := json.Marshal(v)
	cmd := NewCommand(v.Id, entity.MessageId, nil, obd)
	self.snapshot.Update(cmd)
	return true
}
func (self *FileStore) Delete(messageId string) bool {
	lock, link, el := self.hash(messageId)
	lock.Lock()
	defer lock.Unlock()
	self.innerDelete(messageId, link, el)
	return true

}

func (self *FileStore) innerDelete(messageId string, link *list.List,
	el map[string]*list.Element) {
	e, ok := el[messageId]
	if !ok {
		return
	}

	//delete log
	delete(el, messageId)
	link.Remove(e)

	v := e.Value.(*opBody)

	//delete
	obd, _ := json.Marshal(v)
	cmd := NewCommand(v.Id, messageId, nil, obd)
	self.snapshot.Delete(cmd)
	// log.Info("FileStore|innerDelete|%s\n", messageId)
}

//根据kiteServer名称查询需要重投的消息 返回值为 是否还有更多、和本次返回的数据结果
func (self *FileStore) PageQueryEntity(hashKey string, kiteServer string, nextDeliveryTime int64, startIdx, limit int) (bool, []*MessageEntity) {

	var pe []*MessageEntity

	lock, link, _ := self.hash(hashKey)
	lock.RLock()
	i := 0
	for e := link.Back(); nil != e; e = e.Prev() {
		ob := e.Value.(*opBody)
		//query message
		if ob.NextDeliverTime <= nextDeliveryTime {
			if startIdx <= i {
				if nil == pe {
					pe = make([]*MessageEntity, 0, limit+1)
				}

				//创建消息
				entity := &MessageEntity{
					MessageId: ob.MessageId}
				entity.Commit = ob.Commit
				entity.FailGroups = ob.FailGroups
				entity.SuccGroups = ob.SuccGroups
				entity.NextDeliverTime = ob.NextDeliverTime
				entity.DeliverCount = ob.DeliverCount
				pe = append(pe, entity)
			}

			i++
			if len(pe) > limit {
				break
			}
		}
	}

	lock.RUnlock()

	if len(pe) > limit {
		return true, pe[:limit]
	} else {
		return false, pe
	}
	return false, nil

}
