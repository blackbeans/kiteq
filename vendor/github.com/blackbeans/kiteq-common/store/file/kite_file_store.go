package file

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/blackbeans/kiteq-common/protocol"
	. "github.com/blackbeans/kiteq-common/store"
	log "github.com/blackbeans/log4go"
	"strconv"
	"sync"
	"time"
)

//delvier tags
type opBody struct {
	Id              int64      `json:"id"`
	MessageId       string     `json:"mid"`
	Topic           string     `json:"topic"`
	Commit          bool       `json:"commit"`
	FailGroups      []string   `json:"fg",omitempty`
	SuccGroups      []string   `json:"sg",omitempty`
	NextDeliverTime int64      `json:"ndt"`
	DeliverCount    int32      `json:"dc"`
	saveDone        chan int64 //save done
}

const (
	CONCURRENT_LEVEL = 16
)

type KiteFileStore struct {
	oplogs     []map[string] /*messageId*/ *list.Element //用于oplog的replay
	datalink   []*list.List                              //*opBody
	locks      []*sync.RWMutex
	maxcap     int
	currentSid int64 // 当前segment的id
	snapshot   *MessageStore
	delChan    []chan *command
	sync.RWMutex
	sync.WaitGroup
	running bool
}

func NewKiteFileStore(dir string, batchFlush int, maxcap int, checkPeriod time.Duration) *KiteFileStore {

	datalink := make([]*list.List, 0, CONCURRENT_LEVEL)
	oplogs := make([]map[string]*list.Element, 0, CONCURRENT_LEVEL)
	locks := make([]*sync.RWMutex, 0, CONCURRENT_LEVEL)
	delChan := make([]chan *command, 0, CONCURRENT_LEVEL)
	for i := 0; i < CONCURRENT_LEVEL; i++ {
		splitMap := make(map[string] /*messageId*/ *list.Element, maxcap/CONCURRENT_LEVEL)
		locks = append(locks, &sync.RWMutex{})
		oplogs = append(oplogs, splitMap)
		datalink = append(datalink, list.New())
		ch := make(chan *command, 8000)
		delChan = append(delChan, ch)
	}

	kms := &KiteFileStore{
		datalink: datalink,
		oplogs:   oplogs,
		locks:    locks,
		maxcap:   maxcap / CONCURRENT_LEVEL,
		delChan:  delChan}

	kms.snapshot =
		NewMessageStore(dir+"/snapshot/", batchFlush, checkPeriod, func(ol *oplog) {
			kms.replay(ol)
		})
	return kms
}

//重放当前data的操作日志还原消息状态
func (self *KiteFileStore) replay(ol *oplog) {

	messageId := ol.LogicId
	l, link, tol, _ := self.hash(messageId)
	// log.Debug("KiteFileStore|replay|%s|%s", ob, ol.Op)
	//如果是更新或者创建，则直接反序列化
	if ol.Op == OP_U || ol.Op == OP_C {
		var body opBody
		err := json.Unmarshal(ol.Body, &body)
		if nil != err {
			log.ErrorLog("kite_store", "KiteFileStore|replay|FAIL|%s|%v", err, ol)
			return
		}

		ob := &body
		ob.Id = ol.ChunkId
		l.Lock()
		//直接覆盖
		e, ok := tol[messageId]
		if ok {
			e.Value = ob
		} else {
			e = link.PushFront(ob)
			tol[messageId] = e
		}
		link.MoveToFront(e)

		l.Unlock()

	} else if ol.Op == OP_D || ol.Op == OP_E {
		//如果为删除操作直接删除已经保存的op_log
		l.Lock()
		e, ok := tol[messageId]
		if ok {
			delete(tol, messageId)
			link.Remove(e)
		}
		l.Unlock()
	} else {
		log.ErrorLog("kite_store", "KiteFileStore|replay|INVALID|%s|%s", ol.Body, ol.Op)
	}

}

func (self *KiteFileStore) Start() {
	self.Lock()
	defer self.Unlock()
	if !self.running {
		self.Add(CONCURRENT_LEVEL)
		self.running = true
		//start snapshost
		self.snapshot.Start()
		for i, ch := range self.delChan {
			go self.delSync(fmt.Sprintf("%x", i), ch)
		}
		log.InfoLog("kite_store", "KiteFileStore|Start...\t%s", self.Monitor())
	}

}
func (self *KiteFileStore) Stop() {

	self.Lock()
	defer self.Unlock()
	if self.running {
		self.running = false
		//wait delete finish
		self.Wait()
		for _, ch := range self.delChan {
			close(ch)
		}

		self.snapshot.Destory()
		log.InfoLog("kite_store", "KiteFileStore|Stop...")
	}

}

func (self *KiteFileStore) RecoverNum() int {
	return CONCURRENT_LEVEL
}

//length
func (self *KiteFileStore) Length() map[string]int {
	defer func() {
		if err := recover(); nil != err {
			log.ErrorLog("kite_store", "KiteFileStore|Length|%s", err)
		}
	}()
	stat := make(map[string]int, 10)
	for i := 0; i < CONCURRENT_LEVEL; i++ {
		_, link, _, _ := self.hash(fmt.Sprintf("%x", i))
		for e := link.Back(); nil != e; e = e.Prev() {
			body := e.Value.(*opBody)
			v, ok := stat[body.Topic]
			if !ok {
				v = 0
			}
			stat[body.Topic] = (v + 1)
		}
	}
	return stat
}

func (self *KiteFileStore) Monitor() string {
	return fmt.Sprintf("\nmessage-length:%v\t writeChannel:%d \n", self.Length(), len(self.snapshot.writeChannel))
}

func (self *KiteFileStore) AsyncUpdate(entity *MessageEntity) bool { return self.UpdateEntity(entity) }
func (self *KiteFileStore) AsyncDelete(topic, messageId string) bool {
	return self.Delete(topic, messageId)
}
func (self *KiteFileStore) AsyncCommit(topic, messageId string) bool {
	return self.Commit(topic, messageId)
}

//hash get elelment
func (self *KiteFileStore) hash(messageid string) (l *sync.RWMutex, link *list.List, ol map[string]*list.Element, delCh chan *command) {
	id := string(messageid[len(messageid)-1])
	i, err := strconv.ParseInt(id, CONCURRENT_LEVEL, 8)
	hashId := int(i)
	if nil != err {
		log.ErrorLog("kite_store", "KiteFileStore|hash|INVALID MESSAGEID|%s\n", messageid)
		hashId = 0
	} else {
		hashId = hashId % CONCURRENT_LEVEL
	}

	// log.Debug("KiteFileStore|hash|%s|%d\n", messageid, hashId)

	//hash part
	l = self.locks[hashId]
	link = self.datalink[hashId]
	ol = self.oplogs[hashId]
	delCh = self.delChan[hashId]
	return
}

//waitSaveDone
func (self *KiteFileStore) waitSaveDone(ob *opBody) {
	if ob.Id < 0 {
		id, ok := <-ob.saveDone
		if ok {
			//is not closed
			if id < 0 {
				//save fail ,so  return nil and delete opBody
				//delete log
			} else {
				//save succ
				ob.Id = id
			}

		} else {
			//channel closed
		}
	} else {
		//save succ
	}
}

func (self *KiteFileStore) Query(topic, messageId string) *MessageEntity {

	lock, _, el, _ := self.hash(messageId)
	lock.RLock()
	defer lock.RUnlock()
	e, ok := el[messageId]
	if !ok {
		return nil
	}

	v := e.Value.(*opBody)

	//wait save done
	self.waitSaveDone(v)

	data, err := self.snapshot.Query(v.Id)
	if nil != err {
		log.Error("KiteFileStore|Query|Entity|FAIL|%s|%d", err, v.Id)
		return nil
	}

	var msg interface{}
	msgType := data[0]
	switch msgType {
	case protocol.CMD_BYTES_MESSAGE:
		var bms protocol.BytesMessage
		err = protocol.UnmarshalPbMessage(data[1:], &bms)
		msg = &bms
	case protocol.CMD_STRING_MESSAGE:
		var sms protocol.StringMessage
		err = protocol.UnmarshalPbMessage(data[1:], &sms)
		msg = &sms
	default:
		log.ErrorLog("kite_store", "KiteFileStore|Query|INVALID|MSGTYPE|%d", msgType)
		return nil
	}

	if nil != err {
		log.ErrorLog("kite_store", "KiteFileStore|Query|UnmarshalPbMessage|Entity|FAIL|%s", err)
		return nil
	} else {
		entity := NewMessageEntity(protocol.NewQMessage(msg))
		//merge data
		entity.Commit = v.Commit
		entity.FailGroups = v.FailGroups
		entity.SuccGroups = v.SuccGroups
		entity.NextDeliverTime = v.NextDeliverTime
		entity.DeliverCount = v.DeliverCount
		return entity
	}

}

func (self *KiteFileStore) Save(entity *MessageEntity) bool {

	lock, link, ol, _ := self.hash(entity.MessageId)
	if len(ol) >= self.maxcap {
		//overflow
		return false
	} else {

		//get lock
		lock.Lock()
		defer lock.Unlock()
		_, ok := ol[entity.MessageId]
		if ok {
			//duplicate messageId
			log.ErrorLog("kite_store", "KiteFileStore|Save|Duplicate MessageId|%s", entity.Header)
			return false
		}
		//value
		data := protocol.MarshalMessage(entity.Header, entity.MsgType, entity.GetBody())
		buff := make([]byte, len(data)+1)
		buff[0] = entity.MsgType
		copy(buff[1:], data)

		//create oplog
		ob := &opBody{
			Id:              -1,
			MessageId:       entity.MessageId,
			Topic:           entity.Header.GetTopic(),
			Commit:          entity.Commit,
			FailGroups:      entity.FailGroups,
			SuccGroups:      entity.SuccGroups,
			NextDeliverTime: entity.NextDeliverTime,
			DeliverCount:    0}

		obd, err := json.Marshal(ob)
		if nil != err {
			log.ErrorLog("kite_store", "KiteFileStore|Save|Encode|Op|FAIL|%s", err)
			return false
		}

		cmd := NewCommand(-1, entity.MessageId, buff, obd)

		//append oplog into file
		idchan := self.snapshot.Append(cmd)
		ob.saveDone = idchan

		//push
		e := link.PushBack(ob)
		ol[entity.MessageId] = e
		return true
	}

}
func (self *KiteFileStore) Commit(topic, messageId string) bool {

	cmd := func() *command {
		lock, _, ol, _ := self.hash(messageId)
		lock.Lock()
		defer lock.Unlock()
		e, ok := ol[messageId]
		if !ok {
			return nil
		}

		v := e.Value.(*opBody)
		//wait save succ
		self.waitSaveDone(v)
		if v.Id < 0 {
			return nil
		}
		//modify commit
		v.Commit = true
		obd, err := json.Marshal(v)
		if nil != err {
			log.ErrorLog("kite_store", "KiteFileStore|Commit|Encode|Op|FAIL|%s", err)
			return nil
		}
		//write oplog
		cmd := NewCommand(v.Id, messageId, nil, obd)
		return cmd
	}()

	if nil != cmd {
		//update log
		self.snapshot.Update(cmd)
	}
	return true

}
func (self *KiteFileStore) Rollback(topic, messageId string) bool {
	return self.Delete(topic, messageId)
}
func (self *KiteFileStore) UpdateEntity(entity *MessageEntity) bool {
	cmd := func() *command {
		lock, link, el, _ := self.hash(entity.MessageId)
		lock.Lock()
		defer lock.Unlock()
		e, ok := el[entity.MessageId]
		if !ok {
			return nil
		}

		//move to back
		link.MoveToBack(e)
		//modify opbody value
		v := e.Value.(*opBody)
		//wait save succ
		self.waitSaveDone(v)
		if v.Id < 0 {
			return nil
		}

		v.DeliverCount = entity.DeliverCount
		v.NextDeliverTime = entity.NextDeliverTime
		v.SuccGroups = entity.SuccGroups
		v.FailGroups = entity.FailGroups

		obd, err := json.Marshal(v)
		if nil != err {
			log.ErrorLog("kite_store", "KiteFileStore|UpdateEntity|Encode|Op|FAIL|%s", err)
			return nil
		}
		//append log
		cmd := NewCommand(v.Id, entity.MessageId, nil, obd)
		return cmd
	}()
	if nil != cmd {
		self.snapshot.Update(cmd)
	}
	return true
}
func (self *KiteFileStore) Delete(topic, messageId string) bool {

	v, ch := func() (*opBody, chan *command) {
		lock, _, el, ch := self.hash(messageId)
		lock.RLock()
		defer lock.RUnlock()
		e, ok := el[messageId]
		if !ok {
			return nil, nil
		}

		//delete
		v := e.Value.(*opBody)
		return v, ch
	}()

	if nil == v {
		return true
	}

	//wait save succ
	self.waitSaveDone(v)
	if v.Id < 0 {
		return true
	}

	obd, err := json.Marshal(v)
	if nil != err {
		log.ErrorLog("kite_store", "KiteFileStore|Delete|Encode|Op|FAIL|%s", err)
		return false
	}
	cmd := NewCommand(v.Id, messageId, nil, obd)
	if self.running {
		ch <- cmd
		return true
	} else {
		return false
	}
	return true
	// log.Info("KiteFileStore|innerDelete|%s\n", messageId)

}

func (self *KiteFileStore) delSync(hashKey string, ch chan *command) {

	delFunc := func(cmds []*command) {
		lock, link, el, _ := self.hash(hashKey)
		lock.Lock()
		defer lock.Unlock()
		for _, c := range cmds {
			e, ok := el[c.logicId]
			if !ok {
				continue
			}
			//delete log
			delete(el, c.logicId)
			link.Remove(e)
			self.snapshot.Delete(c)
		}
	}

	ticker := time.NewTicker(1 * time.Second)
	cmds := make([]*command, 0, 100)
	flush := false
	for self.running {
		select {
		//no batch / wait for data
		case c := <-ch:
			if nil != c {
				cmds = append(cmds, c)
				if len(cmds) >= 100 {
					flush = true
				}
			}
		case <-ticker.C:
			flush = (len(cmds) > 0)
		}

		//flush
		if flush {
			delFunc(cmds)
			go func() {
				for _, c := range cmds {
					self.snapshot.Delete(c)
				}
			}()
			cmds = cmds[:0]
		}
	}

	finished := false
	// need flush left data
	for !finished {
		select {
		case c := <-ch:
			if nil != c {
				cmds = append(cmds, c)
			}
		default:
			finished = true
		}
		//last flush
		if finished && len(cmds) > 0 {
			delFunc(cmds)
		}
	}
	self.Done()
}

//expired
func (self *KiteFileStore) Expired(topic, messageId string) bool {

	cmd := func() *command {
		lock, link, el, _ := self.hash(messageId)
		lock.Lock()
		defer lock.Unlock()
		e, ok := el[messageId]
		if !ok {
			return nil
		}

		//delete log
		delete(el, messageId)
		link.Remove(e)

		v := e.Value.(*opBody)
		//wait save succ
		self.waitSaveDone(v)
		if v.Id < 0 {
			return nil
		}
		c := NewCommand(v.Id, messageId, nil, nil)
		return c
	}()

	if nil != cmd {
		self.snapshot.Expired(cmd)
	}
	return true
}

func (self *KiteFileStore) MoveExpired() {
	//donothing
}

//根据kiteServer名称查询需要重投的消息 返回值为 是否还有更多、和本次返回的数据结果
func (self *KiteFileStore) PageQueryEntity(hashKey string, kiteServer string, nextDeliveryTime int64, startIdx, limit int) (bool, []*MessageEntity) {

	var pe []*MessageEntity
	var del [][]string
	lock, link, _, _ := self.hash(hashKey)
	lock.RLock()
	i := 0
	for e := link.Front(); nil != e; e = e.Next() {
		ob := e.Value.(*opBody)
		//wait save done
		self.waitSaveDone(ob)
		if ob.Id < 0 {
			if nil == del {
				del = make([][]string, 0, limit)
			}
			del = append(del, []string{ob.Topic, ob.MessageId})
			continue
		}

		//query message
		if ob.NextDeliverTime <= nextDeliveryTime {
			if startIdx <= i {
				if nil == pe {
					pe = make([]*MessageEntity, 0, limit+1)
				}
				//创建消息
				entity := &MessageEntity{
					MessageId: ob.MessageId}
				entity.Topic = ob.Topic
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

	//remove save failmessage
	for _, mid := range del {
		self.Delete(mid[0], mid[1])
	}

	hasMore := false
	if len(pe) > limit {
		pe = pe[:limit]
		hasMore = true
	}

	tmp := pe[:0]
	//query messageEntity
	for _, e := range pe {
		entity := self.Query(e.Topic, e.MessageId)
		if nil != entity {
			tmp = append(tmp, entity)
		} else {

		}
	}

	return hasMore, tmp

}
