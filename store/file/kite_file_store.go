package file

import (
	"container/list"
	"encoding/json"
	"fmt"
	log "github.com/blackbeans/log4go"
	"kiteq/protocol"
	. "kiteq/store"
	"strconv"
	"sync"
	"time"
)

//delvier tags
type opBody struct {
	Id              int64      `json:"id"`
	MessageId       string     `json:"mid"`
	Commit          bool       `json:"commit"`
	FailGroups      []string   `json:"fg",omitempty`
	SuccGroups      []string   `json:"sg",omitempty`
	NextDeliverTime int64      `json:"ndt"`
	DeliverCount    int32      `json:"dc"`
	saveDone        chan int64 //save done
	sync.WaitGroup             //wait set Id finish
}

const (
	CONCURRENT_LEVEL = 16
)

type KiteFileStore struct {
	oplogs     []map[string] /*messageId*/ *list.Element //用于oplog的replay
	loglink    []*list.List                              //*opBody
	locks      []*sync.RWMutex
	maxcap     int
	currentSid int64 // 当前segment的id
	snapshot   *MessageStore
	delChan    chan *command
	sync.RWMutex
	sync.WaitGroup
	running bool
}

func NewKiteFileStore(dir string, maxcap int, checkPeriod time.Duration) *KiteFileStore {

	loglink := make([]*list.List, 0, CONCURRENT_LEVEL)
	oplogs := make([]map[string]*list.Element, 0, CONCURRENT_LEVEL)
	locks := make([]*sync.RWMutex, 0, CONCURRENT_LEVEL)
	for i := 0; i < CONCURRENT_LEVEL; i++ {
		splitMap := make(map[string] /*messageId*/ *list.Element, maxcap/CONCURRENT_LEVEL)
		locks = append(locks, &sync.RWMutex{})
		oplogs = append(oplogs, splitMap)
		loglink = append(loglink, list.New())
	}

	kms := &KiteFileStore{
		loglink: loglink,
		oplogs:  oplogs,
		locks:   locks,
		maxcap:  maxcap / CONCURRENT_LEVEL,
		delChan: make(chan *command, 1000)}

	kms.snapshot =
		NewMessageStore(dir+"/snapshot/", 300, 2, checkPeriod, func(ol *oplog) {
			kms.replay(ol)
		})
	return kms
}

//重放当前data的操作日志还原消息状态
func (self *KiteFileStore) replay(ol *oplog) {

	var body opBody

	err := json.Unmarshal(ol.Body, &body)
	if nil != err {
		log.ErrorLog("kite_store", "KiteFileStore|replay|FAIL|%s|%s", err, ol.Body)
		return
	}

	ob := &body
	ob.Id = ol.ChunkId

	l, link, tol := self.hash(ob.MessageId)
	// log.Debug("KiteFileStore|replay|%s|%s", ob, ol.Op)
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

	} else if ol.Op == OP_D || ol.Op == OP_E {
		//如果为删除操作直接删除已经保存的op_log
		l.Lock()
		e, ok := tol[ob.MessageId]
		if ok {
			delete(tol, ob.MessageId)
			link.Remove(e)
		}
		l.Unlock()
	} else {
		log.ErrorLog("kite_store", "KiteFileStore|replay|INVALID|%s|%s", ob, ol.Op)
	}

}

func (self *KiteFileStore) Start() {
	self.Lock()
	defer self.Unlock()
	if !self.running {
		self.Add(1)
		self.running = true
		//start snapshost
		self.snapshot.Start()
		go self.delSync()
		log.InfoLog("kite_store", "KiteFileStore|Start...\t%s", self.Monitor())
	}

}
func (self *KiteFileStore) Stop() {

	self.Lock()
	defer self.Unlock()
	if self.running {
		self.running = false
		close(self.delChan)
		//wait delete finish
		self.Wait()
		self.snapshot.Destory()
		log.InfoLog("kite_store", "KiteFileStore|Stop...")
	}

}

func (self *KiteFileStore) RecoverNum() int {
	return CONCURRENT_LEVEL
}

//length
func (self *KiteFileStore) Length() int {
	l := 0
	for i := 0; i < CONCURRENT_LEVEL; i++ {
		_, link, _ := self.hash(fmt.Sprintf("%x", i))
		l += link.Len()
	}
	return l
}

func (self *KiteFileStore) Monitor() string {
	return fmt.Sprintf("message-length:%d\n", self.Length())
}

func (self *KiteFileStore) AsyncUpdate(entity *MessageEntity) bool { return self.UpdateEntity(entity) }
func (self *KiteFileStore) AsyncDelete(messageId string) bool      { return self.Delete(messageId) }
func (self *KiteFileStore) AsyncCommit(messageId string) bool      { return self.Commit(messageId) }

//hash get elelment
func (self *KiteFileStore) hash(messageid string) (l *sync.RWMutex, link *list.List, ol map[string]*list.Element) {
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
	ol = self.oplogs[hashId]
	link = self.loglink[hashId]
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
			ob.Done()
		} else {
			//channel closed
		}
	} else {
		//save succ
	}
	//wait
	ob.Wait()
}

func (self *KiteFileStore) Query(messageId string) *MessageEntity {

	lock, _, el := self.hash(messageId)
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
		// log.Error("KiteFileStore|Query|Entity|FAIL|%s|%d", err, v.Id)
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

	lock, link, ol := self.hash(entity.MessageId)
	if len(ol) >= self.maxcap {
		//overflow
		return false
	} else {

		//value
		data := protocol.MarshalMessage(entity.Header, entity.MsgType, entity.GetBody())
		buff := make([]byte, len(data)+1)
		buff[0] = entity.MsgType
		copy(buff[1:], data)

		//create oplog
		ob := &opBody{
			Id:              -1,
			MessageId:       entity.MessageId,
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
		ob.Add(1)

		//get lock
		lock.Lock()
		//push
		e := link.PushBack(ob)
		ol[entity.MessageId] = e
		lock.Unlock()
		return true
	}

}
func (self *KiteFileStore) Commit(messageId string) bool {

	cmd := func() *command {
		lock, _, ol := self.hash(messageId)
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
func (self *KiteFileStore) Rollback(messageId string) bool {
	return self.Delete(messageId)
}
func (self *KiteFileStore) UpdateEntity(entity *MessageEntity) bool {
	cmd := func() *command {
		lock, link, el := self.hash(entity.MessageId)
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
func (self *KiteFileStore) Delete(messageId string) bool {

	lock, link, el := self.hash(messageId)
	lock.Lock()
	defer lock.Unlock()
	e, ok := el[messageId]
	if !ok {
		return true
	}

	//delete log
	delete(el, messageId)
	link.Remove(e)

	//delete
	v := e.Value.(*opBody)

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
		self.delChan <- cmd
		return true
	} else {
		return false
	}
	// log.Info("KiteFileStore|innerDelete|%s\n", messageId)

}

func (self *KiteFileStore) delSync() {

	for self.running {

		//no batch / wait for data
		c := <-self.delChan
		if nil != c {
			self.snapshot.Delete(c)
		}

	}

	// need flush left data
outter:
	for {
		select {
		case c := <-self.delChan:
			if nil != c {
				self.snapshot.Delete(c)
			} else {
				//channel close
				break outter
			}

		default:
			break outter
		}
	}
	self.Done()
}

//expired
func (self *KiteFileStore) Expired(messageId string) bool {

	cmd := func() *command {
		lock, link, el := self.hash(messageId)
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

//根据kiteServer名称查询需要重投的消息 返回值为 是否还有更多、和本次返回的数据结果
func (self *KiteFileStore) PageQueryEntity(hashKey string, kiteServer string, nextDeliveryTime int64, startIdx, limit int) (bool, []*MessageEntity) {

	var pe []*MessageEntity
	var del []string
	lock, link, _ := self.hash(hashKey)
	lock.RLock()
	i := 0
	for e := link.Front(); nil != e; e = e.Next() {
		ob := e.Value.(*opBody)
		//wait save done
		self.waitSaveDone(ob)

		if ob.Id < 0 {
			if nil == del {
				del = make([]string, 0, limit)
			}
			del = append(del, ob.MessageId)
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
		self.Delete(mid)
	}

	if len(pe) > limit {
		return true, pe[:limit]
	} else {
		return false, pe
	}
	return false, nil

}
