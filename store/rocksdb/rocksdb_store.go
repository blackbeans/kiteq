package rocksdb

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/json"
	"kiteq/store"
	"strconv"
	"sync"
	"time"

	"github.com/blackbeans/kiteq-common/protocol"
	log "github.com/blackbeans/log4go"
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
)

//delvier tags
type opBody struct {
	Topic           string   `json:"topic"`
	MessageId       string   `json:"mid"`
	FailGroups      []string `json:"fg,omitempty"`
	SuccGroups      []string `json:"sg,omitempty"`
	NextDeliverTime int64    `json:"ndt"`
	DeliverCount    int32    `json:"dc"`
}

//recoverItem
type recoverItem struct {
	index           int
	Topic           string `json:"topic"`
	MessageId       string `json:"mid"`
	TTL             int64  `json:"ttl"` //消息的生命周期
	NextDeliverTime int64  `json:"ndt"`
	DeliverCount    int32  `json:"dc"`
}

//recover堆
type recoverHeap []*recoverItem

func (h recoverHeap) Len() int { return len(h) }

func (h recoverHeap) Less(i, j int) bool {
	if h[i].NextDeliverTime < h[j].NextDeliverTime {
		return true
	} else if h[i].NextDeliverTime > h[j].NextDeliverTime {
		return false
	} else {
		if h[i].DeliverCount < h[j].DeliverCount {
			return true
		} else if h[i].DeliverCount > h[j].DeliverCount {
			return false
		}
		return true
	}
}

func (h recoverHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *recoverHeap) Push(x interface{}) {
	item := x.(*recoverItem)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *recoverHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	item.index = -1
	*h = old[0 : n-1]
	return item
}

type RocksDbStore struct {
	store.IKiteStore
	recoverNum uint32
	ctx        context.Context
	rocksDbDir string
	options    map[string]string
	rocksdb    *pebble.DB
	rockDLQ    *pebble.DB

	//opBodies的排序
	recoverMap  *sync.Map
	recoverHeap recoverHeap

	//添加和更新recoverItem
	addChan             chan *recoverItem
	upChan              chan *recoverItem
	delChan             chan *recoverItem
	pageQueryChan       chan pageQuery
	pageQueryResponseCh chan *pageQueryResponse
}

func NewRocksDbStore(ctx context.Context, rocksDbDir string, options map[string]string) *RocksDbStore {
	return &RocksDbStore{
		ctx:                 ctx,
		options:             options,
		rocksDbDir:          rocksDbDir,
		recoverMap:          &sync.Map{},
		recoverHeap:         make([]*recoverItem, 0, 10*10000),
		addChan:             make(chan *recoverItem, 1000),
		upChan:              make(chan *recoverItem, 1000),
		delChan:             make(chan *recoverItem, 1000),
		pageQueryChan:       make(chan pageQuery, 1),
		pageQueryResponseCh: make(chan *pageQueryResponse, 1),
	}
}

func msgKeyForHeader(topic, messageid string) string {
	return "mh:" + topic + ":" + messageid
}

func msgKeyForBody(topic, messageid string) string {
	return "mb:" + topic + ":" + messageid
}

func opLogKey(topic, messageid string) string {
	return "o:" + topic + ":" + messageid
}

func (self *RocksDbStore) Start() {

	go self.heapProcess()

	rocksdb, err := pebble.Open(self.rocksDbDir+"/data/", &pebble.Options{
		MaxConcurrentCompactions: 5,
	})
	if nil != err {
		panic(err)
	}
	self.rocksdb = rocksdb

	rockDLQ, err := pebble.Open(self.rocksDbDir+"/dlq/", &pebble.Options{
		MaxConcurrentCompactions: 5,
	})
	if nil != err {
		panic(err)
	}
	self.rockDLQ = rockDLQ

	keyUpperBound := func(b []byte) []byte {
		end := make([]byte, len(b))
		copy(end, b)
		for i := len(end) - 1; i >= 0; i-- {
			end[i] = end[i] + 1
			if end[i] != 0 {
				return end[:i+1]
			}
		}
		return nil // no upper-bound
	}

	prefixIterOptions := func(prefix []byte) *pebble.IterOptions {
		return &pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: keyUpperBound(prefix),
		}
	}

	//开始遍历下所有的数据，按照下一次投递时间排序

	iter := rocksdb.NewSnapshot().NewIter(prefixIterOptions([]byte("op:")))
	for iter.First(); iter.Valid(); iter.Next() {
		var item recoverItem
		err := json.Unmarshal(iter.Value(), &item)
		if nil != err {
			continue
		}
		self.addChan <- &item
	}
	//关闭遍历
	iter.Close()

	//异步再去查看header没有提交的消息
	go func() {
		iter := rocksdb.NewSnapshot().NewIter(prefixIterOptions([]byte("mh:")))
		for iter.First(); iter.Valid(); iter.Next() {
			var header protocol.Header
			err := protocol.UnmarshalPbMessage(iter.Value()[1:], &header)
			if nil != err {
				log.ErrorLog("kite_store", "KiteFileStore|Start.ReloadUnCommit|%v", err)
				continue
			}

			item := &recoverItem{
				index:     -1,
				Topic:     header.GetTopic(),
				MessageId: header.GetMessageId(),
			}
			if _, loaded := self.recoverMap.Load(msgKeyForHeader(header.GetTopic(), header.GetMessageId())); !loaded {
				self.addChan <- item
			}

		}
		//关闭遍历
		iter.Close()
	}()

}

func (self *RocksDbStore) heapProcess() {
	for {
		select {
		case <-self.ctx.Done():
			return
		case item := <-self.addChan:
			if _, loaded := self.recoverMap.LoadOrStore(msgKeyForHeader(item.Topic, item.MessageId), item); !loaded {
				heap.Push(&self.recoverHeap, item)
				self.recoverNum++
			}
		case update := <-self.upChan:
			if actual, loaded := self.recoverMap.LoadOrStore(msgKeyForHeader(update.Topic, update.MessageId), update); loaded {
				actual.(*recoverItem).NextDeliverTime = update.NextDeliverTime
				actual.(*recoverItem).DeliverCount = update.DeliverCount
				//更新数据
				heap.Fix(&self.recoverHeap, update.index)
			} else {
				//没有则写入堆数据中
				heap.Push(&self.recoverHeap, update)
				self.recoverNum++
			}
		case del := <-self.delChan:
			if _, loaded := self.recoverMap.LoadAndDelete(msgKeyForHeader(del.Topic, del.MessageId)); loaded {
				//更新数据
				heap.Remove(&self.recoverHeap, del.index)
				self.recoverNum--
			}
		case pageQuery := <-self.pageQueryChan:
			//recover items
			recoverItems := make([]*recoverItem, 0, 10)
			for self.recoverHeap.Len() > 0 {
				if self.recoverHeap[0].NextDeliverTime <= pageQuery.nextDeliveryTime {
					min := heap.Pop(&self.recoverHeap)
					if _, loaded := self.recoverMap.LoadAndDelete(msgKeyForHeader(min.(*recoverItem).Topic, min.(*recoverItem).MessageId)); loaded {
						self.recoverNum--
						if min.(*recoverItem).NextDeliverTime <= pageQuery.nextDeliveryTime && len(recoverItems) < pageQuery.limit {
							recoverItems = append(recoverItems, min.(*recoverItem))
						} else {
							//没有比这个更小的了
							break
						}
					}
				} else {
					break
				}
			}

			hasMore := true
			if len(recoverItems) < pageQuery.limit {
				hasMore = false
			}

			//分页查询结果
			self.pageQueryResponseCh <- &pageQueryResponse{
				hasMore: hasMore,
				items:   recoverItems,
			}
		}
	}
}

func (self *RocksDbStore) Stop() {
	self.rocksdb.Close()
	self.rockDLQ.Close()
}

func (self *RocksDbStore) Monitor() string {
	return "RecoverNum:" + strconv.FormatUint(uint64(self.recoverNum), 10)
}

func (self *RocksDbStore) Length() map[string]int {
	return map[string]int{}
}

func (self *RocksDbStore) MoveExpired() {
	//通知堆开始操作dlq的消息,遍历下目前所有的处于recover状态的数据
	//超过最大ttl和投递次数的消息都搬迁到DLQ的rocksdb中
	now := time.Now().Unix() / int64(time.Millisecond)
	self.recoverMap.Range(func(key, value interface{}) bool {
		item := value.(*recoverItem)
		if item.DeliverCount >= 100 || item.TTL <= now {
			self.Expired(item.Topic, item.MessageId)
		}
		return true
	})
}

func (self *RocksDbStore) RecoverNum() int {
	return 1
}

func (self *RocksDbStore) AsyncUpdateDeliverResult(entity *store.MessageEntity) bool {
	opLog := opBody{
		Topic:           entity.Topic,
		MessageId:       entity.MessageId,
		FailGroups:      entity.FailGroups,
		SuccGroups:      entity.SuccGroups,
		NextDeliverTime: entity.NextDeliverTime,
		DeliverCount:    entity.DeliverCount,
	}
	rawOpLog, err := json.Marshal(opLog)
	if nil != err {
		log.ErrorLog("kite_store", "KiteFileStore|AsyncUpdateDeliverResult|MarshalFAIL|%v", err)
		return false
	}

	err = self.rocksdb.Set([]byte(opLogKey(entity.Topic, entity.MessageId)), rawOpLog, pebble.Sync)
	if nil != err {
		log.ErrorLog("kite_store", "KiteFileStore|AsyncUpdateDeliverResult|Set|FAIL|%v", err)
		return false
	}

	item := &recoverItem{
		index:           -1,
		Topic:           entity.Topic,
		MessageId:       entity.MessageId,
		NextDeliverTime: entity.NextDeliverTime,
		DeliverCount:    entity.DeliverCount,
	}

	self.upChan <- item
	return true
}

func (self *RocksDbStore) AsyncDelete(topic, messageId string) bool {
	return self.Delete(topic, messageId)
}

func (self *RocksDbStore) AsyncCommit(topic, messageId string) bool {
	return self.Commit(topic, messageId)
}

func (self *RocksDbStore) Query(topic, messageId string) *store.MessageEntity {
	batch := self.rocksdb.NewIndexedBatch()
	key := []byte(msgKeyForHeader(topic, messageId))
	data, r, err := batch.Get(key)
	if nil != err {
		log.ErrorLog("kite_store", "KiteFileStore|Query|FAIL|%v", err, string(key))
		batch.Close()
		return nil
	}

	var header protocol.Header
	err = protocol.UnmarshalPbMessage(data[1:], &header)
	if nil != err {
		batch.Close()
		log.ErrorLog("kite_store", "KiteFileStore|Query.UnmarshalPbMessage|FAIL|%v", err, string(key))
		return nil
	}
	r.Close()
	var entity *store.MessageEntity
	//构建body
	switch data[0] {
	case protocol.CMD_BYTES_MESSAGE:
		body, r, err := batch.Get([]byte(msgKeyForBody(topic, messageId)))
		if nil != err {
			batch.Close()
			log.ErrorLog("kite_store", "KiteFileStore|Query|FAIL|%v", err, string(key))
			return nil
		}
		r.Close()
		entity = store.NewMessageEntity(protocol.NewQMessage(&protocol.BytesMessage{
			Header: &header,
			Body:   body,
		}))
	case protocol.CMD_STRING_MESSAGE:

		body, r, err := batch.Get([]byte(msgKeyForBody(topic, messageId)))
		if nil != err {
			batch.Close()
			log.ErrorLog("kite_store", "KiteFileStore|Query|FAIL|%v", err, string(key))
			return nil
		}
		r.Close()
		entity = store.NewMessageEntity(protocol.NewQMessage(&protocol.StringMessage{
			Header: &header,
			Body:   proto.String(string(body)),
		}))
	default:
		log.ErrorLog("kite_store", "KiteFileStore|Query|INVALID|MSGTYPE|%d", data[0])
		batch.Close()
		return nil
	}

	//查询投递操作日志
	data, r, err = batch.Get([]byte(opLogKey(topic, messageId)))
	if nil != err {
		batch.Close()
		if err == pebble.ErrNotFound {
			return entity
		} else {
			return entity
		}
	}
	var opLog opBody
	err = json.Unmarshal(data, &opLog)
	if nil != err {
		if err != pebble.ErrNotFound {
			log.ErrorLog("kite_store", "KiteFileStore|QueryLog|FAIL|%v", err)
		}
		r.Close()
		batch.Close()
		return entity
	}
	r.Close()
	//merge data
	if nil != entity {
		entity.FailGroups = opLog.FailGroups
		entity.SuccGroups = opLog.SuccGroups
		entity.NextDeliverTime = opLog.NextDeliverTime
		entity.DeliverCount = opLog.DeliverCount
	}
	batch.Close()
	return entity

}

func (self *RocksDbStore) Save(entity *store.MessageEntity) bool {
	return self.save0(self.rocksdb, entity)
}

var slicePool = &sync.Pool{New: func() interface{} {
	return make([]byte, 0, 2*1024)
}}

//存储数据
func (self *RocksDbStore) save0(rocksdb *pebble.DB, entity *store.MessageEntity) bool {
	batch := rocksdb.NewBatch()
	rawHeader, _ := protocol.MarshalPbMessage(entity.Header)

	arr := slicePool.Get().([]byte)

	buff := bytes.NewBuffer(arr)
	buff.WriteByte(entity.MsgType)
	buff.Write(rawHeader)
	//header
	err := batch.Set([]byte(msgKeyForHeader(entity.Topic, entity.MessageId)), buff.Bytes(), &pebble.WriteOptions{Sync: true})
	if nil != err {
		slicePool.Put(arr[:0])
		batch.Close()
		return false
	}
	slicePool.Put(arr[:0])
	switch entity.MsgType {
	case protocol.CMD_BYTES_MESSAGE:
		//body
		err = batch.Set([]byte(msgKeyForBody(entity.Topic, entity.MessageId)), entity.GetBody().([]byte), pebble.Sync)
		if nil != err {
			batch.Close()
			return false
		}
	case protocol.CMD_STRING_MESSAGE:
		//body
		err = batch.Set([]byte(msgKeyForBody(entity.Topic, entity.MessageId)), []byte(entity.GetBody().(string)), pebble.Sync)
		if nil != err {
			batch.Close()
			return false
		}

	}
	batch.Commit(&pebble.WriteOptions{Sync: true})
	//提交
	return true
}

func (self *RocksDbStore) Commit(topic, messageId string) bool {
	//存储消息已提交
	rawHeader, r, err := self.rocksdb.Get([]byte(msgKeyForHeader(topic, messageId)))
	if nil != err {
		r.Close()
		return false
	}

	var header protocol.Header
	err = protocol.UnmarshalPbMessage(rawHeader[1:], &header)
	if nil != err {
		r.Close()
		log.ErrorLog("kite_store", "KiteFileStore|Commit.UnmarshalPbMessage|FAIL|%v|%s|%s", err, topic, messageId)
		return false
	}
	r.Close()
	header.Commit = proto.Bool(true)
	newRawHeader, err := protocol.MarshalPbMessage(&header)
	if nil != err {
		log.ErrorLog("kite_store", "KiteFileStore|Commit.MarshalPbMessage|FAIL|%v|%s|%s", err, topic, messageId)
		return false
	}
	rawHeader = append(append(rawHeader[:0], rawHeader[0]), newRawHeader...)
	//body
	err = self.rocksdb.Set([]byte(msgKeyForHeader(topic, messageId)), rawHeader, pebble.NoSync)
	if nil != err {
		return false
	}

	return true
}

func (self *RocksDbStore) Rollback(topic, messageId string) bool {
	return self.Delete(topic, messageId)
}

func (self *RocksDbStore) Delete(topic, messageId string) bool {

	//清理掉需要recover的数据
	if _, loaded := self.recoverMap.Load(msgKeyForHeader(topic, messageId)); loaded {
		self.delChan <- &recoverItem{Topic: topic, MessageId: messageId}
	}

	batch := self.rocksdb.NewBatch()
	batch.Delete([]byte(msgKeyForHeader(topic, messageId)), pebble.NoSync)
	batch.Delete([]byte(msgKeyForBody(topic, messageId)), pebble.NoSync)
	batch.Delete([]byte(opLogKey(topic, messageId)), pebble.NoSync)
	batch.Commit(pebble.NoSync)

	return true
}

//过期消息进行清理
func (self *RocksDbStore) Expired(topic, messageId string) bool {

	//获取过期的数据，并清理掉
	entity := self.Query(topic, messageId)
	if nil != entity {
		//将数据搬迁到DLQ中
		if succ := self.save0(self.rockDLQ, entity); !succ {
			return false
		}
		//查询投递操作日志
		logKey := opLogKey(topic, messageId)
		data, r, err := self.rocksdb.Get([]byte(logKey))
		if nil == err {
			err = self.rockDLQ.Set([]byte(logKey), data, pebble.NoSync)
			if nil != err {
				log.ErrorLog("kite_store", "KiteFileStore|Expired.Set|FAIL|%v|%s", err, string(logKey))
			}
			r.Close()
		}
		//清理掉正常的队列中的数据
		self.Delete(topic, messageId)
	}

	return true
}

type pageQuery struct {
	nextDeliveryTime int64
	limit            int
}

type pageQueryResponse struct {
	hasMore bool
	items   []*recoverItem
}

func (self *RocksDbStore) PageQueryEntity(hashKey string, kiteServer string, nextDeliverySeconds int64, startIdx, limit int) (bool, []*store.MessageEntity) {

	//recover items
	self.pageQueryChan <- pageQuery{
		nextDeliveryTime: nextDeliverySeconds,
		limit:            limit,
	}

	//获取查询之后的响应
	response := <-self.pageQueryResponseCh

	//获取对应的实体
	entities := make([]*store.MessageEntity, 0, len(response.items))
	batch := self.rocksdb.NewIndexedBatch()
	for _, item := range response.items {

		key := msgKeyForHeader(item.Topic, item.MessageId)
		rawHeader, hr, err := batch.Get([]byte(key))
		if nil != err {
			continue
		}
		var header protocol.Header
		err = protocol.UnmarshalPbMessage(rawHeader[1:], &header)
		if nil != err {
			hr.Close()
			log.ErrorLog("kite_store", "KiteFileStore|PageQueryEntity.UnmarshalPbMessage|FAIL|%v", err, string(key))
			continue
		}
		hr.Close()

		//创建消息
		entity := &store.MessageEntity{
			Header:    &header,
			MessageId: header.GetMessageId(),
			Topic:     header.GetTopic(),
			Commit:    header.GetCommit()}

		//查询投递操作日志
		data, r, err := batch.Get([]byte(opLogKey(item.Topic, item.MessageId)))
		if nil != err {
			if err == pebble.ErrNotFound {
				continue
			} else {
				continue
			}
		}

		var opLog opBody
		err = json.Unmarshal(data, &opLog)
		if nil != err {
			if err != pebble.ErrNotFound {
				log.ErrorLog("kite_store", "KiteFileStore|QueryLog|FAIL|%v", err)
			}
			r.Close()
		} else {
			r.Close()
		}

		//merge data
		entity.FailGroups = opLog.FailGroups
		entity.SuccGroups = opLog.SuccGroups
		entity.NextDeliverTime = opLog.NextDeliverTime
		entity.DeliverCount = opLog.DeliverCount
		entities = append(entities, entity)
	}
	batch.Close()
	return response.hasMore, entities
}
