package rocksdb

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"kiteq/store"
	"strconv"
	"sync"
	"time"

	"github.com/blackbeans/kiteq-common/protocol"
	log "github.com/blackbeans/log4go"
	"github.com/cockroachdb/pebble"
)

//delvier tags
type opBody struct {
	MessageId       string   `json:"mid"`
	Topic           string   `json:"topic"`
	Commit          bool     `json:"commit"`
	FailGroups      []string `json:"fg,omitempty"`
	SuccGroups      []string `json:"sg,omitempty"`
	NextDeliverTime int64    `json:"ndt"`
	DeliverCount    int32    `json:"dc"`
}

//recoverItem
type recoverItem struct {
	index           int
	MessageId       string `json:"mid"`
	Topic           string `json:"topic"`
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

func msgKey(topic, messageid string) string {
	return fmt.Sprintf("%s:%s", topic, messageid)
}

func opLogKey(topic, messageid string) string {
	return fmt.Sprintf("oplog:%s:%s", topic, messageid)
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
	iter := rocksdb.NewSnapshot().NewIter(prefixIterOptions([]byte("oplog:")))
	for iter.First(); iter.Valid(); iter.Next() {
		var item recoverItem
		err := json.Unmarshal(iter.Value(), &item)
		if nil != err {
			continue
		}
		if _, loaded := self.recoverMap.LoadOrStore(item.MessageId, &item); !loaded {
			self.addChan <- &item
		}
	}
	//关闭遍历
	iter.Close()

}

func (self *RocksDbStore) heapProcess() {
	lastCheck := time.Now()
	for {
		select {
		case <-self.ctx.Done():
			return
		case item := <-self.addChan:
			heap.Push(&self.recoverHeap, item)
		case update := <-self.upChan:
			//更新数据
			heap.Fix(&self.recoverHeap, update.index)
		case del := <-self.delChan:
			heap.Remove(&self.recoverHeap, del.index)
		case pageQuery := <-self.pageQueryChan:
			//recover items
			recoverItems := make([]*recoverItem, 0, 10)
			for self.recoverHeap.Len() > 0 {
				if self.recoverHeap[0].NextDeliverTime <= pageQuery.nextDeliveryTime {
					min := heap.Pop(&self.recoverHeap)
					if min.(*recoverItem).NextDeliverTime <= pageQuery.nextDeliveryTime && len(recoverItems) < pageQuery.limit {
						recoverItems = append(recoverItems, min.(*recoverItem))
					} else {
						//没有比这个更小的了
						break
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
		//超过5分钟那么就需要计算下recover的数量
		if time.Now().Sub(lastCheck) > 5*time.Second {
			lastCheck = time.Now()
			self.recoverNum = uint32(len(self.recoverHeap))
		}
	}
}

func (self *RocksDbStore) Stop() {
	self.rocksdb.Close()
	self.rockDLQ.Close()
}

func (self *RocksDbStore) Monitor() string {
	return "Recover:" + strconv.FormatUint(uint64(self.recoverNum), 10)
}

func (self *RocksDbStore) Length() map[string]int {
	return map[string]int{}
}

func (self *RocksDbStore) MoveExpired() {
	//通知堆开始操作dlq的消息,遍历下目前所有的处于recover状态的数据
	//超过最大ttl和投递次数的消息都搬迁到DLQ的rocksdb中
	now := time.Now().UnixNano() / int64(time.Millisecond)
	self.recoverMap.Range(func(key, value interface{}) bool {
		item := value.(*recoverItem)
		if item.DeliverCount >= 100 || item.TTL <= now {
			key := []byte(msgKey(item.Topic, item.MessageId))
			data, r, err := self.rocksdb.Get(key)
			if nil != err {
				log.ErrorLog("kite_store", "KiteFileStore|MoveExpired.Get|FAIL|%v|%s", err, string(key))
			} else {
				//将数据搬迁到DLQ中
				err = self.rockDLQ.Set(key, data, &pebble.WriteOptions{Sync: true})
				if nil != err {
					log.ErrorLog("kite_store", "KiteFileStore|MoveExpired.DataKey.Set|FAIL|%v|%s", err, string(key))
				}
				r.Close()
			}

			//查询投递操作日志
			logKey := opLogKey(item.Topic, item.MessageId)
			data, r, err = self.rocksdb.Get([]byte(logKey))
			if nil == err {
				err = self.rockDLQ.Set([]byte(logKey), data, &pebble.WriteOptions{Sync: true})
				if nil != err {
					log.ErrorLog("kite_store", "KiteFileStore|MoveExpired.LogKey.Set|FAIL|%v|%s", err, string(key))
				}
				r.Close()
			}

			//清理掉正常的队列中的数据
			self.Delete(item.Topic, item.MessageId)
		}
		return true
	})
}

func (self *RocksDbStore) RecoverNum() int {
	return 0
}

func (self *RocksDbStore) AsyncUpdate(entity *store.MessageEntity) bool {
	opLog := opBody{
		MessageId:       entity.MessageId,
		Topic:           entity.Topic,
		Commit:          entity.Commit,
		FailGroups:      entity.FailGroups,
		SuccGroups:      entity.SuccGroups,
		NextDeliverTime: entity.NextDeliverTime,
		DeliverCount:    entity.DeliverCount,
	}
	rawOpLog, err := json.Marshal(opLog)
	if nil != err {
		log.ErrorLog("kite_store", "KiteFileStore|AsyncUpdate|MarshalFAIL|%v", err)
		return false
	}

	err = self.rocksdb.Set([]byte(opLogKey(entity.Topic, entity.MessageId)), rawOpLog, pebble.Sync)
	if nil != err {
		log.ErrorLog("kite_store", "KiteFileStore|AsyncUpdate|Set|FAIL|%v", err)
		return false
	}

	item := &recoverItem{
		MessageId:       entity.MessageId,
		Topic:           entity.Topic,
		NextDeliverTime: entity.NextDeliverTime,
		DeliverCount:    entity.DeliverCount,
	}

	//更新对应的数据recover数据
	if actual, loaded := self.recoverMap.LoadOrStore(entity.MessageId, &item); loaded {
		actual.(*recoverItem).NextDeliverTime = entity.NextDeliverTime
		actual.(*recoverItem).DeliverCount = entity.DeliverCount
		//推送更新
		self.upChan <- actual.(*recoverItem)
	} else {
		//之前不存在那么久新增一个
		self.addChan <- item
	}

	return true
}

func (self *RocksDbStore) AsyncDelete(topic, messageId string) bool {
	return self.Delete(topic, messageId)
}

func (self *RocksDbStore) AsyncCommit(topic, messageId string) bool {
	return self.Delete(topic, messageId)
}

func (self *RocksDbStore) Query(topic, messageId string) *store.MessageEntity {
	key := []byte(msgKey(topic, messageId))
	data, r, err := self.rocksdb.Get(key)
	if nil != err {
		log.ErrorLog("kite_store", "KiteFileStore|QueryMsg|FAIL|%v", err, string(key))
		return nil
	}

	var entity *store.MessageEntity
	switch data[0] {
	case protocol.CMD_BYTES_MESSAGE:
		var bms protocol.BytesMessage
		if err = protocol.UnmarshalPbMessage(data[1:], &bms); nil == err {
			entity = store.NewMessageEntity(protocol.NewQMessage(&bms))
		}
		r.Close()
	case protocol.CMD_STRING_MESSAGE:
		var sms protocol.StringMessage
		if err = protocol.UnmarshalPbMessage(data[1:], &sms); nil == err {
			entity = store.NewMessageEntity(protocol.NewQMessage(&sms))
		}
		r.Close()
	default:
		log.ErrorLog("kite_store", "KiteFileStore|Query|INVALID|MSGTYPE|%d", data[0])
		return nil
	}

	//查询投递操作日志
	data, r, err = self.rocksdb.Get([]byte(opLogKey(topic, messageId)))
	if nil != err {
		if err == pebble.ErrNotFound {
			return entity
		} else {
			return entity
		}
	}
	defer r.Close()
	var opLog opBody
	err = json.Unmarshal(data, &opLog)
	if nil != err {
		if err != pebble.ErrNotFound {
			log.ErrorLog("kite_store", "KiteFileStore|QueryLog|FAIL|%v", err)
		}
		return entity
	}

	//merge data
	if nil != entity {
		entity.Commit = opLog.Commit
		entity.FailGroups = opLog.FailGroups
		entity.SuccGroups = opLog.SuccGroups
		entity.NextDeliverTime = opLog.NextDeliverTime
		entity.DeliverCount = opLog.DeliverCount
	}
	return entity

}

func (self *RocksDbStore) Save(entity *store.MessageEntity) bool {
	data := protocol.MarshalMessage(entity.Header, entity.MsgType, entity.GetBody())
	buff := make([]byte, len(data)+1)
	buff[0] = entity.MsgType
	copy(buff[1:], data)
	err := self.rocksdb.Set([]byte(msgKey(entity.Topic, entity.MessageId)), buff, &pebble.WriteOptions{Sync: true})
	if nil != err {
		return false
	}
	return true
}

func (self *RocksDbStore) Commit(topic, messageId string) bool {
	opLog := opBody{
		MessageId:       messageId,
		Topic:           topic,
		Commit:          true,
		NextDeliverTime: 0,
		DeliverCount:    0,
	}
	rawOpLog, err := json.Marshal(opLog)
	if nil != err {
		return false
	}
	//存储消息已提交
	err = self.rocksdb.Set([]byte(opLogKey(topic, messageId)), rawOpLog, pebble.Sync)
	if nil != err {
		return false
	}
	return true
}

func (self *RocksDbStore) Rollback(topic, messageId string) bool {

	//清理掉内存中的索引及recover
	if actual, loaded := self.recoverMap.LoadAndDelete(messageId); loaded {
		//清理掉需要recover的数据
		self.delChan <- actual.(*recoverItem)
	}

	batch := self.rocksdb.NewBatch()
	batch.Delete([]byte(msgKey(topic, messageId)), pebble.NoSync)
	batch.Delete([]byte(opLogKey(topic, messageId)), pebble.NoSync)
	batch.Commit(pebble.NoSync)

	return true
}

func (self *RocksDbStore) Delete(topic, messageId string) bool {

	//清理掉内存中的索引及recover
	if actual, loaded := self.recoverMap.LoadAndDelete(messageId); loaded {
		//清理掉需要recover的数据
		self.delChan <- actual.(*recoverItem)
	}

	batch := self.rocksdb.NewBatch()
	batch.Delete([]byte(msgKey(topic, messageId)), pebble.NoSync)
	batch.Delete([]byte(opLogKey(topic, messageId)), pebble.NoSync)
	batch.Commit(pebble.NoSync)
	return true
}

//过期消息进行清理
func (self *RocksDbStore) Expired(topic, messageId string) bool {

	//获取过期的数据，并清理掉
	entity := self.Query(topic, messageId)
	//将数据搬迁到DLQ中
	data := protocol.MarshalMessage(entity.Header, entity.MsgType, entity.GetBody())
	buff := make([]byte, len(data)+1)
	buff[0] = entity.MsgType
	copy(buff[1:], data)
	err := self.rockDLQ.Set([]byte(entity.Topic+":"+entity.MessageId), buff, &pebble.WriteOptions{Sync: true})
	if nil != err {
		return false
	}

	//清理掉正常的队列中的数据
	self.Delete(topic, messageId)
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

func (self *RocksDbStore) PageQueryEntity(hashKey string, kiteServer string, nextDeliveryTime int64, startIdx, limit int) (bool, []*store.MessageEntity) {

	//recover items
	self.pageQueryChan <- pageQuery{
		nextDeliveryTime: nextDeliveryTime,
		limit:            limit,
	}

	//获取查询之后的响应
	response := <-self.pageQueryResponseCh
	//获取对应的实体
	entities := make([]*store.MessageEntity, 0, len(response.items))
	for _, item := range response.items {
		if entity := self.Query(item.Topic, item.MessageId); nil != entity {
			entities = append(entities, entity)
		}
	}

	return response.hasMore, entities
}
