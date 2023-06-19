package rocksdb

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/json"
	"github.com/blackbeans/logx"
	"github.com/valyala/bytebufferpool"
	"kiteq/store"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
)

var log = logx.GetLogger("kiteq_store")

//delvier tags
type opBody struct {
	Topic           string   `json:"topic"`
	MessageId       string   `json:"mid"`
	TTL             int64    `json:"ttl"`
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

//恢复的map
type recoverHeapMap struct {
	sort.Interface
	capacity int
	h        recoverHeap
	uniq     map[string]*recoverItem
}

func (m *recoverHeapMap) Less(i, j int) bool {
	return m.h.Less(i, j)
}

func (m *recoverHeapMap) Swap(i, j int) {
	m.h.Swap(i, j)
}

func (m *recoverHeapMap) Push(x interface{}) {
	item := x.(*recoverItem)

	if _, ok := m.uniq[msgKeyForBinlog(item.Topic, item.MessageId)]; !ok {
		//如果大于总容量，那么进行pop操作，老的数据先pop掉
		for len(m.uniq) >= m.capacity {
			m.Pop()
		}
		heap.Push(&m.h, x)
		m.uniq[msgKeyForBinlog(item.Topic, item.MessageId)] = item
	}
}

func (m *recoverHeapMap) Remove(item *recoverItem) {
	key := msgKeyForBinlog(item.Topic, item.MessageId)
	if v, ok := m.uniq[key]; ok {
		heap.Remove(&m.h, v.index)
		delete(m.uniq, key)
	}
}

func (m *recoverHeapMap) Pop() interface{} {
	x := heap.Pop(&m.h)
	if nil != x {
		item := x.(*recoverItem)
		delete(m.uniq, msgKeyForBinlog(item.Topic, item.MessageId))
	}
	return x
}

func (m recoverHeapMap) Peek() interface{} {
	return m.h[0]
}

func (m recoverHeapMap) Len() int { return len(m.uniq) }

//recover堆
type recoverHeap []*recoverItem

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

func (h recoverHeap) Len() int { return len(h) }

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
	rocksdb    *pebble.DB //数据
	rockBinlog *pebble.DB //投递记录

	recoverHeap *recoverHeapMap

	//添加和更新recoverItem
	addChan       chan *recoverItem
	upChan        chan *recoverItem
	delChan       chan *recoverItem
	pageQueryChan chan *pageQuery
}

func NewRocksDbStore(ctx context.Context, rocksDbDir string, options map[string]string) *RocksDbStore {
	return &RocksDbStore{
		ctx:        ctx,
		options:    options,
		rocksDbDir: rocksDbDir,
		recoverHeap: &recoverHeapMap{
			capacity: 50 * 10000,
			h:        make(recoverHeap, 0, 50*10000),
			uniq:     make(map[string]*recoverItem, 50*10000)},
		addChan:       make(chan *recoverItem, 5000),
		delChan:       make(chan *recoverItem, 5000),
		pageQueryChan: make(chan *pageQuery, 1),
	}
}

const (
	//消息头部
	Prefix_Header = "m:h:"
	Prefix_Body   = "m:b:"
)

var prefixIterator = func(prefix string) *pebble.IterOptions {
	return &pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: func(b []byte) []byte {
			end := make([]byte, len(b))
			copy(end, b)
			for i := len(end) - 1; i >= 0; i-- {
				end[i] = end[i] + 1
				if end[i] != 0 {
					return end[:i+1]
				}
			}
			return nil // no upper-bound
		}([]byte(prefix)),
	}
}

func msgKeyForHeader(topic, messageid string) string {
	return Prefix_Header + topic + ":" + messageid
}

func msgKeyForBody(topic, messageid string) string {
	return Prefix_Body + topic + ":" + messageid
}

func msgKeyForBinlog(topic, messageid string) string {
	return topic + ":" + messageid
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

	rockBinlog, err := pebble.Open(self.rocksDbDir+"/binlog/", &pebble.Options{
		MaxConcurrentCompactions: 5,
	})
	if nil != err {
		panic(err)
	}
	self.rockBinlog = rockBinlog

	//恢复oplog
	batch := rockBinlog.NewBatch()
	iter := rockBinlog.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
		var tmp opBody
		err := json.Unmarshal(iter.Value(), &tmp)
		if nil == err {
			self.addChan <- &recoverItem{
				index:           -1,
				Topic:           tmp.Topic,
				MessageId:       tmp.MessageId,
				NextDeliverTime: tmp.NextDeliverTime,
				DeliverCount:    tmp.DeliverCount,
			}
		} else {
			//解析 错误那么删除这个投递日志
			batch.Delete(iter.Key(), pebble.NoSync)
		}
	}
	//关闭遍历
	iter.Close()
	batch.Commit(pebble.Sync)
	batch.Close()

	iter = rocksdb.NewIter(prefixIterator(Prefix_Header))
	batch = rocksdb.NewBatch()
	for iter.First(); iter.Valid(); iter.Next() {
		topicAndMsgId := bytes.TrimPrefix(iter.Key(), []byte(Prefix_Header))
		split := strings.Split(string(topicAndMsgId), ":")
		//没有找到投递日志，则创建一个任务
		_, r, err := rockBinlog.Get(topicAndMsgId)
		if err == pebble.ErrNotFound {
			self.addChan <- &recoverItem{
				index:           -1,
				Topic:           split[0],
				MessageId:       split[1],
				NextDeliverTime: 0,
				DeliverCount:    0,
			}
		} else {
			r.Close()
		}

	}
	iter.Close()
	batch.Commit(pebble.Sync)
	batch.Close()
}

func (self *RocksDbStore) heapProcess() {
	for {
		select {
		case <-self.ctx.Done():
			self.rocksdb.Flush()
			self.rockBinlog.Flush()
			return
		case item := <-self.addChan:
			self.recoverHeap.Push(item)
		case del := <-self.delChan:
			self.recoverHeap.Remove(del)
		case pq := <-self.pageQueryChan:
			pageQuery := pq
			//recover items
			recoverItems := make([]*recoverItem, 0, 10)
			for self.recoverHeap.Len() > 0 {
				peak := self.recoverHeap.Peek()
				if peak.(*recoverItem).NextDeliverTime <= pageQuery.nextDeliveryTime && len(recoverItems) < pageQuery.limit {
					min := self.recoverHeap.Pop()
					recoverItems = append(recoverItems, min.(*recoverItem))
				} else {
					break
				}
			}

			hasMore := true
			if len(recoverItems) < pageQuery.limit {
				hasMore = false
			}
			//分页查询结果
			pageQuery.onResponse(hasMore, recoverItems...)
		}
	}
}

func (self *RocksDbStore) Stop() {
	self.rocksdb.Close()
	self.rockBinlog.Close()
}

func (self *RocksDbStore) Monitor() string {
	return "RecoverNum:" + strconv.FormatUint(uint64(self.recoverNum), 10)
}

func (self *RocksDbStore) Length() map[string]int {
	topics := make(map[string]int, 0)
	snapshot := self.rocksdb.NewSnapshot()
	it := snapshot.NewIter(prefixIterator(Prefix_Header))
	for it.First(); it.Valid(); it.Next() {
		topic := strings.Split(strings.TrimPrefix(string(it.Key()), Prefix_Header), ":")[0]
		topics[string(topic)]++
	}
	it.Close()
	snapshot.Close()

	snapshot = self.rockBinlog.NewSnapshot()
	it = snapshot.NewIter(nil)

	for it.First(); it.Valid(); it.Next() {
		topic := strings.Split(string(it.Key()), ":")[0]
		topics[topic+":binlog"]++
	}
	it.Close()
	snapshot.Close()
	topics["rocksDbHeap"] = self.recoverHeap.Len()
	return topics
}

func (self *RocksDbStore) MoveExpired() {
	//通知堆开始操作dlq的消息,遍历下目前所有的处于recover状态的数据
	//超过最大ttl和投递次数的消息都搬迁到DLQ的rocksdb中
	now := time.Now().Unix() / int64(time.Millisecond)
	iter := self.rockBinlog.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
		splits := strings.Split(string(iter.Key()), ":")
		var tmp opBody
		if err := json.Unmarshal(iter.Value(), &tmp); nil == err {
			if tmp.DeliverCount >= 100 || tmp.TTL <= now {
				self.Expired(tmp.Topic, tmp.MessageId)
				continue
			}
		}

		//尝试写入recover
		self.addChan <- &recoverItem{
			Topic:     splits[0],
			MessageId: splits[1],
		}
	}

	//压缩整理
	err := self.rockBinlog.Compact([]byte(""), []byte("z"))
	if nil != err {
		log.Warnf("RocksDbStore|MoveExpired.Compact.rockBinlog|FAIL|%v", err)
	}

	//整理
	err = self.rocksdb.Compact([]byte(""), []byte("z"))
	if nil != err {
		log.Warnf("RocksDbStore|MoveExpired.Compact.rocksdb|FAIL|%v", err)
	}
}

func (self *RocksDbStore) RecoverNum() int {
	return 1
}

func (self *RocksDbStore) AsyncUpdateDeliverResult(entity *store.MessageEntity) bool {
	opLog := opBody{
		Topic:           entity.Topic,
		MessageId:       entity.MessageId,
		TTL:             entity.Header.GetExpiredTime(),
		FailGroups:      entity.FailGroups,
		SuccGroups:      entity.SuccGroups,
		NextDeliverTime: entity.NextDeliverTime,
		DeliverCount:    entity.DeliverCount,
	}
	rawOpLog, err := json.Marshal(opLog)
	if nil != err {
		log.Errorf("KiteFileStore|AsyncUpdateDeliverResult|MarshalFAIL|%v", err)
		return false
	}

	err = self.rockBinlog.Set([]byte(msgKeyForBinlog(entity.Topic, entity.MessageId)), rawOpLog, pebble.NoSync)
	if nil != err {
		log.Errorf("KiteFileStore|AsyncUpdateDeliverResult|Set|FAIL|%v", err)
		return false
	}

	self.addChan <- &recoverItem{
		index:           -1,
		Topic:           entity.Topic,
		MessageId:       entity.MessageId,
		NextDeliverTime: entity.NextDeliverTime,
		TTL:             opLog.TTL,
		DeliverCount:    entity.DeliverCount,
	}
	return true
}

func (self *RocksDbStore) AsyncDelete(topic, messageId string) bool {
	return self.Delete(topic, messageId)
}

func (self *RocksDbStore) AsyncCommit(topic, messageId string) bool {
	return self.Commit(topic, messageId)
}

func (self *RocksDbStore) Query(topic, messageId string) *store.MessageEntity {
	key := []byte(msgKeyForHeader(topic, messageId))
	data, r, err := self.rocksdb.Get(key)
	if nil != err {
		log.Errorf("KiteFileStore|Query|FAIL|%v|%s", err, string(key))
		return nil
	}

	var header protocol.Header
	err = protocol.UnmarshalPbMessage(data[1:], &header)
	if nil != err {
		log.Errorf("KiteFileStore|Query.UnmarshalPbMessage|FAIL|%v|%s", err, string(key))
		return nil
	}
	r.Close()
	var entity *store.MessageEntity
	//构建body
	switch data[0] {
	case protocol.CMD_BYTES_MESSAGE:
		body, r, err := self.rocksdb.Get([]byte(msgKeyForBody(topic, messageId)))
		if nil != err {
			log.Errorf("KiteFileStore|Query|FAIL|%v|%s", err, string(key))
			return nil
		}
		r.Close()
		entity = store.NewMessageEntity(protocol.NewQMessage(&protocol.BytesMessage{
			Header: &header,
			Body:   body,
		}))
	case protocol.CMD_STRING_MESSAGE:

		body, r, err := self.rocksdb.Get([]byte(msgKeyForBody(topic, messageId)))
		if nil != err {
			log.Errorf("KiteFileStore|Query|FAIL|%v|%s", err, string(key))
			return nil
		}
		r.Close()
		entity = store.NewMessageEntity(protocol.NewQMessage(&protocol.StringMessage{
			Header: &header,
			Body:   proto.String(string(body)),
		}))
	default:
		log.Errorf("KiteFileStore|Query|INVALID|MSGTYPE|%d", data[0])
		return nil
	}

	//查询投递操作日志
	data, r, err = self.rockBinlog.Get([]byte(msgKeyForBinlog(topic, messageId)))
	if nil != err {
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
			log.Errorf("KiteFileStore|QueryLog|FAIL|%v", err)
		}
		r.Close()
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
	return entity

}

func (self *RocksDbStore) Save(entity *store.MessageEntity) bool {
	rawHeader, _ := protocol.MarshalPbMessage(entity.Header)

	buff := slicePool.Get()
	buff.WriteByte(entity.MsgType)
	buff.Write(rawHeader)

	batch := self.rocksdb.NewBatch()
	defer batch.Close()
	//header
	err := batch.Set([]byte(msgKeyForHeader(entity.Topic, entity.MessageId)), buff.Bytes(), pebble.NoSync)
	if nil != err {
		slicePool.Put(buff)
		return false
	}
	slicePool.Put(buff)
	switch entity.MsgType {
	case protocol.CMD_BYTES_MESSAGE:
		//body
		err = batch.Set([]byte(msgKeyForBody(entity.Topic, entity.MessageId)), entity.GetBody().([]byte), pebble.NoSync)
		if nil != err {
			return false
		}
	case protocol.CMD_STRING_MESSAGE:
		//body
		err = batch.Set([]byte(msgKeyForBody(entity.Topic, entity.MessageId)), []byte(entity.GetBody().(string)), pebble.NoSync)
		if nil != err {
			return false
		}
	}
	batch.Commit(pebble.NoSync)
	//提交

	//增加一个投递
	self.addChan <- &recoverItem{
		index:           -1,
		Topic:           entity.Header.GetTopic(),
		MessageId:       entity.Header.GetMessageId(),
		TTL:             entity.Header.GetExpiredTime(),
		NextDeliverTime: time.Now().Unix(),
		DeliverCount:    0,
	}
	return true
}

var slicePool = &bytebufferpool.Pool{}

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
		log.Errorf("KiteFileStore|Commit.UnmarshalPbMessage|FAIL|%v|%s|%s", err, topic, messageId)
		return false
	}
	r.Close()
	header.Commit = proto.Bool(true)
	newRawHeader, err := protocol.MarshalPbMessage(&header)
	if nil != err {
		log.Errorf("KiteFileStore|Commit.MarshalPbMessage|FAIL|%v|%s|%s", err, topic, messageId)
		return false
	}
	rawHeader = append(append(rawHeader[:0], rawHeader[0]), newRawHeader...)
	//body
	err = self.rocksdb.Set([]byte(msgKeyForHeader(topic, messageId)), rawHeader, pebble.NoSync)
	if nil != err {
		return false
	}

	//增加一个投递
	self.addChan <- &recoverItem{
		index:           -1,
		Topic:           header.GetTopic(),
		MessageId:       header.GetMessageId(),
		TTL:             header.GetExpiredTime(),
		NextDeliverTime: time.Now().Unix(),
		DeliverCount:    0,
	}

	return true
}

func (self *RocksDbStore) Rollback(topic, messageId string) bool {
	return self.Delete(topic, messageId)
}

func (self *RocksDbStore) Delete(topic, messageId string) bool {
	//清理掉需要recover的数据
	batch := self.rocksdb.NewBatch()
	batch.Delete([]byte(msgKeyForHeader(topic, messageId)), pebble.NoSync)
	batch.Delete([]byte(msgKeyForBody(topic, messageId)), pebble.NoSync)
	batch.Commit(pebble.NoSync)
	batch.Close()

	//投递日志也删掉
	self.rockBinlog.Delete([]byte(msgKeyForBinlog(topic, messageId)), pebble.NoSync)

	self.delChan <- &recoverItem{
		Topic:     topic,
		MessageId: messageId,
	}
	return true
}

//过期消息进行清理
func (self *RocksDbStore) Expired(topic, messageId string) bool {
	self.Delete(topic, messageId)
	return true
}

type pageQuery struct {
	sync.WaitGroup
	nextDeliveryTime int64
	limit            int
	onResponse       func(hasMore bool, items ...*recoverItem)
}

func (self *RocksDbStore) PageQueryEntity(hashKey string, kiteServer string, nextDeliverySeconds int64, startIdx, limit int) (bool, []*store.MessageEntity) {

	//获取对应的实体
	entities := make([]*store.MessageEntity, 0, limit)
	hasMore := false

	pq := &pageQuery{
		nextDeliveryTime: nextDeliverySeconds,
		limit:            limit,
	}
	pq.Add(1)
	pq.onResponse = func(more bool, items ...*recoverItem) {
		hasMore = more
		batch := self.rocksdb.NewIndexedBatch()
		defer func() {
			batch.Close()
			pq.Done()
		}()
		for _, item := range items {

			key := msgKeyForHeader(item.Topic, item.MessageId)
			rawHeader, hr, err := batch.Get([]byte(key))
			if nil != err {
				log.Errorf("KiteFileStore|PageQueryEntity.BatchGet|FAIL|%v|%s", err, string(key))
				self.Delete(item.Topic, item.MessageId)
				continue
			}
			var header protocol.Header
			err = protocol.UnmarshalPbMessage(rawHeader[1:], &header)
			if nil != err {
				hr.Close()
				log.Errorf("KiteFileStore|PageQueryEntity.UnmarshalPbMessage|FAIL|%v|%s", err, string(key))
				self.Delete(item.Topic, item.MessageId)
				continue
			}
			hr.Close()

			//创建消息
			entity := &store.MessageEntity{
				Header:    &header,
				MessageId: header.GetMessageId(),
				Topic:     header.GetTopic(),
				Commit:    header.GetCommit()}

			//merge data
			entities = append(entities, entity)
		}

	}

	//recover items
	self.pageQueryChan <- pq
	pq.Wait()
	return hasMore, entities
}
