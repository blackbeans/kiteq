package remoting

import (
	// 	log "github.com/blackbeans/log4go"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CONCURRENT_LEVEL = 16
)

//网络层参数
type RemotingConfig struct {
	FlowStat         *RemotingFlow //网络层流量
	MaxDispatcherNum chan int      //最大分发处理协程数
	ReadBufferSize   int           //读取缓冲大小
	WriteBufferSize  int           //写入缓冲大小
	WriteChannelSize int           //写异步channel长度
	ReadChannelSize  int           //读异步channel长度
	IdleTime         time.Duration //连接空闲时间
	RequestHolder    *ReqHolder
}

func NewRemotingConfig(
	rflow *RemotingFlow,
	maxdispatcherNum,
	readbuffersize, writebuffersize, writechannlesize, readchannelsize int,
	idletime time.Duration, maxOpaque int) *RemotingConfig {

	//定义holder
	holders := make([]map[int32]chan interface{}, 0, CONCURRENT_LEVEL)
	locks := make([]*sync.Mutex, 0, CONCURRENT_LEVEL)
	for i := 0; i < CONCURRENT_LEVEL; i++ {
		splitMap := make(map[int32]chan interface{}, maxOpaque/CONCURRENT_LEVEL)
		holders = append(holders, splitMap)
		locks = append(locks, &sync.Mutex{})
	}
	rh := &ReqHolder{
		opaque:    0,
		locks:     locks,
		holders:   holders,
		maxOpaque: maxOpaque}

	//初始化
	rc := &RemotingConfig{
		FlowStat:         rflow,
		MaxDispatcherNum: make(chan int, maxdispatcherNum),
		ReadBufferSize:   readbuffersize,
		WriteBufferSize:  writebuffersize,
		WriteChannelSize: writechannlesize,
		ReadChannelSize:  readchannelsize,
		IdleTime:         idletime,
		RequestHolder:    rh}
	return rc
}

type ReqHolder struct {
	maxOpaque int
	opaque    uint32
	locks     []*sync.Mutex
	holders   []map[int32]chan interface{}
}

func (self *ReqHolder) CurrentOpaque() int32 {
	return int32((atomic.AddUint32(&self.opaque, 1) % uint32(self.maxOpaque)))
}

//从requesthold中移除
func (self *ReqHolder) Detach(opaque int32, obj interface{}) {

	l, m := self.locker(opaque)
	l.Lock()
	defer l.Unlock()

	ch, ok := m[opaque]
	if ok {
		delete(m, opaque)
		ch <- obj
		close(ch)
		// log.Printf("ReqHolder|Attach|%s|%s\n", opaque, obj)
	}
}

func (self *ReqHolder) Attach(opaque int32, ch chan interface{}) {
	l, m := self.locker(opaque)
	l.Lock()
	defer l.Unlock()
	m[opaque] = ch
}

func (self *ReqHolder) locker(id int32) (*sync.Mutex, map[int32]chan interface{}) {
	return self.locks[id%CONCURRENT_LEVEL], self.holders[id%CONCURRENT_LEVEL]
}
