package turbo

import (
	"errors"
	"sync/atomic"
	"time"
)

const (
	CONCURRENT_LEVEL = 8
)

var TIMEOUT_ERROR = errors.New("WAIT RESPONSE TIMEOUT ")
var ERROR_OVER_FLOW = errors.New("Group Over Flow")
var ERROR_NO_HOSTS = errors.New("NO VALID RemoteClient")

//-----------响应的future
type Future struct {
	opaque     int32
	response   chan interface{}
	TargetHost string
	Err        error
}

func NewFuture(opaque int32, TargetHost string) *Future {

	return &Future{
		opaque,
		make(chan interface{}, 1),
		TargetHost,
		nil}
}

//创建有错误的future
func NewErrFuture(opaque int32, TargetHost string, err error) *Future {
	return &Future{
		opaque,
		make(chan interface{}, 1),
		TargetHost,
		err}
}

func (self Future) Error(err error) {
	self.Err = err
	self.response <- err
}

func (self Future) SetResponse(resp interface{}) {
	self.response <- resp

}

func (self Future) Get(timeout <-chan time.Time) (interface{}, error) {
	//强制设置
	if nil != self.Err {
		return nil, self.Err
	}

	select {
	case <-timeout:
		select {
		case resp := <-self.response:
			return resp, nil
		default:
			//如果是已经超时了但是当前还是没有响应也认为超时
			return nil, TIMEOUT_ERROR
		}

	case resp := <-self.response:
		e, ok := resp.(error)
		if ok {
			return nil, e
		} else {
			//如果没有错误直接等待结果
			return resp, nil
		}
	}
}

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
	TW               *TimeWheel // timewheel
}

func NewRemotingConfig(name string,
	maxdispatcherNum,
	readbuffersize, writebuffersize, writechannlesize, readchannelsize int,
	idletime time.Duration, maxOpaque int) *RemotingConfig {

	//定义holder
	holders := make([]map[int32]*Future, 0, CONCURRENT_LEVEL)
	locks := make([]chan bool, 0, CONCURRENT_LEVEL)
	for i := 0; i < CONCURRENT_LEVEL; i++ {
		splitMap := make(map[int32]*Future, maxOpaque/CONCURRENT_LEVEL)
		holders = append(holders, splitMap)
		locks = append(locks, make(chan bool, 1))
	}
	rh := &ReqHolder{
		opaque:    0,
		locks:     locks,
		holders:   holders,
		maxOpaque: maxOpaque}

	//初始化
	rc := &RemotingConfig{
		FlowStat:         NewRemotingFlow(name),
		MaxDispatcherNum: make(chan int, maxdispatcherNum),
		ReadBufferSize:   readbuffersize,
		WriteBufferSize:  writebuffersize,
		WriteChannelSize: writechannlesize,
		ReadChannelSize:  readchannelsize,
		IdleTime:         idletime,
		RequestHolder:    rh,
		TW:               NewTimeWheel(1*time.Second, 6, 10)}
	return rc
}

type ReqHolder struct {
	maxOpaque int
	opaque    uint32
	locks     []chan bool
	holders   []map[int32]*Future
}

func (self *ReqHolder) CurrentOpaque() int32 {
	return int32((atomic.AddUint32(&self.opaque, 1) % uint32(self.maxOpaque)))
}

//从requesthold中移除
func (self *ReqHolder) Detach(opaque int32, obj interface{}) {

	l, m := self.locker(opaque)
	l <- true
	defer func() { <-l }()

	future, ok := m[opaque]
	if ok {
		delete(m, opaque)
		future.SetResponse(obj)
		// log.Printf("ReqHolder|Attach|%s|%s\n", opaque, obj)
	}
}

func (self *ReqHolder) Attach(opaque int32, future *Future) {
	l, m := self.locker(opaque)
	l <- true
	defer func() { <-l }()
	m[opaque] = future
}

func (self *ReqHolder) locker(id int32) (chan bool, map[int32]*Future) {
	return self.locks[id%CONCURRENT_LEVEL], self.holders[id%CONCURRENT_LEVEL]
}
