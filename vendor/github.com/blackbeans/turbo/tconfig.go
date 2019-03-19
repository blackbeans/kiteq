package turbo

import (
	"context"
	"github.com/blackbeans/pool"
	"sync/atomic"
	"time"
)

const (
	CONCURRENT_LEVEL = 8
)

//-----------响应的future
type Future struct {
	timeout    time.Duration
	opaque     int32
	response   chan interface{}
	TargetHost string
	Err        error
	ctx        context.Context
}

func NewFuture(opaque int32, timeout time.Duration, targetHost string, ctx context.Context) *Future {

	return &Future{
		timeout:    timeout,
		opaque:     opaque,
		response:   make(chan interface{}, 1),
		TargetHost: targetHost,
		ctx:        ctx,
		Err:        nil}
}

//创建有错误的future
func NewErrFuture(opaque int32, targetHost string, err error, ctx context.Context) *Future {
	return &Future{
		timeout:    0,
		opaque:     opaque,
		response:   make(chan interface{}, 1),
		TargetHost: targetHost,
		Err:        err,
		ctx:        ctx}
}

func (self *Future) Error(err error) {
	self.Err = err
	self.response <- err
}

func (self *Future) SetResponse(resp interface{}) {
	self.response <- resp

}

func (self *Future) Get(timeout <-chan time.Time) (interface{}, error) {
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
			return nil, ERR_TIMEOUT
		}

	case resp := <-self.response:
		e, ok := resp.(error)
		if ok {
			return nil, e
		} else {
			//如果没有错误直接等待结果
			return resp, nil
		}
	case <-self.ctx.Done():
		//当前请求已经被中断
		select {
		case resp := <-self.response:
			return resp, nil
		default:
			//返回链接中断
			return nil, ERR_CONNECTION_BROKEN
		}
	}
}

//网络层参数
type TConfig struct {
	FlowStat         *RemotingFlow //网络层流量
	dispool          pool.Pool     //   最大分发处理协程数
	ReadBufferSize   int           //读取缓冲大小
	WriteBufferSize  int           //写入缓冲大小
	WriteChannelSize int           //写异步channel长度
	ReadChannelSize  int           //读异步channel长度
	IdleTime         time.Duration //连接空闲时间
	RequestHolder    *ReqHolder
	TW               *TimerWheel // timewheel
}

func NewTConfig(name string,
	maxdispatcherNum,
	readbuffersize,
	writebuffersize,
	writechannlesize,
	readchannelsize int,
	idletime time.Duration,
	maxOpaque uint32) *TConfig {

	tw := NewTimerWheel(100*time.Millisecond, 50)

	rh := &ReqHolder{
		opaque:    0,
		holder:    NewLRUCache(int(maxOpaque), tw, nil),
		tw:        tw,
		idleTime:  idletime,
		maxOpaque: maxOpaque}

	qsize := uint(maxdispatcherNum) * 2
	if uint(maxdispatcherNum)*2 < 1000 {
		qsize = 1000
	}

	dispool := pool.NewExtLimited(
		uint(maxdispatcherNum)*30/100,
		uint(maxdispatcherNum),
		qsize,
		30*time.Second)
	//初始化
	rc := &TConfig{
		FlowStat:         NewRemotingFlow(name, dispool),
		dispool:          dispool,
		ReadBufferSize:   readbuffersize,
		WriteBufferSize:  writebuffersize,
		WriteChannelSize: writechannlesize,
		ReadChannelSize:  readchannelsize,
		IdleTime:         idletime,
		RequestHolder:    rh,
		TW:               tw,
	}
	return rc
}

type ReqHolder struct {
	maxOpaque uint32
	opaque    uint32
	tw        *TimerWheel
	holder    *LRUCache
	idleTime  time.Duration //连接空闲时间
}

func (self *ReqHolder) CurrentOpaque() int32 {
	return int32(atomic.AddUint32(&self.opaque, 1) % uint32(self.maxOpaque))
}

//从requesthold中移除
func (self *ReqHolder) Detach(opaque int32, obj interface{}) {

	future := self.holder.Remove(opaque)
	if nil != future {
		future.(*Future).SetResponse(obj)
	}
}

func (self *ReqHolder) Attach(opaque int32, future *Future) chan time.Time {
	return self.holder.Put(opaque, future, future.timeout)
}
