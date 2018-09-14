package turbo

import (
	"sync/atomic"
	"time"
	"gopkg.in/go-playground/pool.v3"
)

const (
	CONCURRENT_LEVEL = 8
)



//-----------响应的future
type Future struct {
	timeout time.Duration
	opaque     int32
	response   chan interface{}
	TargetHost string
	Err        error
}

func NewFuture(opaque int32,timeout time.Duration, targetHost string) *Future {

	return &Future{
		timeout:    timeout,
		opaque:     opaque,
		response:   make(chan interface{}, 1),
		TargetHost: targetHost,
		Err:        nil}
}

//创建有错误的future
func NewErrFuture(opaque int32, targetHost string, err error) *Future {
	return &Future{
		timeout:    0,
		opaque:     opaque,
		response:   make(chan interface{}, 1),
		TargetHost: targetHost,
		Err:        err}
}

func (self *Future) Error(err error) {
	self.Err = err
	self.response <- err
}

func  (self *Future)  SetResponse(resp interface{}) {
	self.response <- resp

}

func  (self *Future)  Get(timeout <-chan time.Time) (interface{}, error) {
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
		holder:NewLRUCache(int(maxOpaque),tw,nil),
		tw : tw,
		idleTime:idletime,
		maxOpaque: maxOpaque}

	dispool := pool.NewLimited(uint(maxdispatcherNum))
	//初始化
	rc := &TConfig{
		FlowStat:         NewRemotingFlow(name),
		dispool:          dispool,
		ReadBufferSize:   readbuffersize,
		WriteBufferSize:  writebuffersize,
		WriteChannelSize: writechannlesize,
		ReadChannelSize:  readchannelsize,
		IdleTime:         idletime,
		RequestHolder:    rh,
		TW:               tw,
	}
	rc.FlowStat.DispatcherGo.count = int64(maxdispatcherNum)
	return rc
}

type ReqHolder struct {
	maxOpaque uint32
	opaque    uint32
	tw 	*TimerWheel
	holder *LRUCache
	idleTime  time.Duration //连接空闲时间
}

func (self *ReqHolder) CurrentOpaque() int32 {
	return int32(atomic.AddUint32(&self.opaque, 1) % uint32(self.maxOpaque))
}

//从requesthold中移除
func (self *ReqHolder) Detach(opaque int32, obj interface{}) {

	future := self.holder.Remove(opaque)
	if nil!=future{
		future.(*Future).SetResponse(obj)
	}
}

func (self *ReqHolder) Attach(opaque int32, future *Future) chan time.Time{
	return self.holder.Put(opaque,future,future.timeout)
}
