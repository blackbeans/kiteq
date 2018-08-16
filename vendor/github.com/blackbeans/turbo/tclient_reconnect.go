package turbo

import (
	log "github.com/blackbeans/log4go"
	"math"
	"sync"
	"time"
)

//-------------重连任务
type reconnectTask struct {
	remoteClient *TClient
	retryCount   int
	nextTimeout  time.Duration
	ga           *GroupAuth
	finishHook   func(addr string)
}

func newReconnectTasK(remoteClient *TClient, ga *GroupAuth, finishHook func(addr string)) *reconnectTask {
	return &reconnectTask{
		remoteClient: remoteClient,
		ga:           ga,
		retryCount:   0,
		finishHook:   finishHook}
}

//先进行初次握手上传连接元数据
func (self *reconnectTask) reconnect(handshake func(ga *GroupAuth, remoteClient *TClient) (bool, error)) (bool, error) {

	self.retryCount++
	//开启remoteClient的重连任务
	succ, err := self.remoteClient.reconnect()
	if nil != err || !succ {
		return succ, err
	}

	return handshake(self.ga, self.remoteClient)
}

//重连管理器
type ReconnectManager struct {
	timers            map[string] /*hostport*/int64
	allowReconnect    bool          //是否允许重连
	reconnectTimeout  time.Duration //重连超时
	maxReconnectTimes int           //最大重连次数
	tw *TimerWheel
	handshake         func(ga *GroupAuth, remoteClient *TClient) (bool, error)
	lock              sync.Mutex
}

//重连管理器
func NewReconnectManager(allowReconnect bool,
	reconnectTimeout time.Duration, maxReconnectTimes int,
	handshake func(ga *GroupAuth, remoteClient *TClient) (bool, error)) *ReconnectManager {

	manager := &ReconnectManager{
		timers:            make(map[string]int64, 20),
		tw : NewTimerWheel(1* time.Second,10),
		allowReconnect:    allowReconnect,
		reconnectTimeout:  reconnectTimeout,
		maxReconnectTimes: maxReconnectTimes, handshake: handshake}
	log.Info("ReconnectManager|Start...")
	return manager
}

//提交重连任务
func (self *ReconnectManager) submit(c *TClient, ga *GroupAuth, finishHook func(addr string)) {
	if !self.allowReconnect {
		return
	}
	self.lock.Lock()
	defer self.lock.Unlock()
	//如果已经有该重连任务在执行则忽略
	_, ok := self.timers[c.RemoteAddr()]
	if ok {
		return
	}
	self.submit0(newReconnectTasK(c, ga, finishHook))
}

//提交一个任务
//调用方外面需要加锁。否则还有并发安全问题。
func(self *ReconnectManager) submit0(task *reconnectTask) {

	addr := task.remoteClient.RemoteAddr()
	//创建定时的timer
	timerid, _ := self.tw.AddTimer(
		task.nextTimeout,
		func(t time.Time) {
		succ, err := task.reconnect(self.handshake)
		if nil != err || !succ {
			log.Info("ReconnectManager|RECONNECT|FAIL|%v|%s|%d",err, addr, task.retryCount)
			retry := func() bool{
				self.lock.Lock()
				defer self.lock.Unlock()
				//如果当前重试次数大于最大重试次数则放弃
				if task.retryCount > self.maxReconnectTimes {
					log.Info("ReconnectManager|OVREFLOW MAX TRYCOUNT|REMOVE|%s|%d", addr, task.retryCount)
					_, ok := self.timers[addr]
					if ok {
						delete(self.timers, addr)
						task.finishHook(addr)
					}
					return false
				}
				return true
			}()
			if retry {
				//继续进行下次重连
				task.nextTimeout = time.Duration(int64(math.Pow(2, float64(task.retryCount))) * int64(self.reconnectTimeout))
				//记录timer
				self.lock.Lock()
				self.submit0(task)
				self.lock.Unlock()
			}

		} else {
			self.lock.Lock()
			defer self.lock.Unlock()
			_, ok := self.timers[addr]
			if ok {
				delete(self.timers, addr)
			}
			log.Info("ReconnectManager|RECONNECT|SUCC|%s|addr:%s|retryCount:%d", task.remoteClient.RemoteAddr(), addr,  task.retryCount)
		}

	}, nil)

	//调用方保证这里线程安全
	self.timers[addr] = timerid
}

//取消重连任务
func (self *ReconnectManager) cancel(hostport string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	tid, ok := self.timers[hostport]
	if ok {
		delete(self.timers, hostport)
		self.tw.CancelTimer(tid)
	}
}

func (self *ReconnectManager) stop() {
	self.allowReconnect = false
	self.lock.Lock()
	defer self.lock.Unlock()
	for _, tid := range self.timers {
		self.tw.CancelTimer(tid)
	}
	log.Info("ReconnectManager|stop...")
}
