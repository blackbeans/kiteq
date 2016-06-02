package client

import (
	log "github.com/blackbeans/log4go"
	"math"
	"sync"
	"time"
)

//-------------重连任务
type reconnectTask struct {
	remoteClient *RemotingClient
	retryCount   int
	ga           *GroupAuth
	finishHook   func(addr string)
}

func newReconnectTasK(remoteClient *RemotingClient, ga *GroupAuth, finishHook func(addr string)) *reconnectTask {
	return &reconnectTask{
		remoteClient: remoteClient,
		ga:           ga,
		retryCount:   0,
		finishHook:   finishHook}
}

//先进行初次握手上传连接元数据
func (self *reconnectTask) reconnect(handshake func(ga *GroupAuth, remoteClient *RemotingClient) (bool, error)) (bool, error) {

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
	timers            map[string] /*hostport*/ *time.Timer
	allowReconnect    bool          //是否允许重连
	reconnectTimeout  time.Duration //重连超时
	maxReconnectTimes int           //最大重连次数
	handshake         func(ga *GroupAuth, remoteClient *RemotingClient) (bool, error)
	lock              sync.Mutex
}

//重连管理器
func NewReconnectManager(allowReconnect bool,
	reconnectTimeout time.Duration, maxReconnectTimes int,
	handshake func(ga *GroupAuth, remoteClient *RemotingClient) (bool, error)) *ReconnectManager {

	manager := &ReconnectManager{
		timers:            make(map[string]*time.Timer, 20),
		allowReconnect:    allowReconnect,
		reconnectTimeout:  reconnectTimeout,
		maxReconnectTimes: maxReconnectTimes, handshake: handshake}
	log.Info("ReconnectManager|Start...")
	return manager
}

//提交重连任务
func (self *ReconnectManager) submit(c *RemotingClient, ga *GroupAuth, finishHook func(addr string)) {
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
	self.startReconTask(newReconnectTasK(c, ga, finishHook))
}

func (self *ReconnectManager) startReconTask(task *reconnectTask) {
	addr := task.remoteClient.RemoteAddr()

	//创建定时的timer
	timer := time.AfterFunc(self.reconnectTimeout, func() {
		log.Info("ReconnectManager|START RECONNECT|%s|retryCount:%d\n", addr, task.retryCount)
		succ, err := task.reconnect(self.handshake)
		if nil != err || !succ {
			self.lock.Lock()
			defer self.lock.Unlock()
			timer := self.timers[addr]

			//如果当前重试次数大于最大重试次数则放弃
			if task.retryCount > self.maxReconnectTimes {
				log.Info("ReconnectManager|OVREFLOW MAX TRYCOUNT|REMOVE|%s|%d\n", addr, task.retryCount)
				t, ok := self.timers[addr]
				if ok {
					t.Stop()
					delete(self.timers, addr)
					task.finishHook(addr)
				}
				return
			}

			connTime := time.Duration(int64(math.Pow(2, float64(task.retryCount))) * int64(self.reconnectTimeout))
			//继续提交重试任务,如果成功那么就不用再重建
			timer.Reset(connTime)
		} else {
			self.lock.Lock()
			defer self.lock.Unlock()
			_, ok := self.timers[addr]
			if ok {
				delete(self.timers, addr)
			}
		}
		log.Info("ReconnectManager|END RECONNECT|%s|addr:%s|succ:%s,err:%s,|retryCount:%d\n", task.remoteClient.RemoteAddr(), addr, succ, err, task.retryCount)
	})

	//记录timer
	self.timers[addr] = timer
}

//取消重连任务
func (self *ReconnectManager) cancel(hostport string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	t, ok := self.timers[hostport]
	if ok {
		delete(self.timers, hostport)
		t.Stop()
	}
}

func (self *ReconnectManager) stop() {
	self.allowReconnect = false
	self.lock.Lock()
	defer self.lock.Unlock()
	for _, t := range self.timers {
		t.Stop()
	}
	log.Info("ReconnectManager|stop...")
}
