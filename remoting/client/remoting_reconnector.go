package client

import (
	"log"
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
	taskQ             chan *reconnectTask
	timers            map[string] /*hostport*/ *time.Timer
	allowReconnect    bool          //是否允许重连
	reconnectTimeout  time.Duration //重连超时
	maxReconnectTimes int           //最大重连次数
	handshake         func(ga *GroupAuth, remoteClient *RemotingClient) (bool, error)
}

//重连管理器
func NewReconnectManager(allowReconnect bool,
	reconnectTimeout time.Duration, maxReconnectTimes int,
	handshake func(ga *GroupAuth, remoteClient *RemotingClient) (bool, error)) *ReconnectManager {

	manager := &ReconnectManager{
		taskQ:             make(chan *reconnectTask, 1000),
		timers:            make(map[string]*time.Timer, 20),
		allowReconnect:    allowReconnect,
		reconnectTimeout:  reconnectTimeout,
		maxReconnectTimes: maxReconnectTimes, handshake: handshake}
	return manager
}

//启动重连
func (self *ReconnectManager) Start() {
	go func() {
		for self.allowReconnect {
			task := <-self.taskQ
			addr := task.remoteClient.RemoteAddr()
			//如果当前重试次数大于最大重试次数则放弃
			if task.retryCount > self.maxReconnectTimes {
				log.Printf("ReconnectManager|OVREFLOW MAX TRYCOUNT|REMOVE|%s|%d\n", addr, task.retryCount)
				t, ok := self.timers[addr]
				if ok {
					t.Stop()
					delete(self.timers, addr)
					task.finishHook(addr)
				}
				continue
			}

			timer := time.AfterFunc(time.Duration(task.retryCount*10)*time.Second, func() {
				log.Printf("ReconnectManager|START RECONNECT|%s|retryCount:%d\n", addr, task.retryCount)
				succ, err := task.reconnect(self.handshake)
				if nil != err || !succ {
					timer := self.timers[task.remoteClient.RemoteAddr()]
					//继续提交重试任务,如果成功那么就不用再重建
					timer.Reset(time.Duration(task.retryCount*10) * time.Second)
				} else {
					_, ok := self.timers[addr]
					if ok {
						delete(self.timers, addr)
					}
				}
				log.Printf("ReconnectManager|END RECONNECT|%s|addr:%s|succ:%s,err:%s,|retryCount:%d\n", task.remoteClient.RemoteAddr(), addr, succ, err, task.retryCount)
			})

			//记录timer
			self.timers[addr] = timer
		}

	}()
}

//提交重连任务
func (self *ReconnectManager) submit(task *reconnectTask) {
	if !self.allowReconnect {
		return
	}
	//如果已经有该重连任务在执行则忽略
	_, ok := self.timers[task.remoteClient.RemoteAddr()]
	if ok {
		return
	}
	self.taskQ <- task

}

//取消重连任务
func (self *ReconnectManager) cancel(hostport string) {
	t, ok := self.timers[hostport]
	if ok {
		t.Stop()
	}
}

func (self *ReconnectManager) stop() {
	self.allowReconnect = false
}
