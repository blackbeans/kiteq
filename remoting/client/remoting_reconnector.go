package client

import (
	"container/heap"
	"sync"
	"time"
)

//-------------重连任务
type reconnectTask struct {
	remoteClient *RemotingClient
	retryCount   int
	nextTryTime  int64
	ga           *GroupAuth
	future       chan bool //重连回调
}

func newReconnectTasK(remoteClient *RemotingClient, ga *GroupAuth) *reconnectTask {
	return &reconnectTask{
		remoteClient: remoteClient,
		ga:           ga,
		retryCount:   0}
}

//先进行初次握手上传连接元数据
func (self *reconnectTask) reconnect(handshake func(ga *GroupAuth, remoteClient *RemotingClient) (bool, error)) (bool, error) {

	self.retryCount++
	self.nextTryTime = time.Now().Add(time.Duration(self.retryCount * 30 * 1000)).Unix()
	//开启remoteClient的重连任务
	succ, err := self.remoteClient.reconnect()
	if nil != err || !succ {
		return succ, err
	}

	return handshake(self.ga, self.remoteClient)
}

//重连管理器
type ReconnectManager struct {
	taskQ             *PriorityQueue //重连优先级队列
	lock              sync.Mutex
	allowReconnect    bool          //是否允许重连
	reconnectTimeout  time.Duration //重连超时
	maxReconnectTimes int32         //最大重连次数
	closed            bool          //是否关闭重连任务
	handshake         func(ga *GroupAuth, remoteClient *RemotingClient) (bool, error)
}

//重连管理器
func NewReconnectManager(allowReconnect bool,
	reconnectTimeout time.Duration, maxReconnectTimes int32,
	handshake func(ga *GroupAuth, remoteClient *RemotingClient) (bool, error)) *ReconnectManager {

	manager := &ReconnectManager{
		handshake:         handshake,
		taskQ:             NewPriorityQueue(10),
		allowReconnect:    allowReconnect,
		reconnectTimeout:  reconnectTimeout,
		maxReconnectTimes: maxReconnectTimes}
	return manager
}

//启动重连
func (self *ReconnectManager) Start() {
	go func() {
		for !self.closed {
			//处理重连任务
			// task := self.taskQ.Pop().(*reconnectTask)
			// if task.nextTryTime < time.Now().Unix() {
			// 	go func() {
			// 		succ, err := task.reconnect(self.handshake)
			// 		if nil != err || !succ {
			// 			//失败的额继续入队
			// 			self.taskQ.Push(task)
			// 		} else {
			// 			//连接成功的什么都不做
			// 		}
			// 	}()
			// }

			<-time.After(30 * time.Second)

		}
	}()
}

//提交重连任务
func (self *ReconnectManager) submit(task *reconnectTask) {
	self.lock.Lock()
	defer self.lock.Unlock()
	heap.Push(self.taskQ, &node{task: task})

}

func (self *ReconnectManager) stop() {
	self.closed = true
}

//-------------------优先级队列
type pqueue []*node

type PriorityQueue struct {
	pqueue
	lock sync.Mutex
	idx  int
}

func NewPriorityQueue(capacity int) *PriorityQueue {
	q := &PriorityQueue{}
	q.pqueue = make(pqueue, 0, capacity)
	return q
}

type node struct {
	task  *reconnectTask // The value of the item; arbitrary.
	index int            // The index of the item in the heap.
}

func (self PriorityQueue) Len() int { return len(self.pqueue) }

//按时间由小到大的顺序为优先级
func (self PriorityQueue) Less(i, j int) bool {
	return self.pqueue[i].task.nextTryTime < self.pqueue[j].task.nextTryTime
}

func (self PriorityQueue) Swap(i, j int) {
	self.pqueue[i], self.pqueue[j] = self.pqueue[j], self.pqueue[i]
	self.pqueue[i].index = i
	self.pqueue[j].index = j
}

func (self *PriorityQueue) Push(x interface{}) {
	self.idx++
	n := x.(*node)
	n.index = self.idx
	self.pqueue = append(self.pqueue, n)
}

func (self *PriorityQueue) Pop() interface{} {
	old := self.pqueue
	l := len(old)
	n := old[l-1]
	n.index = -1 // for safety
	self.pqueue = old[0 : l-1]
	return n
}

func (self *PriorityQueue) update(n *node, task *reconnectTask) {
	n.task = task
	heap.Fix(self, n.index)
}
