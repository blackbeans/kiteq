package client

import (
	"container/heap"
	"sync"
	"time"
)

//-------------重连任务
type reconnectTask struct {
	// remoteClient *RemotingClient
	remote      string
	groupId     string
	secretKey   string
	retryCount  int32
	nextTryTime time.Duration

	future chan bool //重连回调
}

//重连管理器
type ReconnectManager struct {
	taskQ             *PriorityQueue //重连优先级队列
	lock              sync.Mutex
	allowReconnect    bool          //是否允许重连
	reconnectTimeout  time.Duration //重连超时
	maxReconnectTimes int32         //最大重连次数
	closed            bool          //是否关闭重连任务
}

//重连管理器
func NewReconnectManager(allowReconnect bool,
	reconnectTimeout time.Duration, maxReconnectTimes int32) *ReconnectManager {

	manager := &ReconnectManager{
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
