package turbo

import (
	"container/heap"
	"sync/atomic"
	"time"
)

type OnEvent func(t time.Time)

//一个timer任务
type Timer struct {
	timerId  int64
	Index    int
	expired  time.Time
	interval time.Duration
	//回调函数
	onTimeout OnEvent
	onCancel  OnEvent
	//是否重复过期
	repeated bool
}

type TimerHeap []*Timer

// A PriorityQueue implements heap.Interface and holds Items.
func (h TimerHeap) Len() int { return len(h) }

func (h TimerHeap) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	if h[i].expired.Before(h[j].expired) {
		return true
	}

	if h[i].timerId < h[i].timerId {
		return true
	}
	return false
}

func (h TimerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].Index = i
	h[j].Index = j
}

func (h *TimerHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*Timer)
	item.Index = n
	*h = append(*h, item)
}

func (h *TimerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	item.Index = -1 // for safety
	*h = old[0 : n-1]
	return item
}

var timerIds int64 = 0

const (
	MIN_INTERVAL = 100 * time.Millisecond
)

func timerId() int64 {
	return atomic.AddInt64(&timerIds, 1)
}

//时间轮
type TimerWheel struct {
	timerHeap   TimerHeap
	tick        *time.Ticker
	hashTimer   map[int64]*Timer
	interval    time.Duration
	cancelTimer chan int64
	addTimer    chan *Timer
	updateTimer chan Timer
	workLimit   chan *interface{}
}

func NewTimerWheel(interval time.Duration, workSize int) *TimerWheel {

	if int64(interval)-int64(MIN_INTERVAL) < 0 {
		interval = MIN_INTERVAL
	}

	tw := &TimerWheel{
		timerHeap:   make(TimerHeap, 0),
		tick:        time.NewTicker(interval),
		hashTimer:   make(map[int64]*Timer, 10),
		interval:    interval,
		updateTimer: make(chan Timer, 2000),
		cancelTimer: make(chan int64, 2000),
		addTimer:    make(chan *Timer, 2000),
		workLimit:   make(chan *interface{}, workSize),
	}
	heap.Init(&tw.timerHeap)
	tw.start()
	return tw
}

func (self *TimerWheel) After(timeout time.Duration) (int64, chan time.Time) {
	ch := make(chan time.Time, 1)
	t := &Timer{
		timerId:   timerId(),
		expired:   time.Now().Add(timeout),
		onTimeout: func(t time.Time) { ch <- t },
		onCancel:  nil}

	self.addTimer <- t
	return t.timerId, ch
}

//周期性的timer
func (self *TimerWheel) RepeatedTimer(interval time.Duration,
	onTimout OnEvent, onCancel OnEvent) (int64, chan time.Time) {
	ch := make(chan time.Time, 1)
	t := &Timer{
		repeated: true,
		interval: interval,
		timerId:  timerId(),
		expired:  time.Now().Add(interval),
		onTimeout: func(t time.Time) {
			ch <- t
			onTimout(t)
		},
		onCancel: onCancel}

	self.addTimer <- t
	return t.timerId, ch

}

func (self *TimerWheel) AddTimer(timeout time.Duration, onTimout OnEvent, onCancel OnEvent) (int64, chan time.Time) {
	ch := make(chan time.Time, 1)
	t := &Timer{
		timerId: timerId(),
		expired: time.Now().Add(timeout),
		onTimeout: func(t time.Time) {
			ch <- t
			onTimout(t)
		},
		onCancel: onCancel}

	self.addTimer <- t
	return t.timerId, ch
}

//更新timer的时间
func (self *TimerWheel) UpdateTimer(timerid int64, expired time.Time) {
	t := Timer{
		timerId: timerid,
		expired: expired}
	self.updateTimer <- t
}

//取消一个id
func (self *TimerWheel) CancelTimer(timerid int64) {
	self.cancelTimer <- timerid
}

func (self *TimerWheel) checkExpired(now time.Time) {
	for self.timerHeap.Len() > 0 {

		t := self.timerHeap.Pop().(*Timer)
		//如果过期时间再当前tick之前则超时
		//或者当前时间和过期时间的差距在一个Interval周期内那么就认为过期的
		cost := now.UnixNano() - t.expired.UnixNano()
		if t.expired.Before(now) ||
			(cost > 0 && cost < int64(self.interval)) {
			if nil != t.onTimeout {
				self.workLimit <- nil
				go func() {
					<-self.workLimit
					t.onTimeout(now)
				}()
				//如果是repeated的那么就检查并且重置过期时间
				if t.repeated {
					//如果是需要repeat的那么继续放回去
					t.expired = now.Add(t.interval)
					if time.Since(t.expired).Seconds() > 10 {
						t.expired = time.Now()
					}
					heap.Push(&self.timerHeap, t)
				}
			}
		} else {
			//没有过期那么久放回去
			heap.Push(&self.timerHeap, t)
			break
		}
	}
}

//
func (self *TimerWheel) start() {

	go func() {
		for {
			select {
			case now := <-self.tick.C:
				self.checkExpired(now)
			case updateT := <-self.updateTimer:
				if t, ok := self.hashTimer[updateT.timerId]; ok {
					t.expired = updateT.expired
				}
			case t := <-self.addTimer:
				heap.Push(&self.timerHeap, t)
				self.hashTimer[t.timerId] = t
			case timerid := <-self.cancelTimer:
				if t, ok := self.hashTimer[timerid]; ok {
					delete(self.hashTimer, timerid)
					heap.Remove(&self.timerHeap, t.Index)

					if nil != t.onCancel {
						self.workLimit <- nil
						go func() {
							<-self.workLimit
							t.onCancel(time.Now())
						}()
					}
				}
			}
		}
	}()
}
