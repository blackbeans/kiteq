package turbo

import (
	"container/list"
	"fmt"
	log "github.com/blackbeans/log4go"
	"sync"
	"sync/atomic"
	"time"
)

type slotJob struct {
	id  uint32
	do  func()
	ttl int
	ch  chan bool
}

type Slot struct {
	index int
	hooks *list.List
	sync.RWMutex
}

type TimeWheel struct {
	maxHash        uint32
	autoId         int32
	tick           <-chan time.Time
	wheel          []*Slot
	hashWheel      map[uint32]*list.List
	ticksPerwheel  int32
	tickPeriod     time.Duration
	currentTick    int32
	slotJobWorkers chan bool
	lock           sync.RWMutex
}

//超时时间及每个个timewheel所需要的tick数
func NewTimeWheel(tickPeriod time.Duration, ticksPerwheel int, slotJobWorkers int) *TimeWheel {
	tick := time.NewTicker(tickPeriod)
	return NewTimeWheelWithTicker(tick.C, tickPeriod, ticksPerwheel, slotJobWorkers)
}

//超时时间及每个个timewheel所需要的tick数
func NewTimeWheelWithTicker(ticker <-chan time.Time, tickPeriod time.Duration, ticksPerwheel int, slotJobWorkers int) *TimeWheel {

	tw := &TimeWheel{
		maxHash:        50 * 10000,
		lock:           sync.RWMutex{},
		tickPeriod:     tickPeriod,
		hashWheel:      make(map[uint32]*list.List, 50*10000),
		tick:           ticker,
		slotJobWorkers: make(chan bool, slotJobWorkers),
		wheel: func() []*Slot {
			//ticksPerWheel make ticksPerWheel+1 slide
			w := make([]*Slot, 0, ticksPerwheel+1)
			for i := 0; i < ticksPerwheel+1; i++ {
				w = append(w, func() *Slot {
					return &Slot{
						index: i,
						hooks: list.New()}
				}())
			}
			return w
		}(),
		ticksPerwheel: int32(ticksPerwheel) + 1,
		currentTick:   0}

	go func() {
		// pre := time.Now()
		for i := int32(0); ; i++ {
			i = i % tw.ticksPerwheel
			<-tw.tick
			// fmt.Printf("-------------%d\n", t.Nanosecond()-pre.Nanosecond())
			atomic.StoreInt32(&tw.currentTick, i)
			tw.notifyExpired(i)
		}
	}()

	return tw
}

func (self *TimeWheel) Monitor() string {
	ticks := 0
	for _, v := range self.wheel {
		v.RLock()
		ticks += v.hooks.Len()
		v.RUnlock()
	}
	return fmt.Sprintf("TimeWheel|[total-tick:%d\tworkers:%d/%d]",
		ticks, len(self.slotJobWorkers), cap(self.slotJobWorkers))
}

//notifyExpired func
func (self *TimeWheel) notifyExpired(idx int32) {
	var remove []*list.Element

	self.lock.RLock()
	slots := self.wheel[idx]
	//-------clear expired
	slots.RLock()
	for e := slots.hooks.Back(); nil != e; e = e.Prev() {
		sj := e.Value.(*slotJob)
		sj.ttl--
		//ttl expired
		if sj.ttl <= 0 {
			if nil == remove {
				remove = make([]*list.Element, 0, 10)
			}

			//记录删除
			remove = append(remove, e)
			//写出超时
			sj.ch <- true

			self.slotJobWorkers <- true
			//async
			go func() {
				defer func() {
					if err := recover(); nil != err {
						//ignored
						log.Error("TimeWheel|notifyExpired|Do|ERROR|%s\n", err)
					}
					<-self.slotJobWorkers

				}()

				sj.do()

				// log.Debug("TimeWheel|notifyExpired|%d\n", sj.ttl)

			}()
		}
	}
	slots.RUnlock()
	self.lock.RUnlock()

	if len(remove) > 0 {
		slots.Lock()
		//remove
		for _, v := range remove {
			slots.hooks.Remove(v)
		}
		slots.Unlock()

		self.lock.Lock()
		//remove
		for _, v := range remove {
			job := v.Value.(*slotJob)
			hashId := self.hashId(job.id)
			link, ok := self.hashWheel[hashId]
			if ok {
				for e := link.Front(); nil != e; e = e.Next() {
					//same node remove
					if e.Value.(*list.Element) == v {
						//remove
						link.Remove(e)
						select {
						case <-job.ch:
						default:
						}
						break
					}
				}
			}
		}
		self.lock.Unlock()
	}

}

//add timeout func
func (self *TimeWheel) After(timeout time.Duration, do func()) (int64, chan bool) {

	sid := self.preTickIndex()
	ttl := int(int64(timeout) / (int64(self.tickPeriod) * int64(self.ticksPerwheel-1)))
	//fmt.Printf("After|TTL:%d|%d|%d|%d|%d\n", ttl, timeout, int64(self.tickPeriod), int64(self.ticksPerwheel-1), (int64(self.tickPeriod) * int64(self.ticksPerwheel-1)))
	tid := self.timerId(sid)
	ch := make(chan bool, 1)
	job := &slotJob{tid, do, ttl, ch}

	self.lock.Lock()
	slots := self.wheel[sid]
	slots.Lock()
	e := slots.hooks.PushFront(job)
	slots.Unlock()

	v, ok := self.hashWheel[self.hashId(tid)]
	if ok {
		self.hashWheel[self.hashId(tid)].PushBack(e)
	} else {
		v = list.New()
		v.PushBack(e)
		self.hashWheel[self.hashId(tid)] = v
	}
	self.lock.Unlock()
	return int64(tid), job.ch
}

func (self *TimeWheel) Remove(timerId int64) {
	tid := uint32(timerId)
	sid := self.decodeSlot(timerId)

	self.lock.RLock()
	sl := self.wheel[sid]
	link, ok := self.hashWheel[self.hashId(tid)]
	if ok {
		sl.Lock()
		//search and remove
		var tmp *list.Element
		for e := link.Front(); nil != e; e = e.Next() {
			tmp = e.Value.(*list.Element)
			job := tmp.Value.(*slotJob)
			if job.id == tid {
				//remove hashlink
				link.Remove(e)
				//remove slot
				sl.hooks.Remove(tmp)
				break
			}
		}
		sl.Unlock()

	}
	self.lock.RUnlock()
}

func (self *TimeWheel) hashId(id uint32) uint32 {
	return (id % self.maxHash)
}

func (self *TimeWheel) decodeSlot(timerId int64) uint32 {
	return uint32(timerId) >> 32
}

func (self *TimeWheel) timerId(idx int32) uint32 {

	return uint32(idx<<32 | atomic.AddInt32(&self.autoId, 1))
}

func (self *TimeWheel) preTickIndex() int32 {
	idx := atomic.LoadInt32(&self.currentTick)
	if idx > 0 {
		idx -= 1
	} else {
		idx = (self.ticksPerwheel - 1)
	}
	return idx
}
