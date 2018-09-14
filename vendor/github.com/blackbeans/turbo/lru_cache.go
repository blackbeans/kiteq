package turbo

import (
	"time"
	"sync/atomic"
)

type Entry struct {
	Timerid int64
	Value   interface{}
}

//lru的cache
type LRUCache struct {
	hit   uint64
	total uint64

	cache     *cache
	OnEvicted func(k, v interface{})
	tw        *TimerWheel
}

//LRU的cache
func NewLRUCache(maxcapacity int,
	expiredTw *TimerWheel,
	OnEvicted func(k, v interface{})) *LRUCache {

	cache := New(maxcapacity)
	cache.OnEvicted = func(key Key, value interface{}) {
		vv := value.(Entry).Value
		if nil!=OnEvicted {
			OnEvicted(key.(interface{}), vv)
		}
	}

	return &LRUCache{
		cache:     cache,
		tw:        expiredTw,
		OnEvicted: OnEvicted}
}

//本时间段的命中率
func (self *LRUCache) HitRate() int {
	currHit := self.hit
	atomic.StoreUint64(&self.hit, 0)
	currTotal := self.total
	atomic.StoreUint64(&self.total, 0)

	hitrate := int(currHit * 100 / currTotal)

	return hitrate
}

//获取
func (self *LRUCache) Get(key interface{}) (interface{}, bool) {
	atomic.AddUint64(&self.total, 1)
	if v, ok := self.cache.Get(key); ok {
		atomic.AddUint64(&self.hit, 1)
		return v.(Entry).Value, true
	}
	return nil, false
}

//增加元素
func (self *LRUCache) Put(key, v interface{}, ttl time.Duration) chan time.Time{
	entry := Entry{Value: v}
	var ttlChan chan  time.Time
	if ttl > 0 {
		//超时过期或者取消的时候也删除
		if nil != self.tw {
			timerid, ch := self.tw.AddTimer(ttl, func(t time.Time){
				v:= self.cache.Remove(key)
				if nil != v && nil!=self.OnEvicted{
					self.OnEvicted(key, v)
				}
			},
				func(t time.Time) {
				self.cache.Remove(key)
			})
			entry.Timerid = timerid
			ttlChan = ch
		}
	}
	self.cache.Add(key, entry)
	return ttlChan
}

//移除元素
func (self *LRUCache) Remove(key interface{}) (interface{}) {
	entry := self.cache.Remove(key)
	if nil != entry {
		e := entry.(Entry)
		if nil != self.tw {
			self.tw.CancelTimer(e.Timerid)
		}
		return e.Value
	}

	return nil
}

//是否包含当前的KEY
func (self *LRUCache) Contains(key interface{}) (bool) {
	if _, ok := self.cache.Get(key); ok {
		return ok
	}
	return false
}

//c length
func (self *LRUCache) Length() int {
	return self.cache.Len()
}
