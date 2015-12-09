package stat

import (
	"github.com/golang/groupcache/lru"
	"time"
)

//投递注册器
type DeliveryRegistry struct {
	registry []*lru.Cache //key为messageId-->value为过期时间
	locks    []chan bool
}

/*
*
*
 */
func NewDeliveryRegistry(capacity int) *DeliveryRegistry {

	maxCapacity := capacity / 16
	locks := make([]chan bool, 0, 16)
	for i := 0; i < 16; i++ {
		locks = append(locks, make(chan bool, 1))
	}

	registry := make([]*lru.Cache, 0, 16)
	for i := 0; i < 16; i++ {
		cache := lru.New(maxCapacity)
		registry = append(registry, cache)
	}

	return &DeliveryRegistry{registry: registry, locks: locks}
}

/*
*注册投递事件
**/
func (self DeliveryRegistry) Registe(messageId string, exp time.Duration) bool {
	hashKey := messageId[len(messageId)-1]
	hashKey = hashKey % 16
	ch := self.locks[hashKey]
	defer func() { <-ch }()
	ch <- true
	//过期或者不存在在直接覆盖设置
	val, ok := self.registry[hashKey].Get(messageId)
	now := time.Now()
	expiredTime := now.Add(exp)
	if !ok || time.Time(val.(time.Time)).Before(now) {
		self.registry[hashKey].Add(messageId, expiredTime)
		return true
	} else {
		//如果不存在
		return false
	}

}

//取消注册
func (self DeliveryRegistry) UnRegiste(messageId string) {
	hashKey := messageId[len(messageId)-1]
	hashKey = hashKey % 16
	ch := self.locks[hashKey]
	defer func() { <-ch }()
	ch <- true
	self.registry[hashKey].Remove(messageId)
}
