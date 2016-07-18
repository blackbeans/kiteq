package turbo

import (
	"golang.org/x/time/rate"
	"sync/atomic"
	"time"
)

type BurstyLimiter struct {
	rateLimiter   *rate.Limiter
	allowCount    int64
	preAllowCount int64
}

func NewBurstyLimiter(initPermits int, permitsPerSecond int) (*BurstyLimiter, error) {

	limiter := rate.NewLimiter(rate.Limit(permitsPerSecond), initPermits)

	return &BurstyLimiter{rateLimiter: limiter}, nil
}

func (self *BurstyLimiter) PermitsPerSecond() int {
	return int(self.rateLimiter.Limit())
}

//try acquire token
func (self *BurstyLimiter) AcquireCount(count int) bool {
	succ := self.rateLimiter.AllowN(time.Now(), count)
	if succ {
		atomic.AddInt64(&self.allowCount, int64(count))
	}
	return succ
}

//acquire token
func (self *BurstyLimiter) Acquire() bool {
	succ := self.rateLimiter.Allow()
	if succ {
		atomic.AddInt64(&self.allowCount, 1)
	}
	return succ
}

//return 1 : acquired
//return 2 : total
func (self *BurstyLimiter) LimiterInfo() (int, int) {
	tmp := atomic.LoadInt64(&self.allowCount)
	change := int(tmp - self.preAllowCount)
	self.preAllowCount = tmp

	return change, int(self.rateLimiter.Limit())
}

func (self *BurstyLimiter) Destroy() {

}
