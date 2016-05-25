package turbo

import (
	"errors"
	"fmt"
	"time"
)

type BurstyLimiter struct {
	permitsPerSecond int
	limiter          chan time.Time
	ticker           *time.Ticker
}

func NewBurstyLimiter(initPermits int, permitsPerSecond int) (*BurstyLimiter, error) {

	if initPermits < 0 || (permitsPerSecond < initPermits) {
		return nil,
			errors.New(fmt.Sprintf("BurstyLimiter initPermits[%d]<=permitsPerSecond[%d]", initPermits, permitsPerSecond))
	}
	pertick := int64(1*time.Second) / int64(permitsPerSecond)
	if pertick <= 0 {
		return nil,
			errors.New(fmt.Sprintf("BurstyLimiter int64(1*time.Second)< permitsPerSecond[%d]", permitsPerSecond))
	}

	tick := time.NewTicker(time.Duration(pertick))
	ch := make(chan time.Time, permitsPerSecond)
	for i := 0; i < initPermits; i++ {
		ch <- time.Now()
	}
	//insert token
	go func() {
		last := time.Now().UnixNano()
		unit := int64(1 * time.Second)
		for t := range tick.C {
			//calculate delay nano
			cost := (t.UnixNano() - last) % unit
			last = t.UnixNano()
			//calculate delay flow count
			delayCount := int(cost / pertick)
			for ; delayCount > 0; delayCount-- {
				ch <- t
			}
		}
	}()

	return &BurstyLimiter{permitsPerSecond: permitsPerSecond, ticker: tick, limiter: ch}, nil
}

// Ticker (%d) [1s/permitsPerSecond] Must be greater than tw.tickPeriod
func NewBurstyLimiterWithTikcer(initPermits int, permitsPerSecond int, tw *TimeWheel) (*BurstyLimiter, error) {

	pertick := int64(1*time.Second) / int64(permitsPerSecond)

	if pertick < int64(tw.tickPeriod) {
		// return nil, errors.New(fmt.Sprintf("Ticker (%d) [1s/permitsPerSecond] Must be greater than %d ", pertick, tw.tickPeriod))
		pertick = int64(tw.tickPeriod)
	}

	ch := make(chan time.Time, permitsPerSecond)
	for i := 0; i < initPermits; i++ {
		ch <- time.Now()
	}

	//insert token
	go func() {

		last := time.Now().UnixNano()
		unit := int64(1 * time.Second)
		for {
			//registry
			_, timeout := tw.After(time.Duration(pertick), func() {})
			<-timeout
			t := time.Now()
			//calculate delay nano
			cost := (t.UnixNano() - last) % unit
			last = t.UnixNano()
			//calculate delay flow count
			delayCount := int(cost / pertick)
			for ; delayCount > 0; delayCount-- {
				ch <- t
			}

		}
	}()

	return &BurstyLimiter{permitsPerSecond: permitsPerSecond, limiter: ch}, nil
}

func (self *BurstyLimiter) PermitsPerSecond() int {
	return self.permitsPerSecond
}

//try acquire token
func (self *BurstyLimiter) TryAcquire(timeout chan bool) bool {
	select {
	case <-self.limiter:
		return true
	case <-timeout:
		return false
	}
	return false
}

//try acquire token
func (self *BurstyLimiter) TryAcquireWithCount(timeout chan bool, count int) int {
	i := 0
	for ; i < count; i++ {
		select {
		case <-self.limiter:
		case <-timeout:
			return i
		}
	}

	return count
}

//acquire token
func (self *BurstyLimiter) Acquire() bool {
	if len(self.limiter) > 0 {
		select {
		case <-self.limiter:
			return true
		}
	} else {
		return false
	}
}

//return 1 : acquired
//return 2 : total
func (self *BurstyLimiter) LimiterInfo() (int, int) {
	return self.permitsPerSecond - len(self.limiter), self.permitsPerSecond
}

func (self *BurstyLimiter) Destroy() {
	if nil != self.ticker {
		self.ticker.Stop()
	}
}
