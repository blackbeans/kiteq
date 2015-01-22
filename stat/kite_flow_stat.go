package stat

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

type FlowMonitor struct {
	Name      string
	count     int32
	lastcount int32
}

func (self *FlowMonitor) Incr(num int32) {
	atomic.AddInt32(&self.count, num)
}

func (self *FlowMonitor) changes() int32 {
	tmpc := self.count
	tmpl := self.lastcount
	c := tmpc - tmpl
	self.lastcount = tmpc
	return c
}

var monitors map[string]*FlowMonitor = make(map[string]*FlowMonitor, 10)

func MarkFlow(fm *FlowMonitor) {
	monitors[fm.Name] = fm

}

func SechduleMarkFlow() {
	go func() {
		for {

			i := 0
			line := ""
			for k, v := range monitors {
				line += fmt.Sprintf("%s\t%d", k, v.changes())
				i++
				if i%10 == 0 {
					line += "\n"
				}
			}
			log.Println(line)
			time.Sleep(1 * time.Second)

		}
	}()
}
