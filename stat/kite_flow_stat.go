package stat

import (
	"fmt"
	"log"
	"sort"
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
var monitorNames []string = make([]string, 0, 10)

func MarkFlow(fm *FlowMonitor) {
	monitors[fm.Name] = fm
	monitorNames = append(monitorNames, fm.Name)
	sort.Strings(monitorNames)
}

func UnmarkFlow(names ...string) {
	for _, name := range names {
		delete(monitors, name)
	}
}

func SechduleMarkFlow() {
	go func() {
		for {
			i := 0
			line := ""
			for _, k := range monitorNames {
				v, ok := monitors[k]
				if ok {
					line += fmt.Sprintf("%s:%d\t", k, v.changes())
					i++
				}
				if i%10 == 0 {
					line += "\n"
				}

			}
			log.Println(line)
			time.Sleep(1 * time.Second)

		}
	}()
}
