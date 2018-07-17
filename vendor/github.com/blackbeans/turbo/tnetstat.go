package turbo

import (
	"fmt"
	"sync/atomic"
)

//network stat
type NetworkStat struct {
	ReadCount    int32 `json:"read_count"`
	ReadBytes    int32 `json:"read_bytes"`
	WriteCount   int32 `json:"write_count"`
	WriteBytes   int32 `json:"write_bytes"`
	DispatcherGo int32 `json:"dispatcher_go"`
	Connections  int32 `json:"connections"`
}

func (self NetworkStat) String() string {
	return fmt.Sprintf("read:%dKB/%d\twrite:%dKB/%d\tgo:%d\tconns:%d", self.ReadBytes/1024, self.ReadCount,
		self.WriteBytes/1024, self.WriteCount, self.DispatcherGo, self.Connections)
}

type RemotingFlow struct {
	Name           string
	OptimzeStatus  bool //当前优化的状态
	ReadFlow       *Flow
	ReadBytesFlow  *Flow
	DispatcherGo   *Flow
	WriteFlow      *Flow
	WriteBytesFlow *Flow
	Connections    *Flow
}

func NewRemotingFlow(name string) *RemotingFlow {
	return &RemotingFlow{
		OptimzeStatus:  true,
		Name:           name,
		ReadFlow:       &Flow{},
		ReadBytesFlow:  &Flow{},
		DispatcherGo:   &Flow{},
		WriteFlow:      &Flow{},
		WriteBytesFlow: &Flow{},
		Connections:    &Flow{}}
}

//网络状态
func (self *RemotingFlow) Stat() NetworkStat {
	return NetworkStat{
		ReadCount:    self.ReadFlow.Changes(),
		ReadBytes:    self.ReadBytesFlow.Changes(),
		DispatcherGo: self.DispatcherGo.Count(),
		WriteCount:   self.WriteFlow.Changes(),
		WriteBytes:   self.WriteBytesFlow.Changes(),
		Connections:  self.Connections.Count()}
}

type Flow struct {
	count     int64
	lastcount int64
}

func (self *Flow) Incr(num int32) {
	atomic.AddInt64(&self.count, int64(num))
}

func (self *Flow) Count() int32 {
	return int32(atomic.LoadInt64(&self.count))
}

func (self *Flow) Changes() int32 {
	tmpc := atomic.LoadInt64(&self.count)
	tmpl := self.lastcount
	c := tmpc - tmpl
	self.lastcount = tmpc
	return int32(c)
}
