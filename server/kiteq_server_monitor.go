package server

import (
	"encoding/json"
	"net/http"
	"runtime"
)

type kiteqstat struct {
	Goroutine    int32                         `json:"goroutine"`
	DeliverGo    int32                         `json:"deliver_go"`
	DeliverCount int32                         `json:"deliver_count"`
	MessageCount int32                         `json:"message_count"`
	Topics       map[string] /*topicId*/ int32 //topics
}

//handler monitor
func (self *KiteQServer) HandleMonitor(resp http.ResponseWriter, req *http.Request) {

	//network
	rstat := self.remotingServer.NetworkStat()
	rstat.ConnectionCount = self.clientManager.ConnNum()

	//kiteq
	ks := kiteqstat{
		Goroutine:    int32(runtime.NumGoroutine()),
		DeliverGo:    self.kc.flowstat.DeliverPool.Count(),
		DeliverCount: self.kc.flowstat.DeliverFlow.Changes(),
		MessageCount: int32(self.kitedb.Length())}

	result := make(map[string]interface{}, 2)
	result["kiteq"] = ks
	result["network"] = rstat

	data, _ := json.Marshal(result)

	//write monitor
	resp.Write(data)
}
