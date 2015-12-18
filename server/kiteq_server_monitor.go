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
	MessageCount map[string]int                `json:"message_count"`
	Topics       map[string] /*topicId*/ int32 `json:"topics"`
}

//handler monitor
func (self *KiteQServer) HandleStat(resp http.ResponseWriter, req *http.Request) {

	//network
	rstat := self.remotingServer.NetworkStat()
	rstat.Connections = self.clientManager.ConnNum()

	//统计topic的数量消息
	topics := make(map[string]int32, 20)
	for topic, f := range self.kc.flowstat.TopicsFlows {
		topics[topic] = f.Changes()
	}
	msgMap := self.kitedb.Length()
	for _, t := range self.kc.topics {
		_, ok := msgMap[t]
		if !ok {
			msgMap[t] = 0
		}

		_, ok = topics[t]
		if !ok {
			topics[t] = 0
		}
	}

	//kiteq
	ks := kiteqstat{
		Goroutine:    int32(runtime.NumGoroutine()),
		DeliverGo:    self.kc.flowstat.DeliverFlow.Changes(),
		DeliverCount: self.kc.flowstat.DeliverGo.Count(),
		MessageCount: msgMap,
		Topics:       topics}

	result := make(map[string]interface{}, 2)
	result["kiteq"] = ks
	result["network"] = rstat

	data, _ := json.Marshal(result)

	//write monitor
	resp.Write(data)
}

//handler monitor
func (self *KiteQServer) HandleBindings(resp http.ResponseWriter, req *http.Request) {

	binds := self.exchanger.Topic2Groups()
	data, _ := json.Marshal(binds)

	//write monitor
	resp.Write(data)
}
