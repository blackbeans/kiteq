package server

import (
	"encoding/json"
	"fmt"
	log "github.com/blackbeans/log4go"
	"net/http"
	"runtime"
	"time"
)

type kiteqstat struct {
	Goroutine    int32                         `json:"goroutine"`
	DeliverGo    int32                         `json:"deliver_go"`
	DeliverCount int32                         `json:"deliver_count"`
	MessageCount map[string]int                `json:"message_count"`
	Topics       map[string] /*topicId*/ int32 `json:"topics"`
	Groups       map[string][]string           `json:"groups"`
}

//handler monitor
func (self *KiteQServer) HandleStat(resp http.ResponseWriter, req *http.Request) {

	//network
	rstat := self.lastNetstat
	rstat.Connections = self.clientManager.ConnNum()

	//统计topic的数量消息
	//kiteq
	ks := *self.lastKiteStat
	ks.Groups = self.clientManager.CloneGroups()

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

func (self *KiteQServer) startFlow() {

	go func() {
		t := time.NewTicker(1 * time.Second)
		for !self.stop {

			ns := self.remotingServer.NetworkStat()
			self.lastNetstat = &ns

			//统计topic的数量消息
			topics := make(map[string]int32, 20)
			for topic, f := range self.kc.flowstat.TopicsFlows {
				topics[topic] = f.Changes()
			}

			//消息堆积数量
			msgMap := make(map[string]int, 20)
			if nil != self.kitedb {
				msgMap = self.kitedb.Length()
			}

			for _, t := range self.kc.so.topics {
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
			self.lastKiteStat = &kiteqstat{
				Goroutine:    int32(runtime.NumGoroutine()),
				DeliverGo:    self.kc.flowstat.DeliverFlow.Changes(),
				DeliverCount: self.kc.flowstat.DeliverGo.Count(),
				MessageCount: msgMap,
				Topics:       topics}

			line := fmt.Sprintf("\nRemoting: \tread:%d/%d\twrite:%d/%d\tdispatcher_go:%d\tconnetions:%d\n", ns.ReadBytes, ns.ReadCount,
				ns.WriteBytes, ns.WriteCount, ns.DispatcherGo, self.clientManager.ConnNum())

			line = fmt.Sprintf("%sKiteQ:\tdeliver:%d\tdeliver-go:%d", line, self.lastKiteStat.DeliverCount,
				self.lastKiteStat.DeliverGo)
			if nil != self.kitedb {
				line = fmt.Sprintf("%s\nKiteStore:%s", line, self.kitedb.Monitor())

			}
			log.InfoLog("kite_server", line)
			<-t.C
		}
		t.Stop()
	}()
}
