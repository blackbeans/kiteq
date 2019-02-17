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
	Goroutine        int32                         `json:"goroutine"`
	DeliverGo        int32                         `json:"deliver_go"`
	DeliverCount     int32                         `json:"deliver_count"`
	RecieveCount     int32                         `json:"recieve_count"`
	MessageCount     map[string]int                `json:"message_count"`
	TopicsDeliver    map[string] /*topicId*/ int32 `json:"topics_deliver"`
	TopicsRecieve    map[string] /*topicId*/ int32 `json:"topics_recieve"`
	Groups           map[string][]string           `json:"groups"`
	KiteServerLimter []int                         `json:"accept_limiter"`
}

// HandleStat: handler monitor
func (self *KiteQServer) HandleStat(resp http.ResponseWriter, req *http.Request) {

	defer func() {
		if err := recover(); nil != err {
			//do nothing
		}
	}()

	idx := (time.Now().Unix() - 1) % 2

	//network
	rstat := self.lastNetstat[idx]
	rstat.Connections = self.clientManager.ConnNum()

	//统计topic的数量消息
	//kiteq
	ks := self.lastKiteStat[idx]
	ks.Groups = self.clientManager.CloneGroups()

	result := make(map[string]interface{}, 2)
	result["kiteq"] = ks
	result["network"] = rstat

	data, _ := json.Marshal(result)

	//write monitor
	resp.Write(data)
}

type BindInfo struct {
	Topic2Groups    map[string][]string         `json:"topic_2_groups"`
	Topics2Limiters map[string]map[string][]int `json:"topic_limiters"`
}

// HandleBindings: handler monitor
func (self *KiteQServer) HandleBindings(resp http.ResponseWriter, req *http.Request) {

	binds := self.exchanger.Topic2Groups()
	limters := self.exchanger.Topic2Limiters()
	bi := BindInfo{
		Topic2Groups:    binds,
		Topics2Limiters: limters}
	data, _ := json.Marshal(bi)
	//write monitor
	resp.Write(data)
}

func (self *KiteQServer) startFlow() {

	go func() {
		t := time.NewTicker(1 * time.Second)
		count := 0
		for !self.stop {

			ns := self.remotingServer.NetworkStat()

			//统计topic的数量消息
			topicsdeliver, topicsrecieve := self.kc.flowstat.TopicFlowSnapshot()

			//消息堆积数量
			var msgMap map[string]int
			if nil != self.kitedb {
				msgMap = self.kitedb.Length()
				if nil == msgMap {
					msgMap = make(map[string]int, 20)
				}
			}

			for _, t := range self.kc.so.topics {
				_, ok := msgMap[t]
				if !ok {
					msgMap[t] = 0
				}

				_, ok = topicsdeliver[t]
				if !ok {
					topicsdeliver[t] = 0
				}

				_, ok = topicsrecieve[t]
				if !ok {
					topicsrecieve[t] = 0
				}
			}

			used, total := self.limiter.LimiterInfo()
			//kiteq
			ks := kiteqstat{
				Goroutine:        int32(runtime.NumGoroutine()),
				DeliverGo:        self.kc.flowstat.DeliverGo.Count(),
				DeliverCount:     self.kc.flowstat.DeliverFlow.Changes(),
				RecieveCount:     self.kc.flowstat.RecieveFlow.Changes(),
				MessageCount:     msgMap,
				TopicsDeliver:    topicsdeliver,
				TopicsRecieve:    topicsrecieve,
				KiteServerLimter: []int{used, total}}

			line := fmt.Sprintf("\nRemoting: \tread:%d/%d\twrite:%d/%d\tdispatcher_go:%d\tconnetions:%d\n", ns.ReadBytes, ns.ReadCount,
				ns.WriteBytes, ns.WriteCount, ns.DispatcherGo, self.clientManager.ConnNum())

			line = fmt.Sprintf("%sKiteQ:\tdeliver:%d\tdeliver-go:%d", line, ks.DeliverCount,
				ks.DeliverGo)
			if nil != self.kitedb {
				line = fmt.Sprintf("%s\nKiteStore:%s", line, self.kitedb.Monitor())

			}
			log.InfoLog("kite_server", line)
			self.lastNetstat[count%2] = ns
			self.lastKiteStat[count%2] = ks
			count++
			<-t.C
		}
		t.Stop()
	}()
}
