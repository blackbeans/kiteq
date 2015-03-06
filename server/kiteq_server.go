package server

import (
	"kiteq/binding"
	"kiteq/handler"
	"kiteq/pipe"
	"kiteq/protocol"
	"kiteq/remoting/client"
	"kiteq/remoting/server"
	"kiteq/stat"
	"kiteq/store"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type KiteQServer struct {
	local          string
	topics         []string
	reconnManager  *client.ReconnectManager
	clientManager  *client.ClientManager
	exchanger      *binding.BindExchanger
	remotingServer *server.RemotingServer
	pipeline       *pipe.DefaultPipeline
	recoverManager *RecoverManager
	flowControl    *stat.FlowControl
	rc             *protocol.RemotingConfig
}

//握手包
func handshake(ga *client.GroupAuth, remoteClient *client.RemotingClient) (bool, error) {
	return false, nil
}

func NewKiteQServer(local, zkhost string, rc *protocol.RemotingConfig, topics []string, db string) *KiteQServer {

	kitedb := parseDB(db)

	recoverPeriod := 1 * time.Minute
	kiteqName, _ := os.Hostname()

	flowControl := stat.NewFlowControl("KiteQ")
	//重连管理器
	reconnManager := client.NewReconnectManager(false, -1, -1, handshake)

	//客户端连接管理器
	clientManager := client.NewClientManager(reconnManager)

	// 临时在这里创建的BindExchanger
	exchanger := binding.NewBindExchanger(zkhost, local)

	//重投策略
	rw := make([]handler.RedeliveryWindow, 0, 10)
	rw = append(rw, handler.NewRedeliveryWindow(0, 3, 5*3600))
	rw = append(rw, handler.NewRedeliveryWindow(3, 10, 10*3600))
	rw = append(rw, handler.NewRedeliveryWindow(10, 20, 30*3600))
	rw = append(rw, handler.NewRedeliveryWindow(20, -1, 24*60*3600))

	//初始化pipeline
	pipeline := pipe.NewDefaultPipeline()
	pipeline.RegisteHandler("packet", handler.NewPacketHandler("packet", flowControl))
	pipeline.RegisteHandler("access", handler.NewAccessHandler("access", clientManager))
	pipeline.RegisteHandler("validate", handler.NewValidateHandler("validate", clientManager))
	pipeline.RegisteHandler("accept", handler.NewAcceptHandler("accept"))
	pipeline.RegisteHandler("heartbeat", handler.NewHeartbeatHandler("heartbeat"))
	pipeline.RegisteHandler("persistent", handler.NewPersistentHandler("persistent", kitedb))
	pipeline.RegisteHandler("txAck", handler.NewTxAckHandler("txAck", kitedb))
	pipeline.RegisteHandler("deliverpre", handler.NewDeliverPreHandler("deliverpre", kitedb, exchanger))
	pipeline.RegisteHandler("deliver", handler.NewDeliverHandler("deliver"))
	pipeline.RegisteHandler("remoting", pipe.NewRemotingHandler("remoting", clientManager, flowControl))
	pipeline.RegisteHandler("remote-future", handler.NewRemotingFutureHandler("remote-future"))
	pipeline.RegisteHandler("deliverResult", handler.NewDeliverResultHandler("deliverResult", 1*time.Second, kitedb, rw))
	//以下是处理投递结果返回事件，即到了remoting端会backwark到future-->result-->record

	recoverManager := NewRecoverManager(kiteqName, recoverPeriod, pipeline, kitedb)

	return &KiteQServer{
		local:          local,
		topics:         topics,
		reconnManager:  reconnManager,
		clientManager:  clientManager,
		exchanger:      exchanger,
		pipeline:       pipeline,
		flowControl:    flowControl,
		recoverManager: recoverManager,
		rc:             rc}

}

func parseDB(db string) store.IKiteStore {
	var kitedb store.IKiteStore
	if strings.HasPrefix(db, "mock://") {
		kitedb = &store.MockKiteStore{}
	} else if strings.HasPrefix(db, "mmap://") {
		url := strings.TrimLeft(db, "mmap://")
		split := strings.Split(url, "&")
		params := make(map[string]string, len(split))
		for _, v := range split {
			p := strings.SplitN(v, "=", 2)
			params[p[0]] = p[1]
		}

		file := params["file"]
		if len(file) <= 0 {
			log.Fatalf("NewKiteQServer|INVALID|FILE PATH|%s\n", db)
		}
		initval := 10 * 10000
		initcap, ok := params["initcap"]
		if ok {
			v, e := strconv.ParseInt(initcap, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|INIT CAP|%s\n", db)
			}
			initval = int(v)
		}
		max := 50 * 10000
		maxcap, ok := params["maxcap"]
		if ok {
			v, e := strconv.ParseInt(maxcap, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|MAX CAP|%s\n", db)
			}
			max = int(v)
		}
		kitedb = store.NewKiteMMapStore(file, initval, max)
	} else if strings.HasPrefix(db, "mysql://") {
		mysql := strings.TrimLeft(db, "mysql://")
		kitedb = store.NewKiteMysql(mysql)
	} else {
		log.Fatalf("NewKiteQServer|UNSUPPORT DB PROTOCOL|%s\n", db)
	}

	return kitedb
}

func (self *KiteQServer) Start() {

	self.remotingServer = server.NewRemotionServer(self.local, self.flowControl, self.rc,
		func(rclient *client.RemotingClient, packet *protocol.Packet) {
			self.flowControl.DispatcherFlow.Incr(1)
			event := pipe.NewPacketEvent(rclient, packet)
			err := self.pipeline.FireWork(event)
			if nil != err {
				log.Printf("RemotingServer|onPacketRecieve|FAIL|%s|%t\n", err, packet)
			} else {
				// log.Printf("RemotingServer|onPacketRecieve|SUCC|%s|%t\n", rclient.RemoteAddr(), packet)
			}
		})

	err := self.remotingServer.ListenAndServer()
	if nil != err {
		log.Fatalf("KiteQServer|RemotionServer|START|FAIL|%s|%s\n", err, self.local)
	} else {
		log.Printf("KiteQServer|RemotionServer|START|SUCC|%s\n", self.local)
	}
	//推送可发送的topic列表并且获取了对应topic下的订阅关系
	succ := self.exchanger.PushQServer(self.local, self.topics)
	if !succ {
		log.Fatalf("KiteQServer|PushQServer|FAIL|%s|%s\n", err, self.topics)
	} else {
		log.Printf("KiteQServer|PushQServer|SUCC|%s\n", self.topics)
	}
	//开启recover
	self.recoverManager.Start()

}

func (self *KiteQServer) Shutdown() {
	self.clientManager.Shutdown()
	self.remotingServer.Shutdown()
	self.exchanger.Shutdown()
}
