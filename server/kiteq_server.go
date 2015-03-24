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
	smm "kiteq/store/mmap"
	smq "kiteq/store/mysql"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type KiteQServer struct {
	reconnManager  *client.ReconnectManager
	clientManager  *client.ClientManager
	exchanger      *binding.BindExchanger
	remotingServer *server.RemotingServer
	pipeline       *pipe.DefaultPipeline
	recoverManager *RecoverManager
	kc             KiteQConfig
}

//握手包
func handshake(ga *client.GroupAuth, remoteClient *client.RemotingClient) (bool, error) {
	return false, nil
}

func NewKiteQServer(kc KiteQConfig) *KiteQServer {

	kitedb := parseDB(kc.db)

	kiteqName, _ := os.Hostname()

	kc.rc.FlowStat = stat.NewFlowStat("KiteQ-" + kc.server)

	//重连管理器
	reconnManager := client.NewReconnectManager(false, -1, -1, handshake)

	//客户端连接管理器
	clientManager := client.NewClientManager(reconnManager)

	// 临时在这里创建的BindExchanger
	exchanger := binding.NewBindExchanger(kc.zkhost, kc.server)

	//重投策略
	rw := make([]handler.RedeliveryWindow, 0, 10)
	rw = append(rw, handler.NewRedeliveryWindow(0, 3, 5*3600))
	rw = append(rw, handler.NewRedeliveryWindow(3, 10, 10*3600))
	rw = append(rw, handler.NewRedeliveryWindow(10, 20, 30*3600))
	rw = append(rw, handler.NewRedeliveryWindow(20, -1, 24*60*3600))

	//初始化pipeline
	pipeline := pipe.NewDefaultPipeline()
	pipeline.RegisteHandler("packet", handler.NewPacketHandler("packet"))
	pipeline.RegisteHandler("access", handler.NewAccessHandler("access", clientManager))
	pipeline.RegisteHandler("validate", handler.NewValidateHandler("validate", clientManager))
	pipeline.RegisteHandler("accept", handler.NewAcceptHandler("accept"))
	pipeline.RegisteHandler("heartbeat", handler.NewHeartbeatHandler("heartbeat"))
	pipeline.RegisteHandler("check_message", handler.NewCheckMessageHandler("check_message", kc.topics))
	pipeline.RegisteHandler("persistent", handler.NewPersistentHandler("persistent", kc.rc.FlowStat, kc.maxDeliverWorkers, kitedb))
	pipeline.RegisteHandler("txAck", handler.NewTxAckHandler("txAck", kitedb))
	pipeline.RegisteHandler("deliverpre", handler.NewDeliverPreHandler("deliverpre", kitedb, exchanger))
	pipeline.RegisteHandler("deliver", handler.NewDeliverHandler("deliver"))
	pipeline.RegisteHandler("remoting", pipe.NewRemotingHandler("remoting", clientManager))
	pipeline.RegisteHandler("remote-future", handler.NewRemotingFutureHandler("remote-future"))
	pipeline.RegisteHandler("deliverResult", handler.NewDeliverResultHandler("deliverResult", 1*time.Second, kitedb, rw))
	//以下是处理投递结果返回事件，即到了remoting端会backwark到future-->result-->record

	recoverManager := NewRecoverManager(kiteqName, kc.recoverPeriod, pipeline, kitedb)

	return &KiteQServer{
		reconnManager:  reconnManager,
		clientManager:  clientManager,
		exchanger:      exchanger,
		pipeline:       pipeline,
		recoverManager: recoverManager,
		kc:             kc}

}

func parseDB(db string) store.IKiteStore {
	var kitedb store.IKiteStore
	if strings.HasPrefix(db, "mock://") {
		kitedb = &store.MockKiteStore{}
	} else if strings.HasPrefix(db, "mmap://") {
		url := strings.TrimPrefix(db, "mmap://")
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
		kitedb = smm.NewKiteMMapStore(file, initval, max)
	} else if strings.HasPrefix(db, "mysql://") {
		url := strings.TrimPrefix(db, "mysql://")
		mp := strings.Split(url, "?")
		params := make(map[string]string, 5)
		if len(mp) > 1 {
			split := strings.Split(mp[1], "&")
			for _, v := range split[1:] {
				p := strings.SplitN(v, "=", 2)
				params[p[0]] = p[1]
			}
		}

		bus := 1000
		u, ok := params["batchUpdateSize"]
		if ok {
			v, e := strconv.ParseInt(u, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|batchUpdateSize|%s\n", db)
			}
			bus = int(v)
		}

		bds := 1000
		d, ok := params["batchDelSize"]
		if ok {
			v, e := strconv.ParseInt(d, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|batchDelSize|%s\n", db)
			}
			bds = int(v)
		}

		flushPeriod := 1 * time.Second
		fp, ok := params["flushPeriod"]
		if ok {
			v, e := strconv.ParseInt(fp, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|batchDelSize|%s\n", db)
			}
			flushPeriod = time.Duration(v * int64(1*time.Millisecond))
		}

		options := smq.MysqlOptions{
			Addr:         mp[0],
			BatchUpSize:  bus,
			BatchDelSize: bds,
			FlushPeriod:  flushPeriod,
			MaxIdleConn:  1024,
			MaxOpenConn:  1024}
		kitedb = smq.NewKiteMysql(options)
	} else {
		log.Fatalf("NewKiteQServer|UNSUPPORT DB PROTOCOL|%s\n", db)
	}

	return kitedb
}

func (self *KiteQServer) Start() {

	self.remotingServer = server.NewRemotionServer(self.kc.server, self.kc.rc,
		func(rclient *client.RemotingClient, packet *protocol.Packet) {
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
		log.Fatalf("KiteQServer|RemotionServer|START|FAIL|%s|%s\n", err, self.kc.server)
	} else {
		log.Printf("KiteQServer|RemotionServer|START|SUCC|%s\n", self.kc.server)
	}
	//推送可发送的topic列表并且获取了对应topic下的订阅关系
	succ := self.exchanger.PushQServer(self.kc.server, self.kc.topics)
	if !succ {
		log.Fatalf("KiteQServer|PushQServer|FAIL|%s|%s\n", err, self.kc.topics)
	} else {
		log.Printf("KiteQServer|PushQServer|SUCC|%s\n", self.kc.topics)
	}

	//开启流量统计
	self.kc.rc.FlowStat.Start()

	//开启recover
	self.recoverManager.Start()

}

func (self *KiteQServer) Shutdown() {
	self.clientManager.Shutdown()
	self.remotingServer.Shutdown()
	self.exchanger.Shutdown()
}
