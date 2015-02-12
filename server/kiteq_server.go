package server

import (
	"kiteq/binding"
	"kiteq/handler"
	"kiteq/pipe"
	"kiteq/remoting/client"
	"kiteq/remoting/server"
	"kiteq/stat"
	"kiteq/store"
	"log"
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
	flowControl    *stat.FlowControl
}

//握手包
func handshake(ga *client.GroupAuth, remoteClient *client.RemotingClient) (bool, error) {
	return false, nil
}

func NewKiteQServer(local, zkhost string, topics []string, mysql string) *KiteQServer {
	var kitedb store.IKiteStore
	if mysql == "mock" {
		kitedb = &store.MockKiteStore{}
	} else {
		kitedb = store.NewKiteMysql(mysql)
	}

	flowControl := stat.NewFlowControl("KiteQ")
	//重连管理器
	reconnManager := client.NewReconnectManager(false, -1, -1, handshake)

	clientManager := client.NewClientManager(reconnManager)
	// 临时在这里创建的BindExchanger
	exchanger := binding.NewBindExchanger(zkhost)

	//初始化pipeline
	pipeline := pipe.NewDefaultPipeline()
	pipeline.RegisteHandler("packet", handler.NewPacketHandler("packet", flowControl))
	pipeline.RegisteHandler("access", handler.NewAccessHandler("access", clientManager))
	pipeline.RegisteHandler("accept", handler.NewAcceptHandler("accept"))
	pipeline.RegisteHandler("heartbeat", handler.NewHeartbeatHandler("heartbeat"))
	pipeline.RegisteHandler("persistent", handler.NewPersistentHandler("persistent", kitedb))
	pipeline.RegisteHandler("txAck", handler.NewTxAckHandler("txAck", kitedb))
	pipeline.RegisteHandler("deliverpre", handler.NewDeliverPreHandler("deliverpre", kitedb, exchanger))
	pipeline.RegisteHandler("deliver", handler.NewDeliverHandler("deliver"))
	pipeline.RegisteHandler("remote-result", handler.NewRemoteResultHandler("remote-result"))
	pipeline.RegisteHandler("remoting", pipe.NewRemotingHandler("remoting", clientManager, flowControl))
	pipeline.RegisteHandler("deliverResult", handler.NewDeliverResultHandler("deliverResult", kitedb, 100*time.Millisecond))
	pipeline.RegisteHandler("resultRecord", handler.NewResultRecordHandler("resultRecord", kitedb))

	return &KiteQServer{
		local:         local,
		topics:        topics,
		reconnManager: reconnManager,
		clientManager: clientManager,
		exchanger:     exchanger,
		pipeline:      pipeline,
		flowControl:   flowControl}

}

func (self *KiteQServer) Start() {

	self.remotingServer = server.NewRemotionServer(self.local, 3*time.Second, self.flowControl,
		func(rclient *client.RemotingClient, packet []byte) {
			self.flowControl.DispatcherFlow.Incr(1)
			event := pipe.NewPacketEvent(rclient, packet)
			err := self.pipeline.FireWork(event)
			if nil != err {
				log.Printf("RemotingServer|onPacketRecieve|FAIL|%s|%t\n", err, packet)
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

	self.reconnManager.Start()

}

func (self *KiteQServer) Shutdown() {
	self.clientManager.Shutdown()
	self.remotingServer.Shutdown()
	self.exchanger.Shutdown()
}
