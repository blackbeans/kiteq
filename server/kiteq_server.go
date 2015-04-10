package server

import (
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo/client"
	"github.com/blackbeans/turbo/packet"
	"github.com/blackbeans/turbo/pipe"
	"github.com/blackbeans/turbo/server"
	"kiteq/binding"
	"kiteq/handler"
	"kiteq/store"
	"os"
)

type KiteQServer struct {
	reconnManager  *client.ReconnectManager
	clientManager  *client.ClientManager
	exchanger      *binding.BindExchanger
	remotingServer *server.RemotingServer
	pipeline       *pipe.DefaultPipeline
	recoverManager *RecoverManager
	kc             KiteQConfig
	kitedb         store.IKiteStore
}

//握手包
func handshake(ga *client.GroupAuth, remoteClient *client.RemotingClient) (bool, error) {
	return false, nil
}

func NewKiteQServer(kc KiteQConfig) *KiteQServer {

	kitedb := parseDB(kc)

	kiteqName, _ := os.Hostname()

	//重连管理器
	reconnManager := client.NewReconnectManager(false, -1, -1, handshake)

	//客户端连接管理器
	clientManager := client.NewClientManager(reconnManager)

	// 临时在这里创建的BindExchanger
	exchanger := binding.NewBindExchanger(kc.zkhost, kc.server)

	//重投策略
	rw := make([]handler.RedeliveryWindow, 0, 10)
	rw = append(rw, handler.NewRedeliveryWindow(0, 3, 30))
	rw = append(rw, handler.NewRedeliveryWindow(4, 10, 2*60))
	rw = append(rw, handler.NewRedeliveryWindow(10, 20, 4*60))
	rw = append(rw, handler.NewRedeliveryWindow(20, 30, 8*60))
	rw = append(rw, handler.NewRedeliveryWindow(30, 40, 16*60))
	rw = append(rw, handler.NewRedeliveryWindow(40, 50, 32*60))
	rw = append(rw, handler.NewRedeliveryWindow(50, -1, 60*60))

	//初始化pipeline
	pipeline := pipe.NewDefaultPipeline()
	pipeline.RegisteHandler("packet", handler.NewPacketHandler("packet"))
	pipeline.RegisteHandler("access", handler.NewAccessHandler("access", clientManager))
	pipeline.RegisteHandler("validate", handler.NewValidateHandler("validate", clientManager))
	pipeline.RegisteHandler("accept", handler.NewAcceptHandler("accept"))
	pipeline.RegisteHandler("heartbeat", handler.NewHeartbeatHandler("heartbeat"))
	pipeline.RegisteHandler("check_message", handler.NewCheckMessageHandler("check_message", kc.topics))
	pipeline.RegisteHandler("persistent", handler.NewPersistentHandler("persistent", kc.deliverTimeout, kitedb, kc.fly, kc.flowstat))
	pipeline.RegisteHandler("txAck", handler.NewTxAckHandler("txAck", kitedb))
	pipeline.RegisteHandler("deliverpre", handler.NewDeliverPreHandler("deliverpre", kitedb, exchanger, kc.flowstat, kc.maxDeliverWorkers))
	pipeline.RegisteHandler("deliver", handler.NewDeliverHandler("deliver"))
	pipeline.RegisteHandler("remoting", pipe.NewRemotingHandler("remoting", clientManager))
	pipeline.RegisteHandler("remote-future", handler.NewRemotingFutureHandler("remote-future"))
	pipeline.RegisteHandler("deliverResult", handler.NewDeliverResultHandler("deliverResult", kc.deliverTimeout, kitedb, rw))
	//以下是处理投递结果返回事件，即到了remoting端会backwark到future-->result-->record

	recoverManager := NewRecoverManager(kiteqName, kc.recoverPeriod, pipeline, kitedb)

	return &KiteQServer{
		reconnManager:  reconnManager,
		clientManager:  clientManager,
		exchanger:      exchanger,
		pipeline:       pipeline,
		recoverManager: recoverManager,
		kc:             kc,
		kitedb:         kitedb}

}

func (self *KiteQServer) Start() {

	self.remotingServer = server.NewRemotionServer(self.kc.server, self.kc.rc,
		func(rclient *client.RemotingClient, p *packet.Packet) {
			event := pipe.NewPacketEvent(rclient, p)
			err := self.pipeline.FireWork(event)
			if nil != err {
				log.Error("RemotingServer|onPacketRecieve|FAIL|%s|%t\n", err, p)
			} else {
				// log.Debug("RemotingServer|onPacketRecieve|SUCC|%s|%t\n", rclient.RemoteAddr(), packet)
			}
		})

	err := self.remotingServer.ListenAndServer()
	if nil != err {
		log.Crashf("KiteQServer|RemotionServer|START|FAIL|%s|%s\n", err, self.kc.server)
	} else {
		log.Info("KiteQServer|RemotionServer|START|SUCC|%s\n", self.kc.server)
	}
	//推送可发送的topic列表并且获取了对应topic下的订阅关系
	succ := self.exchanger.PushQServer(self.kc.server, self.kc.topics)
	if !succ {
		log.Crashf("KiteQServer|PushQServer|FAIL|%s|%s\n", err, self.kc.topics)
	} else {
		log.Info("KiteQServer|PushQServer|SUCC|%s\n", self.kc.topics)
	}

	//开启流量统计
	self.kc.flowstat.Start()

	//开启recover
	self.recoverManager.Start()

}

func (self *KiteQServer) Shutdown() {
	//先关闭exchanger让客户端不要再输送数据
	self.exchanger.Shutdown()
	self.recoverManager.Stop()
	self.kitedb.Stop()
	self.clientManager.Shutdown()
	self.remotingServer.Shutdown()
	log.Info("KiteQServer|Shutdown...")

}
