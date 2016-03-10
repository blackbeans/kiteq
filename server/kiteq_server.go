package server

import (
	"fmt"
	"github.com/blackbeans/kiteq-common/binding"
	"github.com/blackbeans/kiteq-common/stat"
	"github.com/blackbeans/kiteq-common/store"
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo/client"
	"github.com/blackbeans/turbo/packet"
	"github.com/blackbeans/turbo/pipe"
	"github.com/blackbeans/turbo/server"
	"kiteq/handler"
	"net"
	"net/http"
	"os"
	"strconv"
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
	kitedb         store.IKiteStore
	stop           bool
}

//握手包
func handshake(ga *client.GroupAuth, remoteClient *client.RemotingClient) (bool, error) {
	return false, nil
}

func NewKiteQServer(kc KiteQConfig) *KiteQServer {

	kiteqName, _ := os.Hostname()
	kitedb := parseDB(kc, kiteqName)
	kitedb.Start()

	//重连管理器
	reconnManager := client.NewReconnectManager(false, -1, -1, handshake)

	//客户端连接管理器
	clientManager := client.NewClientManager(reconnManager)

	// 临时在这里创建的BindExchanger
	exchanger := binding.NewBindExchanger(kc.so.zkhosts, kc.so.bindHost)

	//创建消息投递注册器
	registry := stat.NewDeliveryRegistry(10 * 10000)

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
	pipeline.RegisteHandler("check_message", handler.NewCheckMessageHandler("check_message", kc.so.topics))
	pipeline.RegisteHandler("persistent", handler.NewPersistentHandler("persistent", kc.so.deliveryTimeout, kitedb, kc.so.deliveryFirst, kc.flowstat))
	pipeline.RegisteHandler("txAck", handler.NewTxAckHandler("txAck", kitedb))
	pipeline.RegisteHandler("deliverpre", handler.NewDeliverPreHandler("deliverpre", kitedb, exchanger, kc.flowstat, kc.so.maxDeliverWorkers))
	pipeline.RegisteHandler("deliver", handler.NewDeliverHandler("deliver", registry))
	pipeline.RegisteHandler("remoting", pipe.NewRemotingHandler("remoting", clientManager))
	pipeline.RegisteHandler("remote-future", handler.NewRemotingFutureHandler("remote-future"))
	pipeline.RegisteHandler("deliverResult", handler.NewDeliverResultHandler("deliverResult", kc.so.deliveryTimeout, kitedb, rw, registry))
	//以下是处理投递结果返回事件，即到了remoting端会backwark到future-->result-->record

	recoverManager := NewRecoverManager(kiteqName, kc.so.recoverPeriod, pipeline, kitedb)

	return &KiteQServer{
		reconnManager:  reconnManager,
		clientManager:  clientManager,
		exchanger:      exchanger,
		pipeline:       pipeline,
		recoverManager: recoverManager,
		kc:             kc,
		kitedb:         kitedb,
		stop:           false}

}

func (self *KiteQServer) Start() {

	self.remotingServer = server.NewRemotionServer(self.kc.so.bindHost, self.kc.rc,
		func(rclient *client.RemotingClient, p *packet.Packet) {
			event := pipe.NewPacketEvent(rclient, p)
			err := self.pipeline.FireWork(event)
			if nil != err {
				log.ErrorLog("kite_server", "RemotingServer|onPacketRecieve|FAIL|%s", err)
			} else {
				// log.Debug("RemotingServer|onPacketRecieve|SUCC|%s|%t\n", rclient.RemoteAddr(), packet)
			}
		})

	err := self.remotingServer.ListenAndServer()
	if nil != err {
		log.Crashf("KiteQServer|RemotionServer|START|FAIL|%s|%s\n", err, self.kc.so.bindHost)
	} else {
		log.InfoLog("kite_server", "KiteQServer|RemotionServer|START|SUCC|%s\n", self.kc.so.bindHost)
	}
	//推送可发送的topic列表并且获取了对应topic下的订阅关系
	succ := self.exchanger.PushQServer(self.kc.so.bindHost, self.kc.so.topics)
	if !succ {
		log.Crashf("KiteQServer|PushQServer|FAIL|%s|%s\n", err, self.kc.so.topics)
	} else {
		log.InfoLog("kite_server", "KiteQServer|PushQServer|SUCC|%s\n", self.kc.so.topics)
	}

	//开启流量统计
	self.startFlow()

	//开启recover
	self.recoverManager.Start()

	//启动DLQ的时间
	self.startDLQ()

	//检查配置更新
	if len(self.kc.so.configPath) > 0 {
		self.startCheckConf()
	}

	//启动pprof
	host, _, _ := net.SplitHostPort(self.kc.so.bindHost)
	go func() {
		if self.kc.so.pprofPort > 0 {
			http.HandleFunc("/stat", self.HandleStat)
			http.HandleFunc("/binds", self.HandleBindings)
			log.Error(http.ListenAndServe(host+":"+strconv.Itoa(self.kc.so.pprofPort), nil))
		}
	}()
}

func (self *KiteQServer) startDLQ() {
	go func() {
		for {
			now := time.Now()
			next := now.Add(time.Hour * 24)
			next = time.Date(next.Year(), next.Month(), next.Day(), self.kc.so.dlqExecHour, 0, 0, 0, next.Location())
			t := time.NewTimer(next.Sub(now))
			<-t.C
			func() {
				defer func() {
					if err := recover(); nil != err {
						log.ErrorLog("kite_server", "KiteQServer|startDLQ|FAIL|%s|%s", err, time.Now())
					}
				}()
				//开始做迁移
				self.kitedb.MoveExpired()
			}()
			log.InfoLog("kite_server", "KiteQServer|startDLQ|SUCC|%s", time.Now())
		}
	}()
	log.InfoLog("kite_server", "KiteQServer|startDLQ|SUCC|%s", time.Now())
}

func (self *KiteQServer) startFlow() {

	go func() {
		t := time.NewTicker(1 * time.Second)
		for !self.stop {
			ns := self.remotingServer.NetworkStat()
			line := fmt.Sprintf("\nRemoting: \tread:%d/%d\twrite:%d/%d\tdispatcher_go:%d\tconnetions:%d\n", ns.ReadBytes, ns.ReadCount,
				ns.WriteBytes, ns.WriteCount, ns.DispatcherGo, self.clientManager.ConnNum())

			line = fmt.Sprintf("%sKiteQ:\tdeliver:%d\tdeliver-go:%d", line, self.kc.flowstat.DeliverFlow.Changes(),
				self.kc.flowstat.DeliverGo.Count())
			if nil != self.kitedb {
				line = fmt.Sprintf("%s\nKiteStore:%s", line, self.kitedb.Monitor())

			}
			log.InfoLog("kite_server", line)
			<-t.C
		}
		t.Stop()
	}()
}

func (self *KiteQServer) startCheckConf() {
	go func() {
		t := time.NewTicker(1 * time.Minute)
		for !self.stop {
			so := ServerOption{}
			err := loadTomlConf(self.kc.so.configPath, self.kc.so.clusterName,
				self.kc.so.bindHost, self.kc.so.pprofPort, &so)
			if nil != err {
				log.ErrorLog("kite_server", "KiteQServer|startCheckConf|FAIL|%s", err)
			}

			//新增或者减少topics
			if len(so.topics) != len(self.kc.so.topics) {
				//推送可发送的topic列表并且获取了对应topic下的订阅关系
				succ := self.exchanger.PushQServer(self.kc.so.bindHost, so.topics)
				if !succ {
					log.ErrorLog("kite_server", "KiteQServer|startCheckConf|PushQServer|FAIL|%s|%s\n", err, so.topics)
				} else {
					log.InfoLog("kite_server", "KiteQServer|startCheckConf|PushQServer|SUCC|%s\n", so.topics)
				}
				//重置数据
				self.kc.so = so
			}

			<-t.C
		}
		t.Stop()
	}()
}

func (self *KiteQServer) Shutdown() {
	self.stop = true
	//先关闭exchanger让客户端不要再输送数据
	self.exchanger.Shutdown()
	self.recoverManager.Stop()
	self.kitedb.Stop()
	self.clientManager.Shutdown()
	self.remotingServer.Shutdown()
	log.InfoLog("kite_server", "KiteQServer|Shutdown...")

}
