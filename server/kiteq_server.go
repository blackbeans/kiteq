package server

import (
	"context"
	"encoding/json"
	"kiteq/handler"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"kiteq/exchange"
	"kiteq/store"
	"kiteq/store/parser"

	"github.com/blackbeans/kiteq-common/protocol"
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"
)

type KiteQServer struct {
	reconnManager  *turbo.ReconnectManager
	clientManager  *turbo.ClientManager
	exchanger      *exchange.BindExchanger
	remotingServer *turbo.TServer
	pipeline       *turbo.DefaultPipeline
	recoverManager *RecoverManager
	kc             KiteQConfig
	kitedb         store.IKiteStore
	stop           bool
	lastNetstat    []turbo.NetworkStat
	lastKiteStat   []kiteqstat
	limiter        *turbo.BurstyLimiter
	topicNotify    chan []string
}

//握手包
func handshake(ga *turbo.GroupAuth, remoteClient *turbo.TClient) (bool, error) {
	return false, nil
}

func NewKiteQServer(ctx context.Context, kc KiteQConfig) *KiteQServer {

	kiteqName, _ := os.Hostname()

	kitedb := parser.ParseDB(ctx, kc.so.db, kiteqName)
	//kc.flowstat.Kitestore = kitedb
	kitedb.Start()

	//重连管理器
	reconnManager := turbo.NewReconnectManager(false, -1, -1, handshake)

	//客户端连接管理器
	clientManager := turbo.NewClientManager(reconnManager)

	// 临时在这里创建的BindExchanger
	exchanger := exchange.NewBindExchanger(kc.so.registryUri, kc.so.bindHost)

	//创建消息投递注册器
	registry := handler.NewDeliveryRegistry(ctx, kc.rc.TW, 10*10000)

	//重投策略
	rw := make([]handler.RedeliveryWindow, 0, 10)
	rw = append(rw, handler.NewRedeliveryWindow(0, 3, 0))
	rw = append(rw, handler.NewRedeliveryWindow(4, 10, 5))
	rw = append(rw, handler.NewRedeliveryWindow(10, 20, 10))
	rw = append(rw, handler.NewRedeliveryWindow(20, 30, 2*10))
	rw = append(rw, handler.NewRedeliveryWindow(30, 40, 4*10))
	rw = append(rw, handler.NewRedeliveryWindow(40, 50, 8*10))
	rw = append(rw, handler.NewRedeliveryWindow(50, -1, 16*10))

	//创建KiteqServer的流控
	limiter, _ := turbo.NewBurstyLimiter(kc.so.recievePermitsPerSecond/2, kc.so.recievePermitsPerSecond)

	topicNotify := make(chan []string, 10)
	topicNotify <- kc.so.topics
	//初始化pipeline
	pipeline := turbo.NewDefaultPipeline()
	pipeline.RegisteHandler("packet", handler.NewPacketHandler("packet"))
	pipeline.RegisteHandler("access", handler.NewAccessHandler("access", clientManager))
	pipeline.RegisteHandler("validate", handler.NewValidateHandler("validate", clientManager))
	pipeline.RegisteHandler("accept", handler.NewAcceptHandler("accept", limiter, kc.flowstat))
	pipeline.RegisteHandler("heartbeat", handler.NewHeartbeatHandler("heartbeat"))
	pipeline.RegisteHandler("check_message", handler.NewCheckMessageHandler("check_message", topicNotify))
	pipeline.RegisteHandler("persistent", handler.NewPersistentHandler("persistent", kc.so.deliveryTimeout, kitedb, kc.so.deliveryFirst))
	pipeline.RegisteHandler("txAck", handler.NewTxAckHandler("txAck", kitedb))
	pipeline.RegisteHandler("deliverpre", handler.NewDeliverPreHandler("deliverpre", kitedb, exchanger, kc.flowstat, kc.so.maxDeliverWorkers, registry))
	pipeline.RegisteHandler("deliver", handler.NewDeliverQosHandler("deliver", kc.flowstat))
	pipeline.RegisteHandler("remoting", turbo.NewRemotingHandler("remoting", clientManager))
	pipeline.RegisteHandler("remote-future", handler.NewRemotingFutureHandler("remote-future"))
	pipeline.RegisteHandler("deliver-result", handler.NewDeliverResultHandler("deliver-result", kc.so.deliveryTimeout, kitedb, rw, registry))
	//以下是处理投递结果返回事件，即到了remoting端会backwark到future-->result-->record

	recoverManager := NewRecoverManager(kiteqName, kc.so.recoverPeriod, pipeline, kitedb, kc.rc.TW)

	return &KiteQServer{
		reconnManager:  reconnManager,
		clientManager:  clientManager,
		exchanger:      exchanger,
		pipeline:       pipeline,
		recoverManager: recoverManager,
		kc:             kc,
		kitedb:         kitedb,
		stop:           false,
		lastNetstat:    make([]turbo.NetworkStat, 2),
		lastKiteStat:   make([]kiteqstat, 2),
		limiter:        limiter,
		topicNotify:    topicNotify}

}

func (self *KiteQServer) Start() {

	codec := protocol.KiteQBytesCodec{MaxFrameLength: turbo.MAX_PACKET_BYTES}
	self.remotingServer = turbo.NewTServerWithCodec(self.kc.so.bindHost, self.kc.rc,
		func() turbo.ICodec {
			return codec
		},
		func(ctx *turbo.TContext) error {
			c := ctx.Client
			p := ctx.Message
			event := turbo.NewPacketEvent(c, p)
			err := self.pipeline.FireWork(event)
			if nil != err {
				log.ErrorLog("kite_server", "RemotingServer|onPacketRecieve|FAIL|%s", err)

			} else {
				// log.Debug("RemotingServer|onPacketRecieve|SUCC|%s|%t\n", rclient.RemoteAddr(), packet)
			}
			return err
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

	http.HandleFunc("/stat", self.HandleStat)
	http.HandleFunc("/binds", self.HandleBindings)
	http.HandleFunc("/reload", self.HandleReloadConf)
	//启动pprof
	host, _, _ := net.SplitHostPort(self.kc.so.bindHost)
	go func() {
		if self.kc.so.pprofPort > 0 {
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

//处理reload配置
func (self *KiteQServer) HandleReloadConf(resp http.ResponseWriter, req *http.Request) {
	so := ServerOption{}
	err := loadTomlConf(self.kc.so.configPath, self.kc.so.clusterName, self.kc.so.bindHost, self.kc.so.pprofPort, &so)
	if nil != err {
		log.ErrorLog("kite_server", "KiteQServer|HandleReloadConf|FAIL|%s", err)
	}

	//新增或者减少topics
	if len(so.topics) != len(self.kc.so.topics) {
		//推送可发送的topic列表并且获取了对应topic下的订阅关系
		succ := self.exchanger.PushQServer(self.kc.so.bindHost, so.topics)
		if !succ {
			log.ErrorLog("kite_server", "KiteQServer|HandleReloadConf|PushQServer|FAIL|%s|%s\n", err, so.topics)
		} else {
			log.InfoLog("kite_server", "KiteQServer|HandleReloadConf|PushQServer|SUCC|%s\n", so.topics)
		}
		//重置数据
		self.kc.so = so
		//下发变化的数据
		self.topicNotify <- so.topics
	}

	var result struct {
		Status int      `json:"status"`
		Topics []string `json:"topics"`
	}
	result.Status = http.StatusOK
	result.Topics = so.topics

	rawJson, _ := json.Marshal(result)
	resp.Header().Set("content-type", "text/json")
	resp.WriteHeader(http.StatusOK)
	resp.Write(rawJson)
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
