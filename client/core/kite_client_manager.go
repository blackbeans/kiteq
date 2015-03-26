package core

import (
	"errors"
	"kiteq/binding"
	"kiteq/client/chandler"
	"kiteq/client/listener"
	"kiteq/pipe"
	"kiteq/protocol"
	rclient "kiteq/remoting/client"
	"log"
	"math/rand"
	"net"
	"os"

	"sync"
	"time"
)

//本地事务的方法
type DoTranscation func(message *protocol.QMessage) (bool, error)

const MAX_CLIENT_CONN = 10

type KiteClientManager struct {
	ga *rclient.GroupAuth

	topics        []string
	binds         []*binding.Binding //订阅的关系
	clientManager *rclient.ClientManager
	kiteClients   map[string] /*topic*/ []*kiteClient //topic对应的kiteclient
	zkManager     *binding.ZKManager
	pipeline      *pipe.DefaultPipeline
	lock          sync.RWMutex
	rc            *protocol.RemotingConfig
}

func NewKiteClientManager(zkAddr, groupId, secretKey string, listen listener.IListener) *KiteClientManager {

	rc := protocol.NewRemotingConfig(
		"kiteclient-"+groupId,
		50, 16*1024,
		16*1024, 10000, 10000,
		10*time.Second, 160000)

	//重连管理器
	reconnManager := rclient.NewReconnectManager(true, 30*time.Second, 100, handshake)

	//构造pipeline的结构
	pipeline := pipe.NewDefaultPipeline()
	clientm := rclient.NewClientManager(reconnManager)
	pipeline.RegisteHandler("kiteclient-packet", chandler.NewPacketHandler("kiteclient-packet"))
	pipeline.RegisteHandler("kiteclient-heartbeat", chandler.NewHeartbeatHandler("kiteclient-heartbeat", 10*time.Second, 5*time.Second, clientm))
	pipeline.RegisteHandler("kiteclient-accept", chandler.NewAcceptHandler("kiteclient-accept", listen))
	pipeline.RegisteHandler("kiteclient-remoting", pipe.NewRemotingHandler("kiteclient-remoting", clientm))

	manager := &KiteClientManager{
		ga:            rclient.NewGroupAuth(groupId, secretKey),
		kiteClients:   make(map[string][]*kiteClient, 10),
		topics:        make([]string, 0, 10),
		pipeline:      pipeline,
		clientManager: clientm,
		rc:            rc}
	manager.zkManager = binding.NewZKManager(zkAddr, manager)

	return manager
}

//启动
func (self *KiteClientManager) Start() {

	//开启流量统计
	self.rc.FlowStat.Start()

	hostname, _ := os.Hostname()
	//推送本机到
	err := self.zkManager.PublishTopics(self.topics, self.ga.GroupId, hostname)
	if nil != err {
		log.Fatalf("KiteClientManager|PublishTopics|FAIL|%s|%s\n", err, self.topics)
	} else {
		log.Printf("KiteClientManager|PublishTopics|SUCC|%s\n", self.topics)
	}

outter:
	for _, b := range self.binds {
		for _, t := range self.topics {
			if t == b.Topic {
				continue outter
			}
		}
		self.topics = append(self.topics, b.Topic)
	}

	for _, topic := range self.topics {

		hosts, err := self.zkManager.GetQServerAndWatch(topic)
		if nil != err {
			log.Fatalf("KiteClientManager|GetQServerAndWatch|FAIL|%s|%s\n", err, topic)
		} else {
			log.Printf("KiteClientManager|GetQServerAndWatch|SUCC|%s|%s\n", topic, hosts)
		}
		self.onQServerChanged(topic, hosts)
	}

	if len(self.kiteClients) <= 0 {
		log.Fatalf("KiteClientManager|Start|NO VALID KITESERVER|%s\n", self.topics)
	}

	if len(self.binds) > 0 {
		//订阅关系推送，并拉取QServer
		err = self.zkManager.PublishBindings(self.ga.GroupId, self.binds)
		if nil != err {
			log.Fatalf("KiteClientManager|PublishBindings|FAIL|%s|%s\n", err, self.binds)
		}
	}

}

//创建物理连接
func dial(hostport string) (*net.TCPConn, error) {
	//连接
	remoteAddr, err_r := net.ResolveTCPAddr("tcp4", hostport)
	if nil != err_r {
		log.Printf("KiteClientManager|RECONNECT|RESOLVE ADDR |FAIL|remote:%s\n", err_r)
		return nil, err_r
	}
	conn, err := net.DialTCP("tcp4", nil, remoteAddr)
	if nil != err {
		log.Printf("KiteClientManager|RECONNECT|%s|FAIL|%s\n", hostport, err)
		return nil, err
	}

	return conn, nil
}

func (self *KiteClientManager) SetPublishTopics(topics []string) {
	self.topics = append(self.topics, topics...)
}

func (self *KiteClientManager) SetBindings(bindings []*binding.Binding) {
	for _, b := range bindings {
		b.GroupId = self.ga.GroupId
	}
	self.binds = bindings

}

//发送事务消息
func (self *KiteClientManager) SendTxMessage(msg *protocol.QMessage, doTranscation DoTranscation) (err error) {
	//路由选择策略
	c, err := self.selectKiteClient(msg.GetHeader())
	if nil != err {
		return err
	}

	//先发送消息
	err = c.sendMessage(msg)
	if nil != err {
		return err
	}

	//执行本地事务返回succ为成功则提交、其余条件包括错误、失败都属于回滚
	feedback := ""
	succ := false
	txstatus := protocol.TX_UNKNOWN
	//执行本地事务
	succ, err = doTranscation(msg)
	if nil == err && succ {
		txstatus = protocol.TX_COMMIT
	} else {
		txstatus = protocol.TX_ROLLBACK
		if nil != err {
			feedback = err.Error()
		}
	}
	//发送txack到服务端
	c.sendTxAck(msg, txstatus, feedback)
	return err
}

//发送消息
func (self *KiteClientManager) SendMessage(msg *protocol.QMessage) error {
	c, err := self.selectKiteClient(msg.GetHeader())
	if nil != err {
		return err
	}
	return c.sendMessage(msg)
}

//kiteclient路由选择策略
func (self *KiteClientManager) selectKiteClient(header *protocol.Header) (*kiteClient, error) {

	self.lock.RLock()
	defer self.lock.RUnlock()

	clients, ok := self.kiteClients[header.GetTopic()]
	if !ok || len(clients) <= 0 {
		// 	log.Printf("KiteClientManager|selectKiteClient|FAIL|NO Remote Client|%s\n", header.GetTopic())
		return nil, errors.New("NO KITE CLIENT ! [" + header.GetTopic() + "]")
	}

	c := clients[rand.Intn(len(clients))]
	return c, nil
}

func (self *KiteClientManager) Destory() {
	self.zkManager.Close()
}
