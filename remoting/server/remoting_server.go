package server

import (
	log "github.com/blackbeans/log4go"
	"kiteq/remoting"
	. "kiteq/remoting/client"
	"kiteq/remoting/packet"
	"net"
	"time"
)

type RemotingServer struct {
	hostport         string
	keepalive        time.Duration
	stopChan         chan bool
	isShutdown       bool
	packetDispatcher func(remoteClient *RemotingClient, packet *packet.Packet)
	rc               *remoting.RemotingConfig
}

func NewRemotionServer(hostport string, rc *remoting.RemotingConfig,
	packetDispatcher func(remoteClient *RemotingClient, packet *packet.Packet)) *RemotingServer {

	//设置为8个并发
	// runtime.GOMAXPROCS(runtime.NumCPU()/2 + 1)

	server := &RemotingServer{
		hostport:         hostport,
		stopChan:         make(chan bool, 1),
		packetDispatcher: packetDispatcher,
		isShutdown:       false,
		rc:               rc,
		keepalive:        5 * time.Second}
	return server
}

func (self *RemotingServer) ListenAndServer() error {

	addr, err := net.ResolveTCPAddr("tcp4", self.hostport)
	if nil != err {
		log.Error("RemotingServer|ADDR|FAIL|%s\n", self.hostport)
		return err
	}

	listener, err := net.ListenTCP("tcp4", addr)
	if nil != err {
		log.Error("RemotingServer|ListenTCP|FAIL|%s\n", addr)
		return err
	}

	stopListener := &StoppedListener{listener, self.stopChan, self.keepalive}

	//开始服务获取连接
	go self.serve(stopListener)
	go self.Start()
	return nil

}

func (self *RemotingServer) Start() {

	t := time.NewTicker(1 * time.Second)
	for !self.isShutdown {
		line := self.rc.FlowStat.Monitor()
		log.Info(line)
		<-t.C
	}
	t.Stop()
}

func (self *RemotingServer) serve(l *StoppedListener) error {
	for !self.isShutdown {
		conn, err := l.Accept()
		if nil != err {
			log.Error("RemotingServer|serve|AcceptTCP|FAIL|%s\n", err)
			continue
		} else {

			log.Debug("RemotingServer|serve|AcceptTCP|SUCC|%s\n", conn.RemoteAddr())
			//创建remotingClient对象
			remoteClient := NewRemotingClient(conn, self.packetDispatcher, self.rc)
			remoteClient.Start()
		}
	}
	return nil
}

func (self *RemotingServer) Shutdown() {
	self.isShutdown = true
	close(self.stopChan)
	log.Info("RemotingServer|Shutdown...")
}
