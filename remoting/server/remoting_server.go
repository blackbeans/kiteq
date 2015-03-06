package server

import (
	"kiteq/protocol"
	. "kiteq/remoting/client"
	"kiteq/stat"
	"log"
	"net"
	"time"
)

type RemotingServer struct {
	hostport         string
	keepalive        time.Duration
	stopChan         chan bool
	isShutdown       bool
	packetDispatcher func(remoteClient *RemotingClient, packet *protocol.Packet)
	flowControl      *stat.FlowControl
	rc               *protocol.RemotingConfig
}

func NewRemotionServer(hostport string, flowControl *stat.FlowControl, rc *protocol.RemotingConfig,
	packetDispatcher func(remoteClient *RemotingClient, packet *protocol.Packet)) *RemotingServer {

	//设置为8个并发
	// runtime.GOMAXPROCS(runtime.NumCPU()/2 + 1)

	server := &RemotingServer{
		hostport:         hostport,
		stopChan:         make(chan bool, 1),
		packetDispatcher: packetDispatcher,
		isShutdown:       false,
		flowControl:      flowControl,
		rc:               rc}
	return server
}

func (self *RemotingServer) ListenAndServer() error {

	//开启流控

	self.flowControl.Start()
	addr, err := net.ResolveTCPAddr("tcp4", self.hostport)
	if nil != err {
		log.Fatalf("RemotingServer|ADDR|FAIL|%s\n", self.hostport)
		return err
	}

	listener, err := net.ListenTCP("tcp4", addr)
	if nil != err {
		log.Fatalf("RemotingServer|ListenTCP|FAIL|%s\n", addr)
		return err
	}

	stopListener := &StoppedListener{listener, self.stopChan, self.keepalive}

	//开始服务获取连接
	go self.serve(stopListener)

	return nil

}

func (self *RemotingServer) serve(l *StoppedListener) error {
	for !self.isShutdown {
		conn, err := l.AcceptTCP()
		if nil != err {
			log.Printf("RemotingServer|serve|AcceptTCP|FAIL|%s\n", err)
			continue
		} else {

			log.Printf("RemotingServer|serve|AcceptTCP|SUCC|%s\n", conn.RemoteAddr())
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
	self.flowControl.Stop()
}
