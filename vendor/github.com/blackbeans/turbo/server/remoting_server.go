package server

import (
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"
	"github.com/blackbeans/turbo/client"
	"github.com/blackbeans/turbo/codec"
	"github.com/blackbeans/turbo/packet"
	"net"
	"time"
)

type RemotingServer struct {
	hostport         string
	keepalive        time.Duration
	stopChan         chan bool
	isShutdown       bool
	packetDispatcher func(remoteClient *client.RemotingClient, p *packet.Packet)
	rc               *turbo.RemotingConfig
	codecFunc        func() codec.ICodec
}

func NewRemotionServer(hostport string, rc *turbo.RemotingConfig,
	packetDispatcher func(remoteClient *client.RemotingClient, p *packet.Packet)) *RemotingServer {

	//设置为8个并发
	// runtime.GOMAXPROCS(runtime.NumCPU()/2 + 1)

	server := &RemotingServer{
		hostport:         hostport,
		stopChan:         make(chan bool, 1),
		packetDispatcher: packetDispatcher,
		isShutdown:       false,
		rc:               rc,
		keepalive:        5 * time.Minute,
		codecFunc: func() codec.ICodec {
			return codec.LengthBasedCodec{
				MaxFrameLength: packet.MAX_PACKET_BYTES,
				SkipLength:     4}

		}}
	return server
}

func NewRemotionServerWithCodec(hostport string, rc *turbo.RemotingConfig, codecFunc func() codec.ICodec,
	packetDispatcher func(remoteClient *client.RemotingClient, p *packet.Packet)) *RemotingServer {

	//设置为8个并发
	// runtime.GOMAXPROCS(runtime.NumCPU()/2 + 1)

	server := &RemotingServer{
		hostport:         hostport,
		stopChan:         make(chan bool, 1),
		packetDispatcher: packetDispatcher,
		isShutdown:       false,
		rc:               rc,
		keepalive:        5 * time.Minute,
		codecFunc:        codecFunc}
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
	return nil

}

//networkstat
func (self *RemotingServer) NetworkStat() turbo.NetworkStat {
	return self.rc.FlowStat.Stat()

}

func (self *RemotingServer) serve(l *StoppedListener) error {
	for !self.isShutdown {
		conn, err := l.Accept()
		if nil != err {
			log.Error("RemotingServer|serve|AcceptTCP|FAIL|%s\n", err)
			continue
		} else {
			// log.Debug("RemotingServer|serve|AcceptTCP|SUCC|%s\n", conn.RemoteAddr())
			//创建remotingClient对象

			remoteClient := client.NewRemotingClient(conn, self.codecFunc, self.packetDispatcher, self.rc)
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
