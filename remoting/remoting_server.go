package remoting

import (
	"github.com/golang/protobuf/proto"
	"go-kite/handler"
	"go-kite/remoting/protocol"
	"log"
	"net"
	"time"
)

type RemotingServer struct {
	hostport   string
	keepalive  time.Duration
	stopChan   chan bool
	isShutdown bool
	pipeline   *handler.DefaultPipeline
}

func NewRemotionServer(hostport string, keepalive time.Duration,
	pipeline *handler.DefaultPipeline) *RemotingServer {
	server := &RemotingServer{
		hostport:   hostport,
		keepalive:  keepalive,
		stopChan:   make(chan bool, 1),
		pipeline:   pipeline,
		isShutdown: false}
	return server
}

func (self *RemotingServer) ListenAndServer() error {
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
	return self.serve(stopListener)

}

func (self *RemotingServer) serve(l *StoppedListener) error {
	for !self.isShutdown {
		conn, err := l.AcceptTCP()
		if nil != err {
			log.Printf("RemotingServer|serve|AcceptTCP|FAIL|%s\n", err)
			return err
		} else {
			//session处理
			session := NewSession(conn)
			self.handleSession(session)
		}
	}
	return nil
}

//处理session
func (self *RemotingServer) handleSession(session *Session) {
	//根据不同的cmdtype 走不同的processor
	go func() {
		for !self.isShutdown {

			//1.读取数据包
			cmdtype, packet, err := session.ReadTLV()
			//如果解析读取的数据有错误则直接略过
			if nil != err {
				//返回数据错误响应已经关闭连接直接退出
				log.Printf("REMOTING SERVER|READ PACKET|ERROR|%s\n", err)
				if "EOF" == err.Error() {
					log.Println("REMOTING SERVER|READ PACKET|ERROR|CLOSED!")
					break
				}
			} else {
				self.packetProcess(cmdtype, packet)
			}
		}
	}()
}

//数据包处理
func (self *RemotingServer) packetProcess(cmdtype uint8, packet []byte) {
	var event *handler.AcceptEvent
	var err error
	//根据类型反解packet
	switch cmdtype {
	//心跳
	case protocol.CMD_TYPE_HEARTBEAT:
		heartBeat := &protocol.HeartBeatPacket{}
		err = proto.Unmarshal(packet, heartBeat)

	case protocol.CMD_TX_ACK:
		txAck := &protocol.TranscationACKPacket{}
		err = proto.Unmarshal(packet, txAck)
	//发送的是bytesmessage
	case protocol.CMD_TYPE_BYTES_MESSAGE:
		msg := &protocol.BytesMessage{}
		err = proto.Unmarshal(packet, msg)
		if nil == err {
			event = handler.NewAcceptEvent(protocol.CMD_TYPE_BYTES_MESSAGE, msg)
		}
	//发送的是StringMessage
	case protocol.CMD_TYPE_STRING_MESSAGE:
		msg := &protocol.StringMessage{}
		err = proto.Unmarshal(packet, msg)
		if nil == err {
			event = handler.NewAcceptEvent(protocol.CMD_TYPE_STRING_MESSAGE, msg)
		}
	}

	log.Printf("REMOTING SERVER|RECIEVE MSG|%s|%d|%d|%t|%t\n", err, cmdtype, len(packet), packet, event)
	//直接启动一个go的携程去走处理
	// go func() {
	if nil == err {
		self.pipeline.FireWork(event)
	} else {
		//反序列化数据包失败
	}

	// }()
}

func (self *RemotingServer) Shutdonw() {
	self.isShutdown = true
	close(self.stopChan)
}
