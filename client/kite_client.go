package client

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"go-kite/protocol"
	"go-kite/remoting/session"
	"log"
	"net"
	"sync/atomic"
	"time"
)

type KiteClient struct {
	remoteHost string
	session    *session.Session
	id         int32
	groupId    string
	secretKey  string
	isClose    bool
	holder     map[int32]chan *protocol.ResponsePacket
	respHolder []chan *protocol.ResponsePacket
}

func NewKitClient(remoteHost, groupId, secretKey string) *KiteClient {

	client := &KiteClient{
		id:         0,
		groupId:    groupId,
		secretKey:  secretKey,
		remoteHost: remoteHost,
		isClose:    false,
		holder:     make(map[int32]chan *protocol.ResponsePacket, 1000),
		respHolder: make([]chan *protocol.ResponsePacket, 1000, 1000)}

	for i := 0; i < 1000; i++ {
		client.respHolder[i] = make(chan *protocol.ResponsePacket, 1)
	}

	client.Start()
	return client
}

func (self *KiteClient) dial() (*net.TCPConn, error) {
	localAddr, err_l := net.ResolveTCPAddr("tcp4", "localhost:54800")
	remoteAddr, err_r := net.ResolveTCPAddr("tcp4", self.remoteHost)
	if nil != err_l || nil != err_r {
		log.Fatalf("KITECLIENT|RESOLVE ADDR |FAIL|L:%s|R:%s", err_l, err_r)
		return nil, err_l
	}
	conn, err := net.DialTCP("tcp4", localAddr, remoteAddr)
	if nil != err {
		log.Fatalf("KITECLIENT|CONNECT|%s|FAIL|%s\n", self.remoteHost, err)
		return nil, err
	}

	return conn, nil
}

func (self *KiteClient) Start() {

	//连接
	conn, err := self.dial()
	if nil != err {
		log.Fatalf("KiteClient|START|FAIL|%s\n", err)
	} else {
		self.session = session.NewSession(conn, self.remoteHost)
	}

	//开启写操作
	go self.session.WritePacket()

	//启动读取
	go self.session.ReadPacket()
	//启动读取数据
	go func() {
		for !self.isClose {
			packet := <-self.session.ReadChannel
			self.onPacketRecieve(packet)
		}
	}()

	//握手完成
	err = self.handShake()
	if nil != err {
		log.Fatalf("KiteClient|START|FAIL|%s\n", err)
		return
	}

}

func (self *KiteClient) onPacketRecieve(packet []byte) {
	//解析packet的数据位requestPacket
	respPacket := &protocol.ResponsePacket{}
	err := respPacket.Unmarshal(packet)
	if nil != err {
		//ignore
		log.Printf("KiteClient|onPacketRecieve|INALID PACKET|%s|%t\n", err, packet)
	} else {

		ch, ok := self.holder[respPacket.Opaque]
		if ok {
			ch <- respPacket
		} else {
			log.Printf("KiteClient|onPacketRecieve|CLEAN|HOLDER|%t\n", respPacket)
		}
	}

}

//先进行初次握手上传连接元数据
func (self *KiteClient) handShake() error {

	connMeta := &protocol.ConnectioMetaPacket{
		GroupId:   proto.String(self.groupId),
		SecretKey: proto.String(self.secretKey)}
	metaPacket, err := proto.Marshal(connMeta)
	if nil != err {
		return err
	}

	return self.innerSend(protocol.CMD_CONN_META, metaPacket)

}

func (self *KiteClient) SendMessage(msg *protocol.StringMessage) error {
	packet, err := proto.Marshal(msg)
	if nil != err {
		return err
	}

	return self.innerSend(protocol.CMD_TYPE_STRING_MESSAGE, packet)

}

func (self *KiteClient) innerSend(cmdType uint8, packet []byte) error {
	// self.write(cmdType, packet)
	// return nil
	rid := self.write(cmdType, packet)
	rholder := self.respHolder[rid]
	self.holder[rid] = rholder

	var resp *protocol.ResponsePacket
	select {
	case <-time.After(500 * time.Millisecond):
		// 清除holder
		delete(self.holder, rid)
	case resp = <-rholder:
	}

	if nil != resp {
		if resp.Status != 200 {
			log.Printf("KiteClient|SendMessage|FAIL|%d|%t\n", cmdType, resp)
			return errors.New("SEND MESSAGE FAIL" + fmt.Sprintf("[%d,%s]", resp.Status, resp.RemoteAddr))
		} else {
			return nil
		}
	}
	return errors.New("SEND MESSAGE TIMEOUT ")
}

//最底层的网络写入
func (self *KiteClient) write(cmdType uint8, packet []byte) int32 {
	tid := atomic.AddInt32(&self.id, 1) % 1000
	rpacket := &protocol.RequestPacket{
		CmdType: cmdType,
		Data:    packet,
		Opaque:  tid}

	self.session.WriteChannel <- rpacket.Marshal()
	return tid
}

func (self *KiteClient) Close() {
	self.isClose = true
	self.session.Close()
	log.Printf("KiteClient|Close|REQ_RESP|%t\n", self.holder)
}
