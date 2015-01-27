package client

import (
	"errors"
	"fmt"
	"kiteq/protocol"
	"kiteq/remoting/session"
	"kiteq/stat"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
)

type KiteClient struct {
	remote      string
	local       string
	session     *session.Session
	id          int32
	groupId     string
	secretKey   string
	isClose     bool
	holder      map[int32]chan *protocol.ResponsePacket
	respHolder  []chan *protocol.ResponsePacket
	flowContorl *stat.FlowControl
}

var MAX_WATER_MARK int = 100000

func NewKitClient(local, remote, groupId, secretKey string) *KiteClient {

	client := &KiteClient{
		id:          0,
		groupId:     groupId,
		secretKey:   secretKey,
		remote:      remote,
		local:       local,
		isClose:     false,
		holder:      make(map[int32]chan *protocol.ResponsePacket, MAX_WATER_MARK),
		respHolder:  make([]chan *protocol.ResponsePacket, MAX_WATER_MARK, MAX_WATER_MARK),
		flowContorl: stat.NewFlowControl(groupId),
	}

	for i := 0; i < MAX_WATER_MARK; i++ {
		client.respHolder[i] = make(chan *protocol.ResponsePacket, 100)
	}

	// client.Start()
	return client
}

func (self *KiteClient) dial() (*net.TCPConn, error) {
	localAddr, err_l := net.ResolveTCPAddr("tcp4", self.local)
	remoteAddr, err_r := net.ResolveTCPAddr("tcp4", self.remote)
	if nil != err_l || nil != err_r {
		log.Fatalf("KITECLIENT|RESOLVE ADDR |FAIL|L:%s|R:%s", err_l, err_r)
		return nil, err_l
	}
	conn, err := net.DialTCP("tcp4", localAddr, remoteAddr)
	if nil != err {
		log.Fatalf("KITECLIENT|CONNECT|%s|FAIL|%s\n", self.remote, err)
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
		self.session = session.NewSession(conn, self.remote, self.onPacketRecieve, self.flowContorl)
	}

	//开启流控
	self.flowContorl.Start()

	//开启写操作
	go self.session.WritePacket()

	go self.session.DispatcherPacket()
	//启动读取
	go self.session.ReadPacket()

	//握手完成
	err = self.handShake()
	if nil != err {
		log.Fatalf("KiteClient|START|FAIL|%s\n", err)
		return
	}

}

func (self *KiteClient) onPacketRecieve(session *session.Session, packet []byte) {
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
			// log.Printf("KiteClient|SendMessage|SUCC|%d|%t\n", cmdType, resp)
			return nil
		}
	}
	return errors.New("SEND MESSAGE TIMEOUT ")
}

//最底层的网络写入
func (self *KiteClient) write(cmdType uint8, packet []byte) int32 {
	tid := atomic.AddInt32(&self.id, 1) % int32(MAX_WATER_MARK)
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
