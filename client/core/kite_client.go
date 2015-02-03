package core

import (
	"errors"
	"fmt"
	"kiteq/protocol"
	rcient "kiteq/remoting/client"
	"kiteq/remoting/session"
	"kiteq/stat"
	"log"
	"net"
	"time"
)

type KiteClient struct {
	local            string
	remote           string
	groupId          string
	secretKey        string
	isClose          bool
	flowControl      *stat.FlowControl
	remoteClient     *rcient.RemotingClient
	packetDispatcher func(remoteClient *rcient.RemotingClient, packet []byte)
}

func NewKitClient(groupId, secretKey, local, remote string,
	packetDispatcher func(remoteClient *rcient.RemotingClient, packet []byte)) *KiteClient {

	client := &KiteClient{
		local:            local,
		remote:           remote,
		groupId:          groupId,
		secretKey:        secretKey,
		flowControl:      stat.NewFlowControl(groupId),
		isClose:          false,
		packetDispatcher: packetDispatcher}

	client.Start()
	return client
}

func (self *KiteClient) Start() {

	//连接
	conn, err := self.dial()
	if nil != err {
		log.Fatalf("RemotingClient|START|FAIL|%s\n", err)
	} else {
		rsession := session.NewSession(conn, self.remote, self.flowControl)
		self.remoteClient = rcient.NewRemotingClient(rsession, self.packetDispatcher)
	}

	//启动remotingClient
	self.remoteClient.Start()
	//握手完成
	err = self.handshake()
	if nil != err {
		log.Fatalf("RemotingClient|START|FAIL|%s\n", err)
		return
	}
}

func (self *KiteClient) dial() (*net.TCPConn, error) {
	_, err_l := net.ResolveTCPAddr("tcp4", self.local)
	remoteAddr, err_r := net.ResolveTCPAddr("tcp4", self.remote)
	if nil != err_l || nil != err_r {
		log.Fatalf("RemotingClient|RESOLVE ADDR |FAIL|L:%s|R:%s", err_l, err_r)
		return nil, err_l
	}
	conn, err := net.DialTCP("tcp4", nil, remoteAddr)
	if nil != err {
		log.Fatalf("RemotingClient|CONNECT|%s|FAIL|%s\n", self.remote, err)
		return nil, err
	}

	return conn, nil
}

//先进行初次握手上传连接元数据
func (self *KiteClient) handshake() error {
	packet := protocol.MarshalConnMeta(self.groupId, self.secretKey)
	rpacket := protocol.NewPacket(protocol.CMD_CONN_META, packet)

	resp, err := self.remoteClient.WriteAndGet(rpacket, 500*time.Millisecond)
	if nil != err {
		return err
	} else {
		authAck, ok := resp.(*protocol.ConnAuthAck)
		if !ok {
			return errors.New("Unmatches Handshake Ack Type! ")
		} else {
			if authAck.GetStatus() {
				log.Printf("RemotingClient|handShake|SUCC|%s\n", authAck.GetFeedback())
				return nil
			} else {
				log.Printf("RemotingClient|handShake|FAIL|%s\n", authAck.GetFeedback())
				return errors.New("Auth FAIL![" + authAck.GetFeedback() + "]")
			}
		}
	}
}

func (self *KiteClient) SendStringMessage(message *protocol.StringMessage) error {
	data, err := protocol.MarshalPbMessage(message)
	if nil != err {
		return err
	}
	return self.innerSendMessage(protocol.CMD_STRING_MESSAGE, data)
}

func (self *KiteClient) SendBytesMessage(message *protocol.BytesMessage) error {
	data, err := protocol.MarshalPbMessage(message)
	if nil != err {
		return err
	}

	return self.innerSendMessage(protocol.CMD_BYTES_MESSAGE, data)
}

func (self *KiteClient) innerSendMessage(cmdType uint8, packet []byte) error {
	msgpacket := protocol.NewPacket(cmdType, packet)

	resp, err := self.remoteClient.WriteAndGet(msgpacket, 200*time.Millisecond)
	if nil != err {
		return err
	} else {
		storeAck, ok := resp.(*protocol.MessageStoreAck)
		if !ok || !storeAck.GetStatus() {
			return errors.New(fmt.Sprintf("KiteClient|SendMessage|FAIL|%s\n", resp))
		} else {
			log.Printf("KiteClient|SendMessage|SUCC|%s|%s\n", storeAck.GetMessageId(), storeAck.GetFeedback())
			return nil
		}
	}
}

func (self *KiteClient) Close() {
	self.isClose = true
	self.remoteClient.Shutdown()
}
