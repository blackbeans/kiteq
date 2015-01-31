package client

import (
	"errors"
	// "fmt"
	"kiteq/protocol"
	rcient "kiteq/remoting/client"
	"log"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
)

type KiteClient struct {
	remoteClient *rcient.RemotingClient
	id           int32
	groupId      string
	secretKey    string
	isClose      bool

	holder     map[int32]chan *protocol.Packet
	respHolder []chan *protocol.Packet
}

var MAX_WATER_MARK int = 100000

func NewKitClient(groupId, secretKey string, remoteClient *rcient.RemotingClient) *KiteClient {

	client := &KiteClient{
		id:           0,
		groupId:      groupId,
		secretKey:    secretKey,
		remoteClient: remoteClient,
		isClose:      false,
		holder:       make(map[int32]chan *protocol.Packet, MAX_WATER_MARK),
		respHolder:   make([]chan *protocol.Packet, MAX_WATER_MARK, MAX_WATER_MARK)}

	for i := 0; i < MAX_WATER_MARK; i++ {
		client.respHolder[i] = make(chan *protocol.Packet, 100)
	}

	client.Start()
	return client
}

func (self *KiteClient) Start() {

	//启动remotingClient
	self.remoteClient.Start()
	//握手完成
	err := self.handShake()
	if nil != err {
		log.Fatalf("KiteClient|START|FAIL|%s\n", err)
		return
	}

}

//先进行初次握手上传连接元数据
func (self *KiteClient) handShake() error {
	metaPacket := protocol.MarshalConnMeta(self.groupId, self.secretKey)
	return self.innerSend(protocol.CMD_CONN_META, metaPacket)

}

func (self *KiteClient) SendMessage(msg *protocol.StringMessage) error {
	packet, err := proto.Marshal(msg)
	if nil != err {
		return err
	}

	return self.innerSend(protocol.CMD_STRING_MESSAGE, packet)

}

func (self *KiteClient) innerSend(cmdType uint8, packet []byte) error {
	rid := self.write(cmdType, packet)
	rholder := self.respHolder[rid]
	self.holder[rid] = rholder

	var resp *protocol.Packet
	select {
	case <-time.After(500 * time.Millisecond):
		// 清除holder
		delete(self.holder, rid)
	case resp = <-rholder:
		log.Printf("innerSend|%t\n", resp)
	}
	return errors.New("SEND MESSAGE TIMEOUT ")
}

//最底层的网络写入
func (self *KiteClient) write(cmdType uint8, packet []byte) int32 {
	tid := atomic.AddInt32(&self.id, 1) % int32(MAX_WATER_MARK)
	rpacket := &protocol.Packet{
		CmdType: cmdType,
		Data:    packet,
		Opaque:  tid}

	self.remoteClient.Write(rpacket)
	return tid
}

func (self *KiteClient) Close() {
	self.isClose = true
	// self.session.Close()
	log.Printf("KiteClient|Close|REQ_RESP|%t\n", self.holder)
}
