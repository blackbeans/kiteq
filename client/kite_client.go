package client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"go-kite/protocol"
	"log"
	"net"
)

type KiteClient struct {
	conn      *net.TCPConn
	id        int32
	groupId   string
	secretKey string
}

func NewKitClient(hostport, groupId, secretKey string) *KiteClient {

	localAddr, err_l := net.ResolveTCPAddr("tcp4", "localhost:24800")
	remoteAddr, err_r := net.ResolveTCPAddr("tcp4", hostport)
	if nil != err_l || nil != err_r {
		log.Fatalf("KITECLIENT|RESOLVE ADDR |FAIL|L:%s|R:%s", err_l, err_r)
	}
	conn, err := net.DialTCP("tcp4", localAddr, remoteAddr)
	if nil != err {
		log.Fatalf("KITECLIENT|CONNECT|%s|FAIL|%s\n", hostport, err)
	}

	client := &KiteClient{}
	client.conn = conn
	client.id = 0
	client.groupId = groupId
	client.secretKey = secretKey

	return client
}

//先进行初次握手上传连接元数据
func (self *KiteClient) HandShake() error {
	connMeta := &protocol.ConnectioMetaPacket{}
	connMeta.GroupId = proto.String(self.groupId)
	connMeta.SecretKey = proto.String(self.secretKey)
	metaPacket, err := proto.Marshal(connMeta)
	if nil != err {
		return err
	}

	_, err = self.write(protocol.CMD_CONN_META, metaPacket)
	return err

}

func (self *KiteClient) SendMessage(msg *protocol.StringMessage) error {
	packet, err := proto.Marshal(msg)
	if nil != err {
		return err
	}

	_, err = self.write(protocol.CMD_TYPE_STRING_MESSAGE, packet)
	return err
}

//最底层的网络写入
func (self *KiteClient) write(cmdType uint8, packet []byte) (int, error) {

	//总长度	  4 字节+ 1字节 + 4字节 + var + \r + \n
	length := 4 + 1 + 4 + len(packet) + 1 + 1

	buffer := make([]byte, 0, length)
	buff := bytes.NewBuffer(buffer)

	self.id++
	//写入数据包的类型
	binary.Write(buff, binary.BigEndian, self.id)
	//写入数据包的类型
	binary.Write(buff, binary.BigEndian, cmdType)
	//写入长度
	binary.Write(buff, binary.BigEndian, uint32(len(packet)))
	//写入真是数据
	binary.Write(buff, binary.BigEndian, packet)
	//写入数据分割
	binary.Write(buff, binary.BigEndian, protocol.CMD_CRLF)

	wl, err := self.conn.Write(buff.Bytes())
	if nil != err {
		log.Printf("KITECLIENT|SEND MESSAGE|FAIL|%s|wl:%d/%d/%d\n", err, wl, length, buff.Len())
		return wl, err
	} else if wl != length {
		errline := fmt.Sprintf("KITECLIENT|SEND MESSAGE|FAIL|%s|wl:%d/%d/%d\n", errors.New("length error"), wl, length, buff.Len())
		return wl, errors.New(errline)
	} else {
		log.Printf("KITECLIENT|SEND MESSAGE|SUCC|type:%d|len:%d/%d|data:%t\n", protocol.CMD_TYPE_STRING_MESSAGE, buff.Len(), length, buff.Bytes())
	}

	return wl, err
}

func (self *KiteClient) Close() {
	self.conn.Close()
}
