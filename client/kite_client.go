package client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"go-kite/remoting/protocol"
	"log"
	"net"
)

type KiteClient struct {
	conn *net.TCPConn
}

func NewKitClient(hostport string) *KiteClient {

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

	return client
}

func (self *KiteClient) SendMessage(msg *protocol.StringMessage) error {
	packet, err := proto.Marshal(msg)
	if nil != err {
		return err
	}
	//总长度	  1字节 + 4字节 + var + \r + \n
	length := 1 + 4 + len(packet) + 1 + 1

	buffer := make([]byte, 0, length)
	buff := bytes.NewBuffer(buffer)
	//写入数据包的类型
	binary.Write(buff, binary.BigEndian, protocol.CMD_TYPE_STRING_MESSAGE)
	//写入长度
	binary.Write(buff, binary.BigEndian, uint32(len(packet)))
	//写入真是数据
	binary.Write(buff, binary.BigEndian, packet)
	//写入数据分割
	binary.Write(buff, binary.BigEndian, protocol.CMD_CRLF)

	wl, err := self.conn.Write(buff.Bytes())
	if nil != err {
		return err
	} else if wl != length {
		errline := fmt.Sprintf("KITECLIENT|SEND MESSAGE|FAIL|wl:%d/%d/%d", wl, length, buff.Len())
		return errors.New(errline)
	} else {
		log.Printf("KITECLIENT|SEND MESSAGE|SUCC|type:%d|len:%d/%d|data:%t", protocol.CMD_TYPE_STRING_MESSAGE, buff.Len(), length, buff.Bytes())
	}
	return nil
}

func (self *KiteClient) Close() {
	self.conn.Close()
}
