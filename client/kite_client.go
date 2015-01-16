package client

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"go-kite/protocol"
	"io"
	"log"
	"net"
)

type KiteClient struct {
	conn      *net.TCPConn
	id        int32
	groupId   string
	secretKey string
	isClose   bool
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

	client := &KiteClient{
		conn:      conn,
		id:        0,
		groupId:   groupId,
		secretKey: secretKey, isClose: false}

	client.Start()
	return client
}

func (self *KiteClient) Start() {
	err := self.handShake()
	if nil != err {
		log.Fatalf("KiteClient|START|FAIL|%s\n", err)
		return
	}

	go func() {

		br := bufio.NewReader(self.conn)
		//缓存本次包的数据
		packetBuff := make([]byte, 0, 1024)
		buff := bytes.NewBuffer(packetBuff)

		for !self.isClose {
			slice, err := br.ReadSlice(protocol.CMD_CRLF[0])
			//读取包
			if err == io.EOF {
				continue
			} else if nil != err {
				log.Printf("KiteClient|ReadPacket|\\r|FAIL|%s\n", err)
			}

			_, err = buff.Write(slice)
			//数据量太庞大直接拒绝
			if buff.Len() >= protocol.MAX_PACKET_BYTES ||
				bytes.ErrTooLarge == err {
				log.Printf("KiteClient|ReadPacket|ErrTooLarge|%d\n", buff.Len())
				buff.Reset()
				continue
			}

			//再读取一个字节判断是否为\n
			delim, err := br.ReadByte()
			if nil != err {
				log.Printf("KiteClient|ReadPacket|\\n|FAIL|%s\n", err)
			} else {
				//继续读取
				buff.WriteByte(delim)
				if delim == protocol.CMD_CRLF[1] {
					//如果是\n那么就是一个完整的包
					packet := make([]byte, 0, buff.Len())
					packet = append(packet, buff.Bytes()...)
					self.onPacketRecieve(packet)
					//重置buffer
					buff.Reset()
				}
			}

		}

	}()
}

func (self *KiteClient) onPacketRecieve(packet []byte) {
	//解析packet的数据位requestPacket
	respPacket := &protocol.ResponsePacket{}
	err := respPacket.Unmarshal(packet)
	if nil != err {
		//ignore
		log.Printf("KiteClient|onPacketRecieve|INALID PACKET|%s|%t\n", err, packet)
	} else {
		// log.Printf("KiteClient|onPacketRecieve|SUCC|%t\n", respPacket)
	}

}

//先进行初次握手上传连接元数据
func (self *KiteClient) handShake() error {
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
		// log.Printf("KITECLIENT|SEND MESSAGE|SUCC|type:%d|len:%d/%d|data:%t\n", protocol.CMD_TYPE_STRING_MESSAGE, buff.Len(), length, buff.Bytes())
	}

	return wl, err
}

func (self *KiteClient) Close() {
	self.isClose = true
	self.conn.Close()
}
