package remoting

import (
	"encoding/binary"
	"errors"
	"go-kite/remoting/protocol"
	"net"
)

type Session struct {
	conn      *net.TCPConn //tcp的session
	heartbeat int64
}

func NewSession(conn *net.TCPConn) *Session {
	session := &Session{conn: conn}
	return session
}

var ERROR_PACKET_DATA = errors.New("invalid packet data length !")

//读取
func (self *Session) ReadTLV() (uint8, []byte, error) {

	// 1字节 + 4字节 + var
	var ptype uint8
	var length uint32
	var data []byte

	//类型数据有问题
	err := binary.Read(self.conn, binary.BigEndian, &ptype)
	if nil != err {

	}

	err = binary.Read(self.conn, binary.BigEndian, &length)
	if nil != err {
		return ptype, data, ERROR_PACKET_DATA
	}

	data = make([]byte, 0, length)
	err = binary.Read(self.conn, binary.BigEndian, data)
	//如果定义的length和真正读取的数据包的长度不一致则有问题
	if nil != err {
		return ptype, data, ERROR_PACKET_DATA
	}

	return ptype, data, nil
}

//设置本次心跳检测的时间
func (self *Session) SetHeartBeat(duration int64) {
	self.heartbeat = duration
}

func (self *Session) GetHeartBeat() int64 {
	return self.heartbeat
}

//写入响应
func (self *Session) WriteReponse(resp *protocol.ResponsePacket) error {
	return nil
}

//写出去ack
func (self *Session) WriteHeartBeatAck(ack *protocol.HeartBeatACKPacket) error {

	return nil
}

func (self *Session) Close() error {

	return nil
}
