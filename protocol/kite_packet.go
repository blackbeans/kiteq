package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"net"
)

type ITLVPacket interface {
	Marshal() []byte
	Unmarshal(packet []byte) error
}

//请求的packet
type RequestPacket struct {
	Opaque     int32
	CmdType    uint8  //类型
	dataLength uint32 //数据长度
	Data       []byte
}

func (self *RequestPacket) Marshal() []byte {
	length := 4 + 1 + len(self.Data)
	buffer := make([]byte, 0, length)
	buff := bytes.NewBuffer(buffer)

	//彻底包装request为TLV
	binary.Write(buff, binary.BigEndian, self.Opaque)            // 请求id
	binary.Write(buff, binary.BigEndian, self.CmdType)           //数据类型
	binary.Write(buff, binary.BigEndian, uint32(len(self.Data))) //总数据包长度
	binary.Write(buff, binary.BigEndian, self.Data)              // 数据包
	return buff.Bytes()
}

func (self *RequestPacket) Unmarshal(packet []byte) error {

	reader := bytes.NewReader(packet)
	err := binary.Read(reader, binary.BigEndian, &self.Opaque)
	if nil != err {
		return err
	}

	err = binary.Read(reader, binary.BigEndian, &self.CmdType)
	if nil != err {
		return err
	}
	err = binary.Read(reader, binary.BigEndian, &self.dataLength)
	if nil != err {
		return err
	}

	//读取数据包
	self.Data = make([]byte, self.dataLength, self.dataLength)
	rl, err := reader.Read(self.Data)
	if nil != err || uint32(rl) != self.dataLength {
		log.Printf("RequestPacket|Corrupt Data|%s|%d/%d|%t\n", err, rl, self.dataLength, packet)
		return errors.New("Corrupt PacketData")
	}
	return nil
}

//返回响应packet
type ResponsePacket struct {
	Opaque     int32
	Status     int32
	RemoteAddr *net.TCPAddr
}

func (self *ResponsePacket) Marshal() []byte {
	addr := []byte(self.RemoteAddr.String())
	length := 4 + 4 + len(addr)
	buffer := make([]byte, 0, length)
	buff := bytes.NewBuffer(buffer)

	//彻底包装response
	binary.Write(buff, binary.BigEndian, self.Opaque)       // 请求id
	binary.Write(buff, binary.BigEndian, self.Status)       //数据类型
	binary.Write(buff, binary.BigEndian, uint32(len(addr))) //总数据包长度
	binary.Write(buff, binary.BigEndian, addr)              // 数据包
	return buff.Bytes()
}

func (self *ResponsePacket) Unmarshal(packet []byte) error {

	reader := bytes.NewReader(packet)
	var dl uint32
	err := binary.Read(reader, binary.BigEndian, &self.Opaque)
	if nil != err {
		return err
	}

	err = binary.Read(reader, binary.BigEndian, &self.Status)
	if nil != err {
		return err
	}
	err = binary.Read(reader, binary.BigEndian, &dl)
	if nil != err {
		return err
	}
	var addr string
	err = binary.Read(reader, binary.BigEndian, &addr)
	if nil != err {
		return err
	}

	tcpAddr, err := net.ResolveTCPAddr("ipv4", string(addr))
	if nil != err {
		return err
	}
	self.RemoteAddr = tcpAddr
	return nil
}
