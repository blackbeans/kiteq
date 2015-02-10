package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
)

//请求的packet
type Packet struct {
	future  chan interface{}
	Opaque  int32
	CmdType uint8 //类型
	Data    []byte
}

func NewPacket(cmdtype uint8, data []byte) *Packet {
	return &Packet{
		Opaque:  -1,
		CmdType: cmdtype,
		Data:    data,
		future:  make(chan interface{}, 1)}
}

func NewRespPacket(opaque int32, cmdtype uint8, data []byte) *Packet {
	p := NewPacket(cmdtype, data)
	p.Opaque = opaque
	return p
}

//重置Opaque
func (self *Packet) ResetOpaque() {
	self.Opaque = -1
	self.future = make(chan interface{}, 1)
}

func (self *Packet) Get() chan interface{} {
	return self.future
}

func (self *Packet) Attach(resp interface{}) {
	self.future <- resp
}

func (self *Packet) Marshal() []byte {
	//总长度	 1+ 4 字节+ 1字节 + 4字节 + var + \r + \n
	length := PACKET_HEAD_LEN + len(self.Data) + 2
	buffer := make([]byte, 0, length)
	buff := bytes.NewBuffer(buffer)

	binary.Write(buff, binary.BigEndian, self.Opaque) // 请求id
	//彻底包装request为TLV
	binary.Write(buff, binary.BigEndian, self.CmdType)           //数据类型
	binary.Write(buff, binary.BigEndian, uint32(len(self.Data))) //总数据包长度
	binary.Write(buff, binary.BigEndian, self.Data)              // 数据包
	binary.Write(buff, binary.BigEndian, CMD_CRLF)
	return buff.Bytes()
}

var ERROR_PACKET_TYPE = errors.New("unmatches packet type ")

func (self *Packet) Unmarshal(r *bytes.Reader) error {

	err := binary.Read(r, binary.BigEndian, &self.Opaque)
	if nil != err {
		return err
	}

	err = binary.Read(r, binary.BigEndian, &self.CmdType)
	if nil != err {
		return err
	}

	var dataLength uint32 //数据长度
	err = binary.Read(r, binary.BigEndian, &dataLength)
	if nil != err {
		return err
	}

	if dataLength > 0 {
		//读取数据包
		self.Data = make([]byte, dataLength, dataLength)
		err = binary.Read(r, binary.BigEndian, self.Data)
		rl := uint32(len(self.Data))
		if nil != err || rl != dataLength {
			// log.Printf("Packet|Unmarshal|Corrupt Data|%s|%d/%d|%t\n", err, rl,
			// 	dataLength, packet)
			return errors.New("Corrupt PacketData")
		}

	} else {
		log.Printf("Packet|Unmarshal|NO Data|%t\n", self)
	}

	return nil
}

//解码packet
func UnmarshalTLV(packet []byte) (*Packet, error) {
	packet = bytes.TrimRight(packet, CMD_STR_CRLF)
	r := bytes.NewReader(packet)

	tlv := &Packet{}
	err := tlv.Unmarshal(r)
	if nil != err {
		return nil, err
	} else {
		return tlv, nil
	}
}
