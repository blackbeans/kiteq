package codec

import (
	"bufio"
	"bytes"
	b "encoding/binary"
	"errors"
	"fmt"
	"github.com/blackbeans/turbo/packet"
)

type ICodec interface {
	//读取数据
	Read(reader *bufio.Reader) (*bytes.Buffer, error)

	//包装秤packet
	UnmarshalPacket(buff *bytes.Buffer) (*packet.Packet, error)
	//序列化packet
	MarshalPacket(p *packet.Packet) []byte
}

type LengthBasedCodec struct {
	ICodec
	MaxFrameLength int32 //最大的包大小
	SkipLength     int16 //跳过长度字节数
}

//读取规定长度的数据
func (self LengthBasedCodec) Read(reader *bufio.Reader) (*bytes.Buffer, error) {
	var length int32

	err := Read(reader, b.BigEndian, &length)
	if nil != err {
		return nil, err
	} else if length <= 0 {
		return nil, errors.New("TOO SHORT PACKET")
	}

	if length > self.MaxFrameLength {
		return nil, errors.New("TOO LARGE PACKET!")
	}

	buff := make([]byte, int(length))
	tmp := buff
	l := 0
	for {
		rl, err := reader.Read(tmp)
		if nil != err {
			return nil, err
		}
		l += rl

		if l < int(length) {
			tmp = tmp[rl:]
			continue
		} else {
			break
		}
	}
	return bytes.NewBuffer(buff), nil
}

//反序列化
func (self LengthBasedCodec) UnmarshalPacket(buff *bytes.Buffer) (*packet.Packet, error) {
	p := &packet.Packet{}
	if buff.Len() < packet.PACKET_HEAD_LEN {
		return nil, errors.New(
			fmt.Sprintf("Corrupt PacketData|Less Than MIN LENGTH:%d/%d", buff.Len(), packet.PACKET_HEAD_LEN))
	}
	reader := bytes.NewReader(buff.Next(packet.PACKET_HEAD_LEN))
	header, err := packet.UnmarshalHeader(reader)
	if nil != err {
		return nil, errors.New(
			fmt.Sprintf("Corrupt PacketHeader|%s", err.Error()))
	}

	if header.BodyLen > packet.MAX_PACKET_BYTES {
		return nil, errors.New(fmt.Sprintf("Too Large Packet %d|%d", header.BodyLen, packet.MAX_PACKET_BYTES))
	}
	p.Header = header
	p.Data = buff.Bytes()

	return p, nil
}

//序列化
func (self LengthBasedCodec) MarshalPacket(packet *packet.Packet) []byte {
	return packet.Marshal()
}
