package codec

import "github.com/blackbeans/turbo/packet"

type ICodec interface {

	//包装秤packet
	UnmarshalPacket(p packet.Packet) (*packet.Packet, error)
	//序列化packet
	MarshalPacket(p packet.Packet) ([]byte, error)
}

type LengthBasedCodec struct {
	ICodec
	MaxFrameLength int32 //最大的包大小
	SkipLength     int16 //跳过长度字节数
}

//反序列化
func (self LengthBasedCodec) UnmarshalPacket(p packet.Packet) (*packet.Packet, error) {
	return &p, nil
}

//序列化
func (self LengthBasedCodec) MarshalPacket(packet packet.Packet) ([]byte, error) {
	rawData := packet.Marshal()
	return rawData, nil
}
