package turbo

import "errors"

const (
	//最大packet的字节数
	MAX_PACKET_BYTES = 2 * 1024 * 1024
	PACKET_HEAD_LEN  = (4 + 1 + 2 + 8 + 4) //请求头部长度	 int32
)

//包体过大
var ERR_TOO_LARGE_PACKET = errors.New("Too Large Packet")
var ERR_TIMEOUT = errors.New("WAIT RESPONSE TIMEOUT ")
var ERR_INVALID_PAYLOAD = errors.New("INVALID PAYLOAD TYPE ! ")
var ERR_MARSHAL_PACKET = errors.New("ERROR MARSHAL PACKET")
var ERR_OVER_FLOW = errors.New("Group Over Flow")
var ERR_NO_HOSTS = errors.New("NO VALID RemoteClient")
var ERR_PONG = errors.New("ERROR PONG TYPE !")


type ICodec interface {

	//反序列化payload
	//返回解析后的interface{}
	UnmarshalPayload(p *Packet) (interface{}, error)
	//序列化payload
	//返回序列化之后的payload字节
	MarshalPayload(p *Packet) ([]byte, error)
}

type LengthBytesCodec struct {
	ICodec
	MaxFrameLength int32 //最大的包大小
}

//反序列化
func (self LengthBytesCodec) UnmarshalPayload(p *Packet) (interface{}, error) {
	return p.Data, nil
}

//序列化
func (self LengthBytesCodec) MarshalPayload(packet *Packet) ([]byte, error) {
	if raw,ok:= packet.PayLoad.([]byte);ok{
		return raw,nil
	}else{
		return nil,ERR_INVALID_PAYLOAD
	}
}

