package protocol

const (

	//心跳包
	CMD_TYPE_HEARTBEAT = uint8(0x00)
	CMD_TX_ACK         = uint8(0x01)

	//message
	CMD_TYPE_BYTES_MESSAGE  = uint8(0x10)
	CMD_TYPE_STRING_MESSAGE = uint8(0x11)
)

var CMD_CRLF = [2]byte{'\r', '\n'}
