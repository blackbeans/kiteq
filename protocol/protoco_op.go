package protocol

const (

	//心跳包
	CMD_TYPE_HEARTBEAT = uint8(0x00)
	CMD_TX_ACK         = uint8(0x01)
	CMD_CONN_META      = uint8(0x02)

	//message
	CMD_TYPE_BYTES_MESSAGE  = uint8(0x10)
	CMD_TYPE_STRING_MESSAGE = uint8(0x11)

	MAX_PACKET_BYTES = 8 * 1024 * 1024
)

var CMD_CRLF = []byte{'\r', '\n'}

var CMD_STR_CRLF = "\r\n"
