package protocol

const (

	//心跳包
	CMD_TYPE_HEARTBEAT = uint8(0x00)
	CMD_TX_ACK         = uint8(0x01)
	CMD_CONN_META      = uint8(0x02)

	//消息持久化cmd
	CMD_TYPE_MESSAGE_STORE = uint8(0x10)
	//message
	CMD_TYPE_BYTES_MESSAGE  = uint8(0x11)
	CMD_TYPE_STRING_MESSAGE = uint8(0x12)

	//最大packet的字节数
	MAX_PACKET_BYTES = 8 * 1024 * 1024

	RESP_STATUS_SUCC    = 200
	RESP_STATUS_FAIL    = 500
	RESP_STATUS_TIMEOUT = 501

	REQ_PACKET_HEAD_LEN  = (4 + 1 + 4) //请求头部长度	  4 字节+ 1字节 + 4字节 + data + \r + \n
	RESP_PACKET_HEAD_LEN = (4 + 1 + 4) //响应头部长度   4 + 1 + 4 + data + \r + \n
)

var CMD_CRLF = []byte{'\r', '\n'}

var CMD_STR_CRLF = "\r\n"
