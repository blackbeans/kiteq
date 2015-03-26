package protocol

type TxStatus int32

const (
	CMD_HEARTBEAT = uint8(0x01) //心跳包
	CMD_CONN_META = uint8(0x02) //连接元信息
	CMD_CONN_AUTH = uint8(0x03) //权限验证cmd
	//消息持久化cmd
	CMD_MESSAGE_STORE_ACK = uint8(0x04) //持久化确认
	CMD_DELIVER_ACK       = uint8(0x05) //投递确认
	CMD_TX_ACK            = uint8(0x06) //事务确认

	//事务处理失败与否
	TX_UNKNOWN  = TxStatus(0)
	TX_COMMIT   = TxStatus(1)
	TX_ROLLBACK = TxStatus(2)

	//message
	CMD_BYTES_MESSAGE  = uint8(0x11)
	CMD_STRING_MESSAGE = uint8(0x12)

	//最大packet的字节数
	MAX_PACKET_BYTES    = 32 * 1024
	MIN_PACKET_BYTES    = 128 //128个字节
	RESP_STATUS_SUCC    = 200
	RESP_STATUS_FAIL    = 500
	RESP_STATUS_TIMEOUT = 501

	PACKET_HEAD_LEN = (4 + 1 + 4) //请求头部长度	 4 字节+ 1字节 + 4字节 + data + \r + \n
)

var CMD_CRLF = []byte{'\r', '\n'}

var CMD_STR_CRLF = "\r\n"
