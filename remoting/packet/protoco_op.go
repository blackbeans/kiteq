package packet

const (
	//最大packet的字节数
	MAX_PACKET_BYTES = 32 * 1024
	MIN_PACKET_BYTES = 128         //128个字节
	PACKET_HEAD_LEN  = (4 + 1 + 4) //请求头部长度	 4 字节+ 1字节 + 4字节 + data + \r + \n
)

var CMD_CRLF = []byte{'\r', '\n'}

var CMD_STR_CRLF = "\r\n"
