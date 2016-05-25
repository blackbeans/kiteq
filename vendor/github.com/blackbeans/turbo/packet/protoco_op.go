package packet

const (
	//最大packet的字节数
	MAX_PACKET_BYTES = 32 * 1024
	PACKET_HEAD_LEN  = (4 + 1 + 2 + 8 + 4) //请求头部长度	 int32
)
