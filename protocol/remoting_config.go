package protocol

import (
	"time"
)

//网络层参数
type RemotingConfig struct {
	ConnReadBufferSize  int
	ConnWriteBufferSize int
	MinPacketSize       int           //最小的包大小
	FlushThreshold      int           //flush的threshold
	FlushTimeout        time.Duration //flush最大的时间间隔
}
