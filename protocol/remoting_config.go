package protocol

import (
	"kiteq/stat"
	"time"
)

//网络层参数
type RemotingConfig struct {
	FlowStat         *stat.FlowStat
	MaxDispatcherNum int           //最大分发大小
	MaxWorkerNum     int           //最大处理协程数
	ReadBufferSize   int           //读取缓冲大小
	WriteBufferSize  int           //写入缓冲大小
	WriteChannelSize int           //写异步channel长度
	ReadChannelSize  int           //读异步channel长度
	IdleTime         time.Duration //连接空闲时间
}
