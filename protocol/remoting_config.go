package protocol

import (
	"kiteq/stat"
)

//网络层参数
type RemotingConfig struct {
	FlowStat         *stat.FlowStat
	MaxDispatcherNum int //最大分发大小
	MaxWriterNum     int //多少个写线程
	MaxWorkerNum     int //最大处理协程数
	ReadBufferSize   int //读取缓冲大小
	WriteBufferSize  int //写入缓冲大小
	WriteChannelSize int //写异步channel长度
	ReadChannelSize  int //读异步channel长度
}
