package server

import (
	"kiteq/protocol"
	"time"
)

type KiteQConfig struct {
	rc                *protocol.RemotingConfig
	server            string
	zkhost            string
	maxDeliverWorkers int           //最大执行实际那
	recoverPeriod     time.Duration //recover的周期
	topics            []string      //可以处理的topics列表
	db                string        //持久层配置
}

func NewKiteQConfig(server, zkhost string, maxDeliverWorkers int,
	recoverPeriod time.Duration,
	topics []string,
	db string,
	rc *protocol.RemotingConfig) KiteQConfig {
	return KiteQConfig{
		rc:                rc,
		server:            server,
		zkhost:            zkhost,
		maxDeliverWorkers: maxDeliverWorkers,
		recoverPeriod:     recoverPeriod,
		topics:            topics,
		db:                db}
}
