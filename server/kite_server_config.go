package server

import (
	"github.com/blackbeans/turbo"
	"kiteq/stat"
	"time"
)

type KiteQConfig struct {
	fly               bool //投递的飞行模式是否开启
	flowstat          *stat.FlowStat
	rc                *turbo.RemotingConfig
	server            string
	zkhost            string
	deliverTimeout    time.Duration //投递超时时间
	maxDeliverWorkers int           //最大执行实际那
	recoverPeriod     time.Duration //recover的周期
	topics            []string      //可以处理的topics列表
	db                string        //持久层配置
}

func NewKiteQConfig(name string, server, zkhost string, fly bool, deliverTimeout time.Duration, maxDeliverWorkers int,
	recoverPeriod time.Duration,
	topics []string,
	db string,
	rc *turbo.RemotingConfig) KiteQConfig {
	return KiteQConfig{
		fly:               fly,
		flowstat:          stat.NewFlowStat(name),
		rc:                rc,
		server:            server,
		zkhost:            zkhost,
		deliverTimeout:    deliverTimeout,
		maxDeliverWorkers: maxDeliverWorkers,
		recoverPeriod:     recoverPeriod,
		topics:            topics,
		db:                db}
}
