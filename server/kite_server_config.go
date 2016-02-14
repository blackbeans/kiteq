package server

import (
	"flag"
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"
	"github.com/naoina/toml"
	"io/ioutil"
	"kiteq/stat"
	"os"
	"strings"
	"time"
)

type KiteQConfig struct {
	deliveryFirst     bool //投递的飞行模式是否开启
	flowstat          *stat.FlowStat
	rc                *turbo.RemotingConfig
	server            string
	pprofPort         int
	zkhost            string
	deliverTimeout    time.Duration //投递超时时间
	maxDeliverWorkers int           //最大执行实际那
	recoverPeriod     time.Duration //recover的周期
	dlqHour           int           //dlq的处理每天固定时间点
	topics            []string      //可以处理的topics列表
	db                string        //持久层配置
}

func NewKiteQConfig(so ServerOption, deliverTimeout time.Duration, maxDeliverWorkers int,
	recoverPeriod time.Duration, rc *turbo.RemotingConfig) KiteQConfig {
	flowstat := stat.NewFlowStat("KiteQ-" + so.bindHost)
	for _, topic := range so.topics {
		flowstat.TopicsFlows[topic] = &turbo.Flow{}
	}
	return KiteQConfig{
		deliveryFirst:     so.deliveryFirst,
		flowstat:          flowstat,
		rc:                rc,
		server:            so.bindHost,
		pprofPort:         so.pprofPort,
		zkhost:            so.zkhosts,
		deliverTimeout:    deliverTimeout,
		maxDeliverWorkers: maxDeliverWorkers,
		recoverPeriod:     recoverPeriod,
		dlqHour:           so.dlqExecHour,
		topics:            so.topics,
		db:                so.db}
}

const (
	DEFAULT_APP = "default"
)

type HostPort struct {
	Hosts string
}

//配置信息
type Option struct {
	Zookeeper map[string]HostPort //zookeeper的配置
	Clusters  map[string]Cluster  //各集群的配置
}

//----------------------------------------
//Cluster配置
type Cluster struct {
	Env           string   //当前环境使用的是dev还是online
	Topics        []string //当前集群所能够处理的topics
	DlqExecHour   int      //过期消息清理时间点 24小时
	DeliveryFirst bool     //投递优先还是存储优先
	Logxml        string   //日志路径
	Db            string   //数据文件
}

type ServerOption struct {
	clusterName   string   //集群名称
	zkhosts       string   //zk地址
	bindHost      string   //绑定的端口和IP
	pprofPort     int      //pprof的Port
	topics        []string //当前集群所能够处理的topics
	dlqExecHour   int      //过期消息清理时间点 24小时
	deliveryFirst bool     //服务端是否投递优先 默认是false，优先存储
	logxml        string   //日志文件路径
	db            string   //底层对应的存储是什么
}

//only for test
func MockServerOption() ServerOption {
	so := ServerOption{}
	so.zkhosts = "localhost:2181"
	so.bindHost = "localhost:13800"
	so.pprofPort = -1
	so.topics = []string{"trade"}
	so.deliveryFirst = false
	so.dlqExecHour = 2
	so.db = "memory://"
	so.clusterName = DEFAULT_APP
	return so
}

func Parse() ServerOption {
	//两种方式都支持
	deliveryFirst := flag.Bool("deliveryFirst", false, "-deliveryFirst=true //开启服务端优先投递，false为优先存储")
	logxml := flag.String("logxml", "./log/log.xml", "-logxml=./log/log.xml")
	bindHost := flag.String("bind", ":13800", "-bind=localhost:13800")
	zkhost := flag.String("zkhost", "localhost:2181", "-zkhost=localhost:2181")
	topics := flag.String("topics", "", "-topics=trade,a,b")
	dlqHourPerDay := flag.Int("dlqHourPerDay", 2, "-dlqExecHour=2 过期消息迁移时间点")
	db := flag.String("db", "memory://initcap=100000&maxcap=200000",
		"-db=mysql://master:3306,slave:3306?db=kite&username=root&password=root&maxConn=500&batchUpdateSize=1000&batchDelSize=1000&flushSeconds=1000")
	pprofPort := flag.Int("pport", -1, "pprof port default value is -1 ")

	clusterName := flag.String("clusterName", DEFAULT_APP, "-clusterName=default")
	configFile := flag.String("configFile", "", "-configFile=${path} kiteq配置的toml文件")
	flag.Parse()

	so := ServerOption{}
	//判断当前采用配置文件加载
	if nil != configFile && len(*configFile) > 0 {
		f, err := os.Open(*configFile)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		buff, rerr := ioutil.ReadAll(f)
		if nil != rerr {
			panic(rerr)
		}
		log.DebugLog("kite_server", "ServerConfig|Parse|toml:%s", string(buff))
		//读取配置
		var option Option
		err = toml.Unmarshal(buff, &option)
		if nil != err {
			panic(err)
		}

		cluster, ok := option.Clusters[*clusterName]
		if !ok {
			panic("no cluster config for " + *clusterName)
		}

		zk, exist := option.Zookeeper[cluster.Env]
		if !exist {
			panic("no zk  for " + *clusterName + ":" + cluster.Env)
		}

		//解析

		so.zkhosts = zk.Hosts
		so.bindHost = *bindHost
		so.pprofPort = *pprofPort
		so.topics = cluster.Topics
		so.deliveryFirst = cluster.DeliveryFirst
		so.dlqExecHour = cluster.DlqExecHour
		so.logxml = cluster.Logxml
		so.db = cluster.Db
		so.clusterName = *clusterName

	} else {
		//采用传参
		so.zkhosts = *zkhost
		so.bindHost = *bindHost
		so.pprofPort = *pprofPort
		so.topics = strings.Split(*topics, ",")
		so.deliveryFirst = *deliveryFirst
		so.dlqExecHour = *dlqHourPerDay
		so.logxml = *logxml
		so.db = *db
		so.clusterName = DEFAULT_APP

	}

	//加载log4go的配置
	log.LoadConfiguration(so.logxml)
	return so
}
