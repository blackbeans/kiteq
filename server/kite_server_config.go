package server

import (
	"errors"
	"flag"
	"github.com/blackbeans/kiteq-common/stat"
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"
	"github.com/naoina/toml"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

type KiteQConfig struct {
	so       ServerOption
	flowstat *stat.FlowStat
	rc       *turbo.RemotingConfig
}

func NewKiteQConfig(so ServerOption, rc *turbo.RemotingConfig) KiteQConfig {
	flowstat := stat.NewFlowStat("KiteQ-" + so.bindHost)
	return KiteQConfig{
		flowstat: flowstat,
		rc:       rc,
		so:       so}
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
	Env                     string   //当前环境使用的是dev还是online
	Topics                  []string //当前集群所能够处理的topics
	DlqExecHour             int      //过期消息清理时间点 24小时
	DeliveryFirst           bool     //投递优先还是存储优先
	Logxml                  string   //日志路径
	Db                      string   //数据文件
	DeliverySeconds         int64    //投递超时时间 单位为s
	MaxDeliverWorkers       int      //最大执行协程数
	RecoverSeconds          int64    //recover的周期 单位为s
	RecievePermitsPerSecond int      //接收消息的最大值  单位s
}

type ServerOption struct {
	clusterName             string        //集群名称
	configPath              string        //配置文件路径
	zkhosts                 string        //zk地址
	bindHost                string        //绑定的端口和IP
	pprofPort               int           //pprof的Port
	topics                  []string      //当前集群所能够处理的topics
	dlqExecHour             int           //过期消息清理时间点 24小时
	deliveryFirst           bool          //服务端是否投递优先 默认是false，优先存储
	logxml                  string        //日志文件路径
	db                      string        //底层对应的存储是什么
	deliveryTimeout         time.Duration //投递超时时间
	maxDeliverWorkers       int           //最大执行协程数
	recoverPeriod           time.Duration //recover的周期
	recievePermitsPerSecond int           //接收消息的最大值  单位s

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
	so.deliveryTimeout = 5 * time.Second
	so.maxDeliverWorkers = 10
	so.recoverPeriod = 60 * time.Second
	so.recievePermitsPerSecond = 8000
	return so
}

func Parse() ServerOption {
	//两种方式都支持
	deliveryFirst := flag.Bool("deliveryFirst", false, "-deliveryFirst=true //开启服务端优先投递，false为优先存储")
	logxml := flag.String("logxml", "./log/log.xml", "-logxml=./log/log.xml")
	bindHost := flag.String("bind", "localhost:13800", "-bind=localhost:13800")
	zkhost := flag.String("zkhost", "localhost:2181", "-zkhost=localhost:2181")
	topics := flag.String("topics", "", "-topics=trade,a,b")
	dlqHourPerDay := flag.Int("dlqHourPerDay", 2, "-dlqExecHour=2 过期消息迁移时间点")
	db := flag.String("db", "memory://initcap=100000&maxcap=200000",
		"-db=mysql://master:3306,slave:3306?db=kite&username=root&password=root&maxConn=500&batchUpdateSize=1000&batchDelSize=1000&flushSeconds=1000")
	pprofPort := flag.Int("pport", -1, "pprof port default value is -1 ")

	clusterName := flag.String("clusterName", "default_dev", "-clusterName=default_dev")
	configPath := flag.String("configPath", "", "-configPath=conf/cluster.toml kiteq配置的toml文件")
	flag.Parse()

	so := ServerOption{}
	//判断当前采用配置文件加载
	if nil != configPath && len(*configPath) > 0 {
		//解析
		err := loadTomlConf(*configPath, *clusterName, *bindHost, *pprofPort, &so)
		if nil != err {
			panic("loadTomlConf|FAIL|" + err.Error())
		}

	} else {
		//采用传参
		so.zkhosts = *zkhost
		so.bindHost = *bindHost
		so.pprofPort = *pprofPort
		so.clusterName = DEFAULT_APP
		so.topics = strings.Split(*topics, ",")
		so.deliveryFirst = *deliveryFirst
		so.dlqExecHour = *dlqHourPerDay
		so.logxml = *logxml
		so.db = *db
		so.deliveryTimeout = 5 * time.Second
		so.maxDeliverWorkers = 8000
		so.recoverPeriod = 60 * time.Second
		so.recievePermitsPerSecond = 8000
	}

	//加载log4go的配置
	log.LoadConfiguration(so.logxml)
	return so
}

func loadTomlConf(path, clusterName, bindHost string, pprofPort int, so *ServerOption) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	buff, rerr := ioutil.ReadAll(f)
	if nil != rerr {
		return rerr
	}
	log.DebugLog("kite_server", "ServerConfig|Parse|toml:%s", string(buff))
	//读取配置
	var option Option
	err = toml.Unmarshal(buff, &option)
	if nil != err {
		return err
	}

	cluster, ok := option.Clusters[clusterName]
	if !ok {
		return errors.New("no cluster config for " + clusterName)
	}

	zk, exist := option.Zookeeper[cluster.Env]
	if !exist {
		return errors.New("no zk  for " + clusterName + ":" + cluster.Env)
	}

	//解析
	so.zkhosts = zk.Hosts
	so.topics = cluster.Topics
	so.deliveryFirst = cluster.DeliveryFirst
	so.dlqExecHour = cluster.DlqExecHour
	so.logxml = cluster.Logxml
	so.db = cluster.Db
	so.deliveryTimeout = time.Duration(cluster.DeliverySeconds * int64(time.Second))
	so.maxDeliverWorkers = cluster.MaxDeliverWorkers
	so.recoverPeriod = time.Duration(cluster.RecoverSeconds * int64(time.Second))
	so.recievePermitsPerSecond = cluster.RecievePermitsPerSecond
	so.bindHost = bindHost
	so.pprofPort = pprofPort
	so.clusterName = clusterName
	so.configPath = path
	return nil
}
