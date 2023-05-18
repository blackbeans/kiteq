package parser

import (
	"context"
	"github.com/blackbeans/logx"
	"kiteq/store"
	smf "kiteq/store/file"
	sm "kiteq/store/memory"
	smq "kiteq/store/mysql"
	"kiteq/store/rocksdb"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var log = logx.GetLogger("kiteq_store")

// storage schema
//  mock    mock://
//  memory  memory://?initcap=1000&maxcap=2000
//  mysql   mysql://master:3306,slave:3306?db=kite&username=root&password=root&maxConn=500&batchUpdateSize=1000&batchDelSize=1000&flushSeconds=1
//  file    file:///path?cap=10000000&checkSeconds=60&flushBatchSize=1000
// rocksdb rocksdb://path?

func ParseDB(ctx context.Context, db string, serverName string) store.IKiteStore {
	var kitedb store.IKiteStore
	parsed, err := url.Parse(db)
	if nil != err {
		panic(err)
	}
	switch parsed.Scheme {
	case "mock":
		kitedb = &store.MockKiteStore{}
	case "memory":
		params := parsed.Query()
		initval := 10 * 10000

		if initcap := params.Get("initcap"); len(initcap) > 0 {
			v, e := strconv.ParseInt(initcap, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|INIT CAP|%s", db)
			}
			initval = int(v)
		}
		max := 50 * 10000
		if maxcap := params.Get("maxcap"); len(maxcap) > 0 {
			v, e := strconv.ParseInt(maxcap, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|MAX CAP|%s", db)
			}
			max = int(v)
		}
		kitedb = sm.NewKiteMemoryStore(ctx, initval, max)
	case "mysql":
		params := parsed.Query()
		bus := 100
		if batchUpdateSize := params.Get("batchUpdateSize"); len(batchUpdateSize) > 0 {
			v, e := strconv.ParseInt(batchUpdateSize, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|batchUpdateSize|%s", db)
			}
			bus = int(v)
		}

		bds := 100
		if batchDelSize := params.Get("batchDelSize"); len(batchDelSize) > 0 {
			v, e := strconv.ParseInt(batchDelSize, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|batchDelSize|%s", db)
			}
			bds = int(v)
		}

		flushPeriod := 1 * time.Second
		if flushSeconds := params.Get("flushSeconds"); len(flushSeconds) > 0 {
			v, e := strconv.ParseInt(flushSeconds, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|batchDelSize|%s", db)
			}
			flushPeriod = time.Duration(v * int64(flushPeriod))
		}

		maxConn := 20
		if mc := params.Get("maxConn"); len(mc) > 0 {
			v, e := strconv.ParseInt(mc, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|batchDelSize|%s", db)
			}
			maxConn = int(v)
		}

		//解析Mysql的host
		master := parsed.Host
		slave := ""
		mysqlHosts := strings.Split(master, ",")
		if len(mysqlHosts) > 1 {
			master = mysqlHosts[0]
			slave = mysqlHosts[1]
		}

		//解析用户名密码：
		username := params.Get("username")
		password := params.Get("password")

		//shard的数量
		shardnum := 4
		if sd := params.Get("shardnum"); len(sd) > 0 {
			v, e := strconv.ParseInt(sd, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|ShardNum|%s", db)
			}
			shardnum = int(v)
		}

		options := smq.MysqlOptions{
			Addr:         master,
			SlaveAddr:    slave,
			ShardNum:     shardnum,
			DB:           params.Get("db"),
			Username:     username,
			Password:     password,
			BatchUpSize:  bus,
			BatchDelSize: bds,
			FlushPeriod:  flushPeriod,
			MaxIdleConn:  maxConn / 2,
			MaxOpenConn:  maxConn}
		kitedb = smq.NewKiteMysql(ctx, options, serverName)
	case "file":
		params := parsed.Query()
		//最大消息容量
		maxcap := 100 * 10000
		if d := params.Get("cap"); len(d) > 0 {
			v, e := strconv.ParseInt(d, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|cap|%s", db)
			}
			maxcap = int(v)
		}

		//检查文件过期时间
		checkPeriod := 1 * time.Second
		if cs := params.Get("checkSeconds"); len(cs) > 0 {
			v, e := strconv.ParseInt(cs, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|checkPeriod|%s", db)
			}
			checkPeriod = time.Duration(v * int64(checkPeriod))
		}

		//批量flush的大小
		fbsize := 1000
		if fbs := params.Get("flushBatchSize"); len(fbs) > 0 {
			v, e := strconv.ParseInt(fbs, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|checkPeriod|%s", db)
			}
			fbsize = int(v)
		}

		kitedb = smf.NewKiteFileStore(ctx, parsed.Host+parsed.Path, fbsize, maxcap, checkPeriod)
		log.Infof("NewKiteQServer|FileStore|%s|%d|%d", parsed.Host+parsed.Path, maxcap, int(checkPeriod.Seconds()))
	case "rocksdb":
		options := make(map[string]string, len(parsed.Query()))
		for k, v := range parsed.Query() {
			if len(v) > 0 {
				options[k] = v[0]
			}
		}
		//获取到存储路径
		kitedb = rocksdb.NewRocksDbStore(ctx, parsed.Host+parsed.Path, options)
		log.Infof("NewKiteQServer|RocksDb|%s|%v", parsed.Host+parsed.Path, options)

	default:
		log.Fatalf("NewKiteQServer|UNSUPPORT DB PROTOCOL|%s", db)
	}
	return kitedb
}
