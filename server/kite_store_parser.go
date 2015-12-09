package server

import (
	log "github.com/blackbeans/log4go"
	"kiteq/store"
	smf "kiteq/store/file"
	sm "kiteq/store/memory"
	smq "kiteq/store/mysql"
	"strconv"
	"strings"
	"time"
)

// storage schema
//  mock    mock://
//  memory  memory://initcap=1000&maxcap=2000
//  mysql   mysql://master:3306,slave:3306?db=kite&username=root&password=root&maxConn=500&batchUpdateSize=1000&batchDelSize=1000&flushSeconds=1
//  file    file:///path?cap=10000000&checkSeconds=60

func parseDB(kc KiteQConfig) store.IKiteStore {
	db := kc.db

	var kitedb store.IKiteStore
	if strings.HasPrefix(db, "mock://") {
		kitedb = &store.MockKiteStore{}
	} else if strings.HasPrefix(db, "memory://") {
		url := strings.TrimPrefix(db, "memory://")
		split := strings.Split(url, "&")
		params := make(map[string]string, len(split))
		for _, v := range split {
			p := strings.SplitN(v, "=", 2)
			if len(p) >= 2 {
				params[p[0]] = p[1]
			}
		}

		initval := 10 * 10000
		initcap, ok := params["initcap"]
		if ok {
			v, e := strconv.ParseInt(initcap, 10, 32)
			if nil != e {
				log.Crashf("NewKiteQServer|INVALID|INIT CAP|%s\n", db)
			}
			initval = int(v)
		}
		max := 50 * 10000
		maxcap, ok := params["maxcap"]
		if ok {
			v, e := strconv.ParseInt(maxcap, 10, 32)
			if nil != e {
				log.Crashf("NewKiteQServer|INVALID|MAX CAP|%s\n", db)
			}
			max = int(v)
		}
		kitedb = sm.NewKiteMemoryStore(initval, max)
	} else if strings.HasPrefix(db, "mysql://") {
		url := strings.TrimPrefix(db, "mysql://")
		mp := strings.Split(url, "?")
		params := make(map[string]string, 5)
		if len(mp) > 1 {
			split := strings.Split(mp[1], "&")
			for _, v := range split {
				p := strings.SplitN(v, "=", 2)
				params[p[0]] = p[1]
			}
		}

		bus := 100
		u, ok := params["batchUpdateSize"]
		if ok {
			v, e := strconv.ParseInt(u, 10, 32)
			if nil != e {
				log.Crashf("NewKiteQServer|INVALID|batchUpdateSize|%s\n", db)
			}
			bus = int(v)
		}

		bds := 100
		d, ok := params["batchDelSize"]
		if ok {
			v, e := strconv.ParseInt(d, 10, 32)
			if nil != e {
				log.Crashf("NewKiteQServer|INVALID|batchDelSize|%s\n", db)
			}
			bds = int(v)
		}

		flushPeriod := 1 * time.Second
		fp, ok := params["flushSeconds"]
		if ok {
			v, e := strconv.ParseInt(fp, 10, 32)
			if nil != e {
				log.Crashf("NewKiteQServer|INVALID|batchDelSize|%s\n", db)
			}
			flushPeriod = time.Duration(v * int64(flushPeriod))
		}

		maxConn := 500
		mc, ok := params["maxConn"]
		if ok {
			v, e := strconv.ParseInt(mc, 10, 32)
			if nil != e {
				log.Crashf("NewKiteQServer|INVALID|batchDelSize|%s\n", db)
			}
			maxConn = int(v)
		}

		//解析Mysql的host
		master := mp[0]
		slave := ""
		mysqlHosts := strings.Split(mp[0], ",")
		if len(mysqlHosts) > 1 {
			master = mysqlHosts[0]
			slave = mysqlHosts[1]
		}

		//解析用户名密码：
		username, _ := params["username"]
		password, pok := params["password"]
		if !pok {
			password = ""
		}

		//shard的数量
		shardnum := 4
		sd, sdok := params["shardnum"]
		if sdok {
			v, e := strconv.ParseInt(sd, 10, 32)
			if nil != e {
				log.Crashf("NewKiteQServer|INVALID|ShardNum|%s\n", db)
			}
			shardnum = int(v)
		}

		options := smq.MysqlOptions{
			Addr:         master,
			SlaveAddr:    slave,
			ShardNum:     shardnum,
			DB:           params["db"],
			Username:     username,
			Password:     password,
			BatchUpSize:  bus,
			BatchDelSize: bds,
			FlushPeriod:  flushPeriod,
			MaxIdleConn:  maxConn / 2,
			MaxOpenConn:  maxConn}
		kitedb = smq.NewKiteMysql(options)
	} else if strings.HasPrefix(db, "file://") {
		url := strings.TrimPrefix(db, "file://")
		mp := strings.Split(url, "?")
		params := make(map[string]string, 5)
		if len(mp) > 1 {
			split := strings.Split(mp[1], "&")
			for _, v := range split {
				p := strings.SplitN(v, "=", 2)
				params[p[0]] = p[1]
			}
		}
		if len(mp[0]) <= 0 {
			log.Crashf("NewKiteQServer|INVALID|FilePath|%s\n", db)
		}

		//最大消息容量
		maxcap := 100
		d, ok := params["cap"]
		if ok {
			v, e := strconv.ParseInt(d, 10, 32)
			if nil != e {
				log.Crashf("NewKiteQServer|INVALID|cap|%s\n", db)
			}
			maxcap = int(v)
		}

		//检查文件过期时间
		checkPeriod := 1 * time.Second
		fp, ok := params["checkSeconds"]
		if ok {
			v, e := strconv.ParseInt(fp, 10, 32)
			if nil != e {
				log.Crashf("NewKiteQServer|INVALID|checkPeriod|%s\n", db)
			}
			checkPeriod = time.Duration(v * int64(checkPeriod))
		}

		kitedb = smf.NewKiteFileStore(mp[0], maxcap, checkPeriod)
		log.Debug("NewKiteQServer|FILESTORE|%s|%d|%d", mp[0], maxcap, int(checkPeriod.Seconds()))
	} else {
		log.Crashf("NewKiteQServer|UNSUPPORT DB PROTOCOL|%s\n", db)
	}
	kc.flowstat.Kitestore = kitedb
	return kitedb
}
