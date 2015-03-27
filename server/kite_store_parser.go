package server

import (
	"kiteq/store"
	smm "kiteq/store/mmap"
	smq "kiteq/store/mysql"
	"log"

	"strconv"
	"strings"
	"time"
)

func parseDB(db string) store.IKiteStore {
	var kitedb store.IKiteStore
	if strings.HasPrefix(db, "mock://") {
		kitedb = &store.MockKiteStore{}
	} else if strings.HasPrefix(db, "mmap://") {
		url := strings.TrimPrefix(db, "mmap://")
		split := strings.Split(url, "&")
		params := make(map[string]string, len(split))
		for _, v := range split {
			p := strings.SplitN(v, "=", 2)
			params[p[0]] = p[1]
		}

		file := params["file"]
		if len(file) <= 0 {
			log.Fatalf("NewKiteQServer|INVALID|FILE PATH|%s\n", db)
		}
		initval := 10 * 10000
		initcap, ok := params["initcap"]
		if ok {
			v, e := strconv.ParseInt(initcap, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|INIT CAP|%s\n", db)
			}
			initval = int(v)
		}
		max := 50 * 10000
		maxcap, ok := params["maxcap"]
		if ok {
			v, e := strconv.ParseInt(maxcap, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|MAX CAP|%s\n", db)
			}
			max = int(v)
		}
		kitedb = smm.NewKiteMMapStore(file, initval, max)
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

		bus := 1000
		u, ok := params["batchUpdateSize"]
		if ok {
			v, e := strconv.ParseInt(u, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|batchUpdateSize|%s\n", db)
			}
			bus = int(v)
		}

		bds := 1000
		d, ok := params["batchDelSize"]
		if ok {
			v, e := strconv.ParseInt(d, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|batchDelSize|%s\n", db)
			}
			bds = int(v)
		}

		flushPeriod := 1 * time.Second
		fp, ok := params["flushPeriod"]
		if ok {
			v, e := strconv.ParseInt(fp, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|batchDelSize|%s\n", db)
			}
			flushPeriod = time.Duration(v * int64(1*time.Millisecond))
		}

		maxConn := 500
		mc, ok := params["maxConn"]
		if ok {
			v, e := strconv.ParseInt(mc, 10, 32)
			if nil != e {
				log.Fatalf("NewKiteQServer|INVALID|batchDelSize|%s\n", db)
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

		options := smq.MysqlOptions{
			Addr:         master,
			SlaveAddr:    slave,
			DB:           params["db"],
			Username:     username,
			Password:     password,
			BatchUpSize:  bus,
			BatchDelSize: bds,
			FlushPeriod:  flushPeriod,
			MaxIdleConn:  maxConn / 2,
			MaxOpenConn:  maxConn}
		kitedb = smq.NewKiteMysql(options)
	} else {
		log.Fatalf("NewKiteQServer|UNSUPPORT DB PROTOCOL|%s\n", db)
	}

	return kitedb
}
