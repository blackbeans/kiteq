package mysql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"strconv"
	// "time"
)

const (
	SHARD_SEED = 16
)

type shardrange struct {
	min     int
	max     int
	shardId int
	master  *sql.DB
	slave   *sql.DB
}

type DbShard struct {
	shardNum    int
	hashNum     int
	shardranges []shardrange
}

func newDbShard(options MysqlOptions) DbShard {
	hash := SHARD_SEED / options.ShardNum

	shardranges := make([]shardrange, 0, hash)
	for i := 0; i < options.ShardNum; i++ {

		//创建shard的db
		master := openDb(
			options.Username+":"+options.Password+"@tcp("+options.Addr+")/"+options.DB,
			i,
			options.MaxIdleConn, options.MaxOpenConn)
		slave := master
		if len(options.SlaveAddr) > 0 {
			slave = openDb(
				options.Username+":"+options.Password+"@tcp("+options.SlaveAddr+")/"+options.DB,
				i,
				options.MaxIdleConn, options.MaxOpenConn)
		}
		shardranges = append(shardranges, shardrange{i * hash, (i + 1) * hash, i, master, slave})
	}

	return DbShard{options.ShardNum, hash, shardranges}
}

func openDb(addr string, shardId int, idleConn, maxConn int) *sql.DB {
	db, err := sql.Open("mysql", addr+"_"+strconv.Itoa(shardId)+"?timeout=30s&readTimeout=30s")
	if err != nil {
		log.Errorf("NewKiteMysql|CONNECT FAIL|%s|%s", err, addr)
		panic(err)
	}
	db.SetMaxIdleConns(idleConn)
	db.SetMaxOpenConns(maxConn)
	// db.SetConnMaxLifetime(5 * time.Minute)
	return db
}

func (s DbShard) FindForShard(key string) shardrange {

	i := s.HashId(key)
	for _, v := range s.shardranges {
		if v.min <= i && v.max > i {
			return v
		}
	}
	return s.shardranges[0]

}

func (s DbShard) FindForKey(key string) int {
	return s.HashId(key) % s.hashNum
}

func (s DbShard) FindSlave(key string) *sql.DB {
	return s.FindForShard(key).slave
}

func (s DbShard) FindMaster(key string) *sql.DB {
	return s.FindForShard(key).master
}

func (s DbShard) FindShardById(id int) shardrange {
	for _, v := range s.shardranges {
		if v.min <= id && v.max > id {
			return v
		}
	}
	return s.shardranges[0]
}

func (s DbShard) HashId(key string) int {
	num := key
	if len(key) > 1 {
		num = string(key[len(key)-1:])
	}

	i, err := strconv.ParseInt(num, 16, 16)
	if nil != err {
		log.Errorf("DbShard|HashId|INVALID HASHKEY|%s|%s", key, err)
		return 0
	}
	return int(i)
}

func (s DbShard) ShardNum() int {
	return s.shardNum
}

func (s DbShard) HashNum() int {
	return s.hashNum
}

func (s DbShard) Stop() {
	for _, v := range s.shardranges {
		v.master.Close()
		v.slave.Close()
	}
}
