package mysql

import (
	"fmt"
	log "github.com/blackbeans/log4go"
	"kiteq/protocol"
	. "kiteq/store"
	"time"
)

//mysql的参数
type MysqlOptions struct {
	ShardNum                  int //分库的数量
	Addr                      string
	SlaveAddr                 string
	DB                        string
	Username, Password        string
	BatchUpSize, BatchDelSize int
	FlushPeriod               time.Duration
	MaxIdleConn               int
	MaxOpenConn               int
}

type KiteMysqlStore struct {
	convertor    convertor
	sqlwrapper   *sqlwrapper
	dbshard      DbShard
	batchUpChan  []chan *MessageEntity
	batchDelChan []chan string
	batchComChan []chan string
	batchUpSize  int
	batchDelSize int
	flushPeriod  time.Duration
	stmtPools    map[batchType][][]*StmtPool //第一层dblevel 第二维table level
	stop         bool
}

func NewKiteMysql(options MysqlOptions) *KiteMysqlStore {

	shard := newDbShard(options)

	sqlwrapper := newSqlwrapper("kite_msg", shard, MessageEntity{})

	ins := &KiteMysqlStore{
		dbshard:      shard,
		convertor:    convertor{columns: sqlwrapper.columns},
		sqlwrapper:   sqlwrapper,
		batchUpSize:  options.BatchUpSize,
		batchDelSize: options.BatchDelSize,
		flushPeriod:  options.FlushPeriod,
		stop:         false}
	ins.Start()

	log.Info("NewKiteMysql|KiteMysqlStore|SUCC|%s|%s...\n", options.Addr, options.SlaveAddr)
	return ins
}

var filternothing = func(colname string) bool {
	return false
}

func (self *KiteMysqlStore) Monitor() string {
	line := "Stmt-Pool\t"
	for k, v := range self.stmtPools {
		numWork := 0
		active := 0
		idle := 0

		for _, t := range v {
			for _, p := range t {
				n, a, i := p.MonitorPool()
				numWork += n
				active += a
				idle += i
			}
		}

		line +=
			fmt.Sprintf("%s[work:%d\tactive:%d\tidle:%d]\t", k, numWork, active, idle)
	}
	return line
}

func (self *KiteMysqlStore) Query(messageId string) *MessageEntity {

	var entity *MessageEntity
	s := self.sqlwrapper.hashQuerySQL(messageId)
	rows, err := self.dbshard.FindSlave(messageId).Query(s, messageId)
	if nil != err {
		log.Error("KiteMysqlStore|Query|FAIL|%s|%s\n", err, messageId)
		return nil
	}
	defer rows.Close()

	if rows.Next() {

		entity = &MessageEntity{}
		fc := self.convertor.convertFields(entity, filternothing)
		err := rows.Scan(fc...)
		if nil != err {
			log.Error("KiteMysqlStore|Query|SCAN|FAIL|%s|%s\n", err, messageId)
			return nil
		}
		self.convertor.Convert2Entity(fc, entity, filternothing)
		switch entity.MsgType {
		case protocol.CMD_BYTES_MESSAGE:
			//do nothing
		case protocol.CMD_STRING_MESSAGE:
			entity.Body = string(entity.GetBody().([]byte))
		}
	}

	return entity
}

func (self *KiteMysqlStore) Save(entity *MessageEntity) bool {
	fvs := self.convertor.Convert2Params(entity)
	s := self.sqlwrapper.hashSaveSQL(entity.MessageId)
	result, err := self.dbshard.FindMaster(entity.MessageId).Exec(s, fvs...)
	if err != nil {
		log.Error("KiteMysqlStore|SAVE|FAIL|%s|%s\n", err, entity.MessageId)
		return false
	}

	num, _ := result.RowsAffected()
	return num == 1
}

func (self *KiteMysqlStore) Commit(messageId string) bool {
	return self.AsyncCommit(messageId)
}

func (self *KiteMysqlStore) Rollback(messageId string) bool {
	return self.Delete(messageId)
}

func (self *KiteMysqlStore) Delete(messageId string) bool {
	return self.AsyncDelete(messageId)
}

var filterbody = func(colname string) bool {
	//不需要查询body
	return colname == "body"
}

//没有body的entity
func (self *KiteMysqlStore) PageQueryEntity(hashKey string, kiteServer string, nextDeliveryTime int64, startIdx, limit int) (bool, []*MessageEntity) {

	s := self.sqlwrapper.hashPQSQL(hashKey)
	// log.Println(s)
	rows, err := self.dbshard.FindSlave(hashKey).
		Query(s, kiteServer, time.Now().Unix(), nextDeliveryTime, startIdx, limit+1)
	if err != nil {
		log.Error("KiteMysqlStore|Query|FAIL|%s|%s\n", err, hashKey)
		return false, nil
	}
	defer rows.Close()

	results := make([]*MessageEntity, 0, limit)
	for rows.Next() {

		entity := &MessageEntity{}
		fc := self.convertor.convertFields(entity, filterbody)
		err := rows.Scan(fc...)
		if err != nil {
			log.Error("KiteMysqlStore|PageQueryEntity|FAIL|%s|%s|%d|%d\n", err, kiteServer, nextDeliveryTime, startIdx)
		} else {

			self.convertor.Convert2Entity(fc, entity, filterbody)
			results = append(results, entity)
		}
	}

	if len(results) > limit {
		return true, results[:limit]
	} else {
		return false, results
	}
}
