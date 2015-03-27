package mysql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"kiteq/protocol"
	. "kiteq/store"
	"log"
	"time"
)

//mysql的参数
type MysqlOptions struct {
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
	db           *sql.DB
	dbslave      *sql.DB
	batchUpChan  []chan *MessageEntity
	batchDelChan []chan string
	batchComChan []chan string
	batchUpSize  int
	batchDelSize int
	flushPeriod  time.Duration
}

func NewKiteMysql(options MysqlOptions) *KiteMysqlStore {

	master := openDb(
		options.Username+":"+options.Password+"@tcp("+options.Addr+")/"+options.DB,
		options.MaxIdleConn, options.MaxOpenConn)
	slave := master
	if len(options.SlaveAddr) > 0 {
		slave = openDb(
			options.Username+":"+options.Password+"@tcp("+options.SlaveAddr+")/"+options.DB,
			options.MaxIdleConn, options.MaxOpenConn)
	}

	sqlwrapper := newSqlwrapper("kite_msg", HashShard{}, MessageEntity{})

	//创建Hash的channel
	batchDelChan := make([]chan string, 0, sqlwrapper.hashshard.ShardCnt())
	batchUpChan := make([]chan *MessageEntity, 0, sqlwrapper.hashshard.ShardCnt())
	batchComChan := make([]chan string, 0, sqlwrapper.hashshard.ShardCnt())
	for i := 0; i < sqlwrapper.hashshard.ShardCnt(); i++ {
		batchUpChan = append(batchUpChan, make(chan *MessageEntity, options.BatchUpSize/sqlwrapper.hashshard.ShardCnt()))
		batchDelChan = append(batchDelChan, make(chan string, options.BatchDelSize/sqlwrapper.hashshard.ShardCnt()))
		batchComChan = append(batchComChan, make(chan string, options.BatchUpSize/sqlwrapper.hashshard.ShardCnt()))
	}

	ins := &KiteMysqlStore{
		db:           master,
		dbslave:      slave,
		convertor:    convertor{columns: sqlwrapper.columns},
		sqlwrapper:   sqlwrapper,
		batchUpChan:  batchUpChan,
		batchUpSize:  cap(batchDelChan),
		batchDelChan: batchDelChan,
		batchDelSize: cap(batchDelChan),
		batchComChan: batchComChan,
		flushPeriod:  options.FlushPeriod}

	log.Printf("NewKiteMysql|KiteMysqlStore|SUCC|%s|%s...\n", options.Addr, options.SlaveAddr)
	ins.start()
	return ins
}

func openDb(addr string, idleConn, maxConn int) *sql.DB {
	db, err := sql.Open("mysql", addr)
	if err != nil {
		log.Panicf("NewKiteMysql|CONNECT FAIL|%s|%s\n", err, addr)
	}

	db.SetMaxIdleConns(idleConn)
	db.SetMaxOpenConns(maxConn)
	return db
}

var filternothing = func(colname string) bool {
	return false
}

func (self *KiteMysqlStore) Query(messageId string) *MessageEntity {

	var entity *MessageEntity
	s := self.sqlwrapper.hashQuerySQL(messageId)
	rows, err := self.dbslave.Query(s, messageId)
	if nil != err {
		log.Printf("KiteMysqlStore|Query|FAIL|%s|%s\n", err, messageId)
		return nil
	}
	defer rows.Close()

	if rows.Next() {

		entity = &MessageEntity{}
		fc := self.convertor.convertFields(entity, filternothing)
		err := rows.Scan(fc...)
		if nil != err {
			log.Printf("KiteMysqlStore|Query|SCAN|FAIL|%s|%s\n", err, messageId)
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
	result, err := self.db.Exec(s, fvs...)
	if err != nil {
		log.Printf("KiteMysqlStore|SAVE|FAIL|%s|%s\n", err, entity.MessageId)
		return false
	}

	num, _ := result.RowsAffected()
	return num == 1
}

func (self *KiteMysqlStore) Commit(messageId string) bool {
	return self.AsyncCommit(messageId)
	// s := self.sqlwrapper.hashCommitSQL(messageId)
	// result, err := self.db.Exec(s, 1, messageId)
	// if err != nil {
	// 	log.Printf("KiteMysqlStore|Commit|FAIL|%s|%s\n", err, messageId)
	// 	return false
	// }
	// num, _ := result.RowsAffected()
	// return true
}

func (self *KiteMysqlStore) Rollback(messageId string) bool {
	return self.Delete(messageId)
}

func (self *KiteMysqlStore) Delete(messageId string) bool {

	// s := self.sqlwrapper.hashDeleteSQL(messageId)
	// result, err := self.db.Exec(s, messageId)
	// if err != nil {
	// 	log.Printf("KiteMysqlStore|Delete|FAIL|%s|%s\n", err, messageId)
	// 	return false
	// }
	// num, _ := result.RowsAffected()
	// return num == 1
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
	rows, err := self.dbslave.Query(s, kiteServer, time.Now().Unix(), nextDeliveryTime, startIdx, limit+1)
	if err != nil {
		log.Printf("KiteMysqlStore|Query|FAIL|%s|%s\n", err, hashKey)
		return false, nil
	}
	defer rows.Close()

	results := make([]*MessageEntity, 0, limit)
	for rows.Next() {

		entity := &MessageEntity{}
		fc := self.convertor.convertFields(entity, filterbody)
		err := rows.Scan(fc...)
		if err != nil {
			log.Printf("KiteMysqlStore|PageQueryEntity|FAIL|%s|%s|%d|%d\n", err, kiteServer, nextDeliveryTime, startIdx)
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
