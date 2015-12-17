package mysql

import (
	"bytes"
	"reflect"
	"strconv"
	"strings"
)

type batchType uint8

func (s batchType) String() string {
	switch s {
	case 1:
		return "Stmt-Commit"
	case 2:
		return "Stmt-Upate"
	case 3:
		return "Stmt-Delete"
	}
	return "Stmt-Unknown"
}

type batchTypes []batchType

func (s batchTypes) Len() int {
	return len(s)
}

func (s batchTypes) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s batchTypes) Less(i, j int) bool {
	return s[i] <= s[j]
}

const (
	COMMIT batchType = 1
	UPDATE batchType = 2
	DELETE batchType = 3
)

type column struct {
	columnName string
	fieldName  string
	isPK       bool
	isHashKey  bool
	fieldKind  reflect.Kind
}

type sqlwrapper struct {
	tablename       string
	columns         []column
	batchSQL        map[batchType][]string
	queryPrepareSQL []string
	pageQuerySQL    []string
	savePrepareSQL  []string
	msgStatSQL      []string
	dbshard         DbShard
}

func newSqlwrapper(tablename string, dbshard DbShard, i interface{}) *sqlwrapper {

	columns := make([]column, 0, 10)
	//开始反射得到所有的field->column
	r := reflect.TypeOf(i)
	for i := 0; i < r.NumField(); i++ {
		f := r.Field(i)
		tag := f.Tag.Get("db")
		c := column{}
		c.fieldName = f.Name
		c.fieldKind = f.Type.Kind()
		//使用字段名称
		if len(tag) <= 0 {
			c.columnName = f.Name
		} else if tag != "transient" {
			tags := strings.Split(tag, ",")
			c.columnName = tags[0] //column
			if len(tags) > 1 && tags[1] == "pk" {
				c.isPK = true
				c.isHashKey = true
			}
		}
		columns = append(columns, c)
	}

	sw := &sqlwrapper{columns: columns, dbshard: dbshard, tablename: tablename}
	sw.initSQL()
	return sw
}

func (self *sqlwrapper) hashQuerySQL(hashkey string) string {
	return self.queryPrepareSQL[self.dbshard.FindForKey(hashkey)]
}
func (self *sqlwrapper) hashSaveSQL(hashkey string) string {
	return self.savePrepareSQL[self.dbshard.FindForKey(hashkey)]
}
func (self *sqlwrapper) hashCommitSQL(hashkey string) string {
	return self.batchSQL[COMMIT][self.dbshard.FindForKey(hashkey)]
}
func (self *sqlwrapper) hashDeleteSQL(hashkey string) string {
	return self.batchSQL[DELETE][self.dbshard.FindForKey(hashkey)]
}
func (self *sqlwrapper) hashPQSQL(hashkey string) string {
	return self.pageQuerySQL[self.dbshard.FindForKey(hashkey)]
}

func (self *sqlwrapper) initSQL() {

	//query
	buff := make([]byte, 0, 128)
	s := bytes.NewBuffer(buff)
	s.WriteString("select ")
	for i, v := range self.columns {
		s.WriteString(v.columnName)
		if i < len(self.columns)-1 {
			s.WriteString(",")
		}
	}

	s.WriteString(" from ")
	s.WriteString(self.tablename)
	s.WriteString("_{} ")
	s.WriteString(" where message_id=?")
	sql := s.String()

	self.queryPrepareSQL = make([]string, 0, self.dbshard.HashNum())
	for i := 0; i < self.dbshard.HashNum(); i++ {
		st := strconv.Itoa(i)
		self.queryPrepareSQL = append(self.queryPrepareSQL, strings.Replace(sql, "{}", st, -1))
	}

	//save
	s.Reset()
	s.WriteString("insert into ")
	s.WriteString(self.tablename)
	s.WriteString("_{} (")
	for i, v := range self.columns {
		s.WriteString(v.columnName)
		if i < len(self.columns)-1 {
			s.WriteString(",")
		}
	}
	s.WriteString(") ")

	s.WriteString(" values (")
	for i, _ := range self.columns {
		s.WriteString("?")
		if i < len(self.columns)-1 {
			s.WriteString(",")
		}

	}
	s.WriteString(" )")

	sql = s.String()

	self.savePrepareSQL = make([]string, 0, self.dbshard.HashNum())
	for i := 0; i < self.dbshard.HashNum(); i++ {
		st := strconv.Itoa(i)
		self.savePrepareSQL = append(self.savePrepareSQL, strings.Replace(sql, "{}", st, -1))
	}

	//page query

	// select
	// a.message_id,a.header,a.msg_type,a.topic,a.message_type,
	// a.publish_group,a.commit,a.publish_time,a.expired_time,
	// a.deliver_count,a.deliver_limit,a.kite_server,a.fail_groups,a.succ_groups,
	// a.next_deliver_time
	// from kite_msg_3 a
	// inner join (
	// select message_id
	// from kite_msg_3 force index(idx_recover)
	// where
	// kite_server='vm-golang001.vm.momo.com' and deliver_count< deliver_limit
	// and expired_time>=1428056089 and next_deliver_time<=1428055512
	// order by next_deliver_time asc  limit 28500,51) b using (message_id);

	s.Reset()
	s.WriteString("select  ")
	for i, v := range self.columns {
		//如果为Body字段则不用于查询
		if v.columnName == "body" {
			continue
		}
		s.WriteString("a.")
		s.WriteString(v.columnName)
		if i < len(self.columns)-1 {
			s.WriteString(",")
		}
	}
	s.WriteString(" from ")
	s.WriteString(self.tablename)
	s.WriteString("_{} a ")
	s.WriteString("  inner join ") //强制使用idx_recover索引
	s.WriteString(" ( select  message_id  from ")
	s.WriteString(self.tablename)
	s.WriteString("_{}  ")
	s.WriteString(" force index(idx_recover) ")
	s.WriteString(" where kite_server=? and deliver_count<deliver_limit and expired_time>=? and next_deliver_time<=? ")
	s.WriteString(" order by next_deliver_time asc  limit ?,?) b")
	s.WriteString(" using (message_id) ")

	sql = s.String()

	self.pageQuerySQL = make([]string, 0, self.dbshard.HashNum())
	for i := 0; i < self.dbshard.HashNum(); i++ {
		st := strconv.Itoa(i)
		self.pageQuerySQL = append(self.pageQuerySQL, strings.Replace(sql, "{}", st, -1))
	}

	//--------------batchOps

	self.batchSQL = make(map[batchType][]string, 4)
	//commit
	s.Reset()
	s.WriteString("update ")
	s.WriteString(self.tablename)
	s.WriteString("_{} ")
	s.WriteString(" set commit=? ")
	s.WriteString(" where message_id=?")

	sql = s.String()

	self.batchSQL[COMMIT] = make([]string, 0, self.dbshard.HashNum())
	for i := 0; i < self.dbshard.HashNum(); i++ {
		st := strconv.Itoa(i)
		self.batchSQL[COMMIT] = append(self.batchSQL[COMMIT], strings.Replace(sql, "{}", st, -1))
	}

	//delete
	s.Reset()
	s.WriteString("delete from  ")
	s.WriteString(self.tablename)
	s.WriteString("_{} ")
	s.WriteString(" where message_id=?")

	sql = s.String()

	self.batchSQL[DELETE] = make([]string, 0, self.dbshard.HashNum())
	for i := 0; i < self.dbshard.HashNum(); i++ {
		st := strconv.Itoa(i)
		self.batchSQL[DELETE] = append(self.batchSQL[DELETE], strings.Replace(sql, "{}", st, -1))
	}

	//batch update
	s.Reset()
	s.WriteString("update ")
	s.WriteString(self.tablename)
	s.WriteString("_{} ")
	s.WriteString(" set succ_groups=?,fail_groups=?,next_deliver_time=?,deliver_count=? ")
	s.WriteString(" where message_id=?")

	sql = s.String()

	self.batchSQL[UPDATE] = make([]string, 0, self.dbshard.HashNum())
	for i := 0; i < self.dbshard.HashNum(); i++ {
		st := strconv.Itoa(i)
		self.batchSQL[UPDATE] = append(self.batchSQL[UPDATE], strings.Replace(sql, "{}", st, -1))
	}

	//----------- 查询本机消息堆积数

	// select
	// 		topic,count(message_id) total
	// from kite_msg_3
	// where
	//		kite_server='vm-golang001.vm.momo.com' group by topic;

	s.Reset()
	s.WriteString("select topic,count(message_id) total ")
	s.WriteString(" from ")
	s.WriteString(self.tablename)
	s.WriteString("_{} ")
	s.WriteString(" where kite_server=? group by topic")

	sql = s.String()

	self.msgStatSQL = make([]string, 0, self.dbshard.HashNum())
	for i := 0; i < self.dbshard.HashNum(); i++ {
		st := strconv.Itoa(i)
		self.msgStatSQL = append(self.msgStatSQL, strings.Replace(sql, "{}", st, -1))
	}

}
