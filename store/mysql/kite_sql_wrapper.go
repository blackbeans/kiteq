package mysql

import (
	"bytes"
	"reflect"
	"strconv"
	"strings"
)

type column struct {
	columnName string
	fieldName  string
	isPK       bool
	isHashKey  bool
	fieldKind  reflect.Kind
}

type sqlwrapper struct {
	tablename           string
	columns             []column
	queryPrepareSQL     []string
	savePrepareSQL      []string
	commitPrepareSQL    []string
	deletePrepareSQL    []string
	pageQueryPrepareSQL []string
	hashshard           HashShard
}

func newSqlwrapper(tablename string, hashshard HashShard, i interface{}) *sqlwrapper {

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

	sw := &sqlwrapper{columns: columns, hashshard: hashshard, tablename: tablename}
	sw.initSQL()
	return sw
}

func (self *sqlwrapper) hashQuerySQL(hashkey string) string {
	return self.queryPrepareSQL[self.hashshard.FindForKey(hashkey)]
}
func (self *sqlwrapper) hashSaveSQL(hashkey string) string {
	return self.savePrepareSQL[self.hashshard.FindForKey(hashkey)]
}
func (self *sqlwrapper) hashCommitSQL(hashkey string) string {
	return self.commitPrepareSQL[self.hashshard.FindForKey(hashkey)]
}
func (self *sqlwrapper) hashDeleteSQL(hashkey string) string {
	return self.deletePrepareSQL[self.hashshard.FindForKey(hashkey)]
}
func (self *sqlwrapper) hashPQSQL(hashkey string) string {
	return self.pageQueryPrepareSQL[self.hashshard.FindForKey(hashkey)]
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

	self.queryPrepareSQL = make([]string, 0, self.hashshard.ShardCnt())
	for i := 0; i < self.hashshard.ShardCnt(); i++ {
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

	self.savePrepareSQL = make([]string, 0, self.hashshard.ShardCnt())
	for i := 0; i < self.hashshard.ShardCnt(); i++ {
		st := strconv.Itoa(i)
		self.savePrepareSQL = append(self.savePrepareSQL, strings.Replace(sql, "{}", st, -1))
	}

	//commit
	s.Reset()
	s.WriteString("update ")
	s.WriteString(self.tablename)
	s.WriteString("_{} ")
	s.WriteString(" set commit=? ")
	s.WriteString(" where message_id=?")

	sql = s.String()

	self.commitPrepareSQL = make([]string, 0, self.hashshard.ShardCnt())
	for i := 0; i < self.hashshard.ShardCnt(); i++ {
		st := strconv.Itoa(i)
		self.commitPrepareSQL = append(self.commitPrepareSQL, strings.Replace(sql, "{}", st, -1))
	}

	//delete
	s.Reset()
	s.WriteString("delete from  ")
	s.WriteString(self.tablename)
	s.WriteString("_{} ")
	s.WriteString(" where message_id=?")

	sql = s.String()

	self.deletePrepareSQL = make([]string, 0, self.hashshard.ShardCnt())
	for i := 0; i < self.hashshard.ShardCnt(); i++ {
		st := strconv.Itoa(i)
		self.deletePrepareSQL = append(self.deletePrepareSQL, strings.Replace(sql, "{}", st, -1))
	}

	//page query
	s.Reset()
	s.WriteString("select  ")
	for i, v := range self.columns {
		//如果为Body字段则不用于查询
		if v.columnName == "body" {
			continue
		}
		s.WriteString(v.columnName)
		if i < len(self.columns)-1 {
			s.WriteString(",")
		}
	}
	s.WriteString(" from ")
	s.WriteString(self.tablename)
	s.WriteString("_{} ")
	s.WriteString(" where kite_server=? and deliver_count<deliver_limit and expired_time>=? and next_deliver_time<=? order by next_deliver_time asc  limit ?,?")

	sql = s.String()

	self.pageQueryPrepareSQL = make([]string, 0, self.hashshard.ShardCnt())
	for i := 0; i < self.hashshard.ShardCnt(); i++ {
		st := strconv.Itoa(i)
		self.pageQueryPrepareSQL = append(self.pageQueryPrepareSQL, strings.Replace(sql, "{}", st, -1))
	}
}
