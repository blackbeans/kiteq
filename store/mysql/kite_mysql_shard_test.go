package mysql

import (
	"testing"
	"time"
)

func TestHash(t *testing.T) {

	options := MysqlOptions{
		Addr:         "localhost:3306",
		Username:     "root",
		Password:     "",
		ShardNum:     4,
		BatchUpSize:  1000,
		BatchDelSize: 1000,
		FlushPeriod:  1 * time.Minute,
		MaxIdleConn:  2,
		MaxOpenConn:  4}

	hs := newDbShard(options)

	fk := hs.FindForShard("26c03f00665862591f696a980b5a6c4f")
	if fk.shardId != 3 {
		t.Fail()
	}

	t.Logf("FindForShard|%d\n", fk)

	sc := hs.ShardNum()
	if sc != 4 {
		t.Fail()
	}

	t.Logf("ShardNum|%d\n", sc)
}
