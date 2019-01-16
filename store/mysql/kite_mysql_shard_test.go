package mysql

import (
	"regexp"
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
		t.Logf("FAIL|FindForShard|26c03f00665862591f696a980b5a6c4f|%d\n", fk.shardId)
		return
	}

	hash := hs.FindForKey("26c03f00665862591f696a980b5a6c4f")
	if hash != 3 {
		t.Fail()
		t.Logf("FAIL|FindForKey|26c03f00665862591f696a980b5a6c4f|%d\n", hash)
	}

	// !regexp.MatchString(, id)
	rc := regexp.MustCompile("[0-9a-f]{32}")
	match := rc.MatchString("26c03f00665862591f696a980b5a6c4f")
	if !match {
		t.Fail()
		t.Log("MatchString|26c03f00665862591f696a980b5a6c4f|FAIL")
	}

	match = rc.MatchString("26c03f006-65862591f696a980b5a6c4")
	if match {
		t.Fail()
		t.Log("MatchString|26c03f006-65862591f696a980b5a6c4|FAIL")
	}

	t.Logf("FindForShard|%d\n", fk)

	sc := hs.ShardNum()
	if sc != 4 {
		t.Fail()
	}

	t.Logf("ShardNum|%d\n", sc)
}
