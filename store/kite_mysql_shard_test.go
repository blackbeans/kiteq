package store

import (
	"testing"
)

func TestHash(t *testing.T) {
	hs := HashShard{}

	fk := hs.FindForKey("26c03f00665862591f696a980b5a6c4f")
	if fk != 15 {
		t.Fail()
	}

	t.Logf("FindForKey|%d\n", fk)

	sc := hs.ShardCnt()
	if sc != 16 {
		t.Fail()
	}

	t.Logf("ShardCnt|%d\n", sc)
}
