package mysql

import (
	"log"
	"strconv"
)

type HashShard struct {
}

func (s *HashShard) FindForKey(key interface{}) int {
	hashId, ok := key.(string)
	if ok {
		i, err := strconv.ParseInt(string(hashId[len(hashId)-1]), 16, 8)
		if nil != err {
			log.Printf("HashShard|FindForKey|INVALID HASHKEY|%s\n", key)
		}
		return int(i) % 16
	}
	panic("INVALID MESSAGE ID !")
}

func (s *HashShard) ShardCnt() int {
	return 16
}
