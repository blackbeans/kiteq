package parser

import (
	"context"
	"testing"
)

func TestParse(t *testing.T) {

	storeUrls := []string{
		"mock://",
		"memory://?initcap=1000&maxcap=2000",
		"mysql://master:3306,slave:3306?db=kite&username=root&password=root&maxConn=500&batchUpdateSize=1000&batchDelSize=1000&flushSeconds=1",
		"file:///path?cap=10000000&checkSeconds=60&flushBatchSize=1000",
		"rocksdb:///data/rocksdb/",
	}
	ctx := context.TODO()
	for _, store := range storeUrls {
		ParseDB(ctx, store, "kiteq001")
	}

}
