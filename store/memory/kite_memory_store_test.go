package memory

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestAppend(t *testing.T) {
	cleanSnapshot("./snapshot/")
	snapshot := NewMemorySnapshot("./snapshot/", "kiteq", 1, 1)

	run := true
	i := 0
	last := 0

	go func() {
		for ; i < 1000000; i++ {
			snapshot.Append([]byte(fmt.Sprintf("hello snapshot|%d", i)))
		}
		run = false
	}()

	for run {
		log.Printf("tps:%d", (i - last))
		last = i
		time.Sleep(1 * time.Second)
	}
	time.Sleep(10 * time.Second)
	t.Logf("snapshot|%s", snapshot)

	if snapshot.chunkId != 1000000 {
		t.Fail()
	}
	snapshot.Destory()
	cleanSnapshot("./snapshot/")
}

func cleanSnapshot(path string) {

	err := os.RemoveAll(path)
	if nil != err {
		log.Printf("Remove|FAIL|%s\n", path)
	} else {
		log.Printf("Remove|SUCC|%s\n", path)
	}

}

func TestQuery(t *testing.T) {

	cleanSnapshot("./snapshot/")
	snapshot := NewMemorySnapshot("./snapshot/", "kiteq", 1, 1)

	for j := 0; j < 1000; j++ {
		snapshot.Append([]byte(fmt.Sprintf("%d|hello snapshot", j)))
	}

	time.Sleep(10 * time.Second)

	run := true
	i := 0
	j := 0
	last := 0

	go func() {
		for ; i < 100; i++ {
			id := int64(rand.Intn(50))
			// id := int64(100)
			chunk := snapshot.Query(id)
			if nil == chunk || chunk.id != id {
				log.Printf("Query|%s\n", chunk)
				t.Fail()
				break
			} else {
				// log.Printf("Query|SUCC|%s\n", chunk)
				j++
			}
		}
		run = false
	}()

	for run {
		log.Printf("qps:%d", (j - last))
		last = j
		time.Sleep(1 * time.Second)
	}
	time.Sleep(10 * time.Second)
	t.Logf("snapshot|%s", snapshot)

	snapshot.Destory()
	cleanSnapshot("./snapshot/")
}

func BenchmarkAppend(t *testing.B) {
	snapshot := NewMemorySnapshot("./snapshot/", "kiteq", 1, 1)
	for i := 0; i < t.N; i++ {
		snapshot.Append([]byte(fmt.Sprintf("hello snapshot-%d", i)))
	}
	snapshot.Destory()
	t.Log(snapshot)
	cleanSnapshot("./snapshot/")

}
