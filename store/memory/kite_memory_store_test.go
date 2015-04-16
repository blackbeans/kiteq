package memory

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func TestAppend(t *testing.T) {
	snapshot := NewMemorySnapshot("./snapshot/", "kiteq", 1, 1)

	run := true
	i := 0
	last := 0

	go func() {
		for ; i < 100000; i++ {
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

	if snapshot.chunkId != 100000 {
		t.Fail()
	}
	snapshot.Destory()
}

func BenchmarkAppend(t *testing.B) {
	snapshot := NewMemorySnapshot("./snapshot/", "kiteq", 1, 1)
	for i := 0; i < t.N; i++ {
		snapshot.Append([]byte(fmt.Sprintf("hello snapshot-%d", i)))
	}
	snapshot.Destory()
	t.Log(snapshot)

}
