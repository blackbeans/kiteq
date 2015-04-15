package memory

import (
	"fmt"
	"testing"
	"time"
)

func TestAppend(t *testing.T) {
	snapshot := NewMemorySnapshot("./snapshot/", "kiteq")
	for i := 0; i < 4; i++ {
		snapshot.Append([]byte(fmt.Sprintf("hello snapshot|%d", i)))
	}
	time.Sleep(10 * time.Second)
	snapshot.Destory()
}

func BenchmarkAppend(t *testing.B) {
	snapshot := NewMemorySnapshot("./snapshot/", "kiteq")
	for i := 0; i < t.N; i++ {
		snapshot.Append([]byte(fmt.Sprintf("hello snapshot-%d", i)))
	}
	snapshot.Destory()
	t.Log(snapshot)

}
