package file

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

func traverse(oplog *oplog) {
	// log.Printf("------%t", *oplog)
}

func TestAppend(t *testing.T) {
	cleanSnapshot("./snapshot/")
	snapshot := NewMessageStore("./snapshot/", 1, 1, traverse)
	snapshot.Start()
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

	if snapshot.chunkId != 1000000-1 {
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

//test delete
func TestDelete(t *testing.T) {
	cleanSnapshot("./snapshot/")
	snapshot := NewMessageStore("./snapshot/", 1, 1, traverse)
	snapshot.Start()
	var data [4]byte
	for j := 0; j < 10000; j++ {

		snapshot.Append(append(data[:4], []byte{
			byte((j >> 24) & 0xFF),
			byte((j >> 16) & 0xFF),
			byte((j >> 8) & 0xFF),
			byte(j & 0xFF)}...))
	}
	snapshot.Destory()

	time.Sleep(5 * time.Second)

	log.Printf("TestDelete|Append|Complete...")
	// //reload
	nsnapshot := NewMessageStore("./snapshot/", 1, 1, traverse)
	nsnapshot.Start()

	log.Printf("TestDelete|Delete|Start...")

	i := 0
	last := 0
	run := true
	go func() {
		for run {
			log.Printf("tps:%d", (i - last))
			last = i
			time.Sleep(1 * time.Second)
		}
	}()

	for j := 0; j < 100; j++ {
		id := int64(j)
		chunk := nsnapshot.Query(id)
		// log.Printf("TestDelete|Query|%s\n", chunk)
		if nil == chunk || chunk.id != id {
			if nil != chunk {
				log.Printf("TestDelete|Query|FAIL|%d\n", chunk.id)
			} else {
				log.Printf("TestDelete|Query|FAIL|NIL Chunk|%d|%s\n", id, chunk)
			}
			t.Fail()
			continue
		}

		nsnapshot.Delete(id)
		i++
		chunk = nsnapshot.Query(id)
		if nil != chunk {
			t.Fail()
			log.Printf("TestDelete|DELETE-QUERY|FAIL|%s\n", chunk)
			continue
		}
	}
	run = false
	nsnapshot.Destory()
	// cleanSnapshot("./snapshot/")
}

func TestQuery(t *testing.T) {

	cleanSnapshot("./snapshot/")
	snapshot := NewMessageStore("./snapshot/", 1, 1, traverse)
	snapshot.Start()
	var data [512]byte
	for j := 0; j < 1000000; j++ {
		snapshot.Append(append(data[:512], []byte{
			byte((j >> 24) & 0xFF),
			byte((j >> 16) & 0xFF),
			byte((j >> 8) & 0xFF),
			byte(j & 0xFF)}...))
	}

	time.Sleep(10 * time.Second)

	run := true
	i := 0
	j := 0
	last := 0

	go func() {
		for run {
			log.Printf("qps:%d", (j - last))
			last = j
			time.Sleep(1 * time.Second)
		}

	}()

	for ; i < 5000000; i++ {
		id := int64(rand.Intn(100000))
		// id := int64(100)
		chunk := snapshot.Query(id)
		if nil == chunk || chunk.id != id {
			log.Printf("Query|%s|%d\n", chunk, id)
			t.Fail()
			break

		} else {
			// log.Printf("Query|SUCC|%s\n", chunk)
			j++
		}
	}
	run = false

	log.Printf("snapshot|%s|%d\n", snapshot, j)

	snapshot.Destory()
	cleanSnapshot("./snapshot/")
}

func BenchmarkDelete(t *testing.B) {
	t.Logf("BenchmarkDelete|Delete|Start...")
	t.StopTimer()
	cleanSnapshot("./snapshot/")
	snapshot := NewMessageStore("./snapshot/", 1, 1, traverse)
	snapshot.Start()

	for j := 0; j < 1000000; j++ {
		snapshot.Append([]byte(fmt.Sprintf("%d|hello snapshot", j)))
	}

	time.Sleep(2 * time.Second)
	t.StartTimer()

	i := 0
	for ; i < t.N; i++ {
		id := int64(rand.Intn(1000000))
		snapshot.Delete(id)

	}

	t.StopTimer()
	snapshot.Destory()
	cleanSnapshot("./snapshot/")
	t.StartTimer()
	t.Logf("BenchmarkDelete|Delete|END...")
}

func BenchmarkQuery(t *testing.B) {

	log.Printf("BenchmarkQuery|Query|Start...")
	t.StopTimer()
	cleanSnapshot("./snapshot/")
	snapshot := NewMessageStore("./snapshot/", 1, 1, traverse)
	snapshot.Start()
	for j := 0; j < 10000; j++ {
		snapshot.Append([]byte(fmt.Sprintf("%d|hello snapshot", j)))
	}

	time.Sleep(2 * time.Second)
	t.StartTimer()

	i := 0
	for ; i < t.N; i++ {
		id := int64(rand.Intn(100)) + 1
		// id := int64(100)
		chunk := snapshot.Query(id)
		if nil == chunk || chunk.id != id {
			log.Printf("Query|%s\n", chunk)
			t.Fail()
			break
		}
	}

	t.StopTimer()
	snapshot.Destory()
	cleanSnapshot("./snapshot/")
	t.StartTimer()

}

func BenchmarkAppend(t *testing.B) {
	t.StopTimer()
	cleanSnapshot("./snapshot/")
	snapshot := NewMessageStore("./snapshot/", 1, 1, traverse)
	snapshot.Start()
	t.StartTimer()

	for i := 0; i < t.N; i++ {
		snapshot.Append([]byte(fmt.Sprintf("hello snapshot-%d", i)))
	}

	t.StopTimer()
	time.Sleep(5 * time.Second)
	snapshot.Destory()
	cleanSnapshot("./snapshot/")
	t.StartTimer()

}
