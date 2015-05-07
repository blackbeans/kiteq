package file

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

func traverse(oplog *oplog) {
	log.Printf("------%d", oplog.ChunkId)
}

func TestAppend(t *testing.T) {
	cleanSnapshot("./snapshot/")
	snapshot := NewMessageStore("./snapshot/", 1, 1, 1*time.Second, traverse)
	snapshot.Start()
	run := true
	i := 0
	last := 0

	go func() {
		for ; i < 1000000; i++ {
			cmd := NewCommand(-1, fmt.Sprint(i), []byte(fmt.Sprintf("hello snapshot|%d", i)), nil)
			snapshot.Append(cmd)
		}
		run = false
	}()

	for run {
		log.Printf("tps:%d", (i - last))
		last = i
		time.Sleep(1 * time.Second)
	}
	time.Sleep(10 * time.Second)
	log.Printf("snapshot|%s", snapshot)

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
	snapshot := NewMessageStore("./snapshot/", 1, 1, 1*time.Second, traverse)
	snapshot.Start()
	for j := 0; j < 10000; j++ {
		d := []byte(fmt.Sprint(j))
		cmd := NewCommand(-1, fmt.Sprint(j), d, nil)
		snapshot.Append(cmd)
	}
	snapshot.Destory()

	time.Sleep(5 * time.Second)

	log.Printf("TestDelete|Append|Complete...")
	// //reload
	nsnapshot := NewMessageStore("./snapshot/", 1, 1, 1*time.Second, traverse)
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
		var data int32
		err := nsnapshot.Query(id, &data)
		if nil != err {
			log.Printf("TestDelete|Query|%s\n", err)
		}

		if data != int32(j) {
			log.Printf("TestDelete|Query|FAIL|%d", data)
			t.Fail()
			continue
		}

		c := NewCommand(id, "", nil, nil)
		nsnapshot.Delete(c)
		i++
		err = nsnapshot.Query(id, &data)
		if nil == err {
			t.Fail()
			log.Printf("TestDelete|DELETE-QUERY|FAIL|%s", data)
			continue
		}
	}
	run = false
	nsnapshot.Destory()
	cleanSnapshot("./snapshot/")
}

func TestQuery(t *testing.T) {

	cleanSnapshot("./snapshot/")
	snapshot := NewMessageStore("./snapshot/", 1, 1, 1*time.Second, traverse)
	snapshot.Start()
	var data [512]byte
	for j := 0; j < 1000000; j++ {
		d := append(data[:512], []byte{
			byte((j >> 24) & 0xFF),
			byte((j >> 16) & 0xFF),
			byte((j >> 8) & 0xFF),
			byte(j & 0xFF)}...)
		v, err := json.Marshal(d)
		if nil != err {
			log.Printf("Query|Marshal|FAIL|%s\n", err)
		}
		cmd := NewCommand(-1, fmt.Sprint(j), v, nil)
		snapshot.Append(cmd)
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

	d := ""
	for ; i < 5000000; i++ {
		id := int64(rand.Intn(100000))

		err := snapshot.Query(id, &d)
		if nil != err {
			log.Printf("Query|%s|%d\n", err, id)
			t.Fail()
			break

		} else {
			// log.Printf("Query|SUCC|%d\n", id)
			j++
		}
	}

	err := snapshot.Query(94921, &d)
	if nil != err {
		log.Printf("Query|%s|%d\n", err, 94921)
		t.Fail()

	}

	err = snapshot.Query(0, &d)
	if nil != err {
		log.Printf("Query|%s|%d\n", err, 0)
		t.Fail()
	}

	run = false

	log.Printf("snapshot|%s|%d\n", snapshot, j)

	snapshot.Destory()
	// cleanSnapshot("./snapshot/")
}

func BenchmarkDelete(t *testing.B) {
	t.Logf("BenchmarkDelete|Delete|Start...")
	t.StopTimer()
	cleanSnapshot("./snapshot/")
	snapshot := NewMessageStore("./snapshot/", 1, 1, 1*time.Second, traverse)
	snapshot.Start()

	for j := 0; j < 1000000; j++ {
		d := []byte(fmt.Sprintf("%d|hello snapshot", j))
		cmd := NewCommand(-1, fmt.Sprint(j), d, nil)
		snapshot.Append(cmd)
	}

	time.Sleep(2 * time.Second)
	t.StartTimer()

	i := 0
	for ; i < t.N; i++ {
		id := int64(rand.Intn(1000000))
		cmd := NewCommand(id, "", nil, nil)
		snapshot.Delete(cmd)

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
	snapshot := NewMessageStore("./snapshot/", 1, 1, 1*time.Second, traverse)
	snapshot.Start()
	for j := 0; j < 10000; j++ {
		d := []byte(fmt.Sprintf("%d|hello snapshot", j))
		cmd := NewCommand(-1, fmt.Sprint(j), d, nil)
		snapshot.Append(cmd)
	}

	time.Sleep(2 * time.Second)
	t.StartTimer()

	i := 0
	for ; i < t.N; i++ {
		id := int64(rand.Intn(100)) + 1
		var a string
		err := snapshot.Query(id, &a)
		if nil != err {
			log.Printf("Query|%s|%d\n", err, id)
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
	snapshot := NewMessageStore("./snapshot/", 1, 1, 1*time.Second, traverse)
	snapshot.Start()
	t.StartTimer()

	for i := 0; i < t.N; i++ {
		d := []byte(fmt.Sprintf("hello snapshot-%d", i))
		cmd := NewCommand(-1, fmt.Sprint(i), d, nil)
		snapshot.Append(cmd)
	}

	t.StopTimer()
	time.Sleep(5 * time.Second)
	snapshot.Destory()
	cleanSnapshot("./snapshot/")
	t.StartTimer()

}
