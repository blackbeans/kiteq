package file

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func traverse(oplog *oplog) {
	log.Printf("------%v", oplog)
}

func TestSingle(t *testing.T) {
	cleanSnapshot("./snapshot/")
	snapshot := NewMessageStore("./snapshot/", 1, 10, traverse)
	snapshot.Start()

	for i := 0; i < 2; i++ {
		cmd := NewCommand(-1, fmt.Sprintln(i), []byte{0}, []byte{1})
		<-snapshot.Append(cmd)

	}

	log.Printf("snapshot|%s", snapshot)

	for i := 0; i < 2; i++ {
		snapshot.Delete(NewCommand(int64(i), fmt.Sprintln(i), []byte{0}, []byte{1}))
	}
	time.Sleep(2 * time.Second)
	snapshot.Destory()

	snapshot = NewMessageStore("./snapshot/", 1, 10, traverse)
	snapshot.Start()

	cleanSnapshot("./snapshot/")
}

func TestAppend(t *testing.T) {
	cleanSnapshot("./snapshot/")
	snapshot := NewMessageStore("./snapshot/", 1, 10, traverse)
	snapshot.Start()
	run := true
	i := 0
	last := 0

	go func() {
		for ; i < 20; i++ {
			cmd := NewCommand(-1, fmt.Sprint(i), []byte(fmt.Sprintf("hello snapshot|%d", i)), nil)
			<-snapshot.Append(cmd)
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

	if snapshot.chunkId != 20-1 {
		t.Fail()
	}
	snapshot.Destory()
	cleanSnapshot("./snapshot/")
}

func cleanSnapshot(path string) {

	err := os.RemoveAll(path)
	if nil != err {
		log.Printf("Remove|FAIL|%s", path)
	} else {
		log.Printf("Remove|SUCC|%s", path)
	}

}

//test delete
func TestDeleteAndStart(t *testing.T) {
	cleanSnapshot("./snapshot/")
	snapshot := NewMessageStore("./snapshot/", 1, 10, traverse)
	snapshot.Start()
	for j := 0; j < 1000; j++ {
		d := []byte(fmt.Sprintln(j))
		cmd := NewCommand(-1, fmt.Sprintln(j), d, nil)
		<-snapshot.Append(cmd)
		// log.Printf("TestDelete|Append|%d|...", j)
	}
	log.Printf("TestDeleteAndStart|Delete|Start...")

	if snapshot.chunkId != 999 {
		t.Fail()
	}

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

	for j := 0; j < 1000; j++ {
		id := int64(j)
		var str string
		data, err := snapshot.Query(id)
		if nil != err {
			log.Printf("TestDeleteAndStart|Query|%s", err)
		}

		str = string(data)
		if str != fmt.Sprintln(j) {
			log.Printf("TestDeleteAndStart|Query|FAIL|%s", str)
			t.Fail()
			continue
		}

		c := NewCommand(id, "", nil, nil)
		snapshot.Delete(c)
		i++
		_, err = snapshot.Query(id)
		if nil == err {
			t.Fail()
			log.Printf("TestDeleteAndStart|DELETE-QUERY|FAIL|%s", str)
			continue
		}
	}
	run = false
	snapshot.Destory()

	log.Printf("TestDeleteAndStart|Start...")
	snapshot = NewMessageStore("./snapshot/", 1, 10, traverse)
	snapshot.Start()

	fcount := 0
	//fetch all Segment
	filepath.Walk("./snapshot/", func(path string, f os.FileInfo, err error) error {
		// log.Info("MessageStore|Walk|%s", path)
		if nil != f && !f.IsDir() &&
			strings.HasSuffix(f.Name(), ".data") && f.Size() == 0 {
			fmt.Println(f.Name())
			fcount++
		}
		return nil
	})

	log.Printf("TestDeleteAndStart|Start|Lstat|%d", fcount)
	if fcount != 1 {
		t.Fail()
	}
	log.Printf("TestDeleteAndStart|Start|ChunkId|%d", snapshot.chunkId)
	if snapshot.chunkId != -1 {
		t.Fail()
	}
	snapshot.Destory()
}

//test delete
func TestDelete(t *testing.T) {
	cleanSnapshot("./snapshot/")
	snapshot := NewMessageStore("./snapshot/", 1, 10, traverse)
	snapshot.Start()
	for j := 0; j < 1000; j++ {
		d := []byte(fmt.Sprintln(j))
		cmd := NewCommand(-1, fmt.Sprintln(j), d, nil)
		<-snapshot.Append(cmd)
		// log.Printf("TestDelete|Append|%d|...", j)
	}
	snapshot.Destory()

	// time.Sleep(5 * time.Second)

	log.Printf("TestDelete|Append|Complete...")
	// //reload
	nsnapshot := NewMessageStore("./snapshot/", 1, 10, traverse)
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

	// for _, s := range nsnapshot.segments {
	// 	for _, c := range s.chunks {
	// 		log.Printf("nsnapshot|------%d", c.id)
	// 	}
	// }
	for j := 0; j < 1000; j++ {
		id := int64(j)
		var str string
		data, err := nsnapshot.Query(id)
		if nil != err {
			log.Printf("TestDelete|Query|%s", err)
		}

		str = string(data)
		if str != fmt.Sprintln(j) {
			log.Printf("TestDelete|Query|FAIL|%s", str)
			t.Fail()
			continue
		}

		c := NewCommand(id, "", nil, nil)
		nsnapshot.Delete(c)
		i++
		_, err = nsnapshot.Query(id)
		if nil == err {
			t.Fail()
			log.Printf("TestDelete|DELETE-QUERY|FAIL|%s", str)
			continue
		}
	}
	run = false
	nsnapshot.Destory()
	cleanSnapshot("./snapshot/")
}

func TestQuery(t *testing.T) {

	cleanSnapshot("./snapshot/")
	snapshot := NewMessageStore("./snapshot/", 1, 10, traverse)
	snapshot.Start()
	var data [512]byte
	for j := 0; j < 20; j++ {
		d := append(data[:512], []byte{
			byte((j >> 24) & 0xFF),
			byte((j >> 16) & 0xFF),
			byte((j >> 8) & 0xFF),
			byte(j & 0xFF)}...)

		cmd := NewCommand(-1, fmt.Sprint(j), d, nil)
		<-snapshot.Append(cmd)
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

	for ; i < 20; i++ {
		id := int64(rand.Intn(20))
		_, err := snapshot.Query(id)
		if nil != err {
			log.Printf("Query|%s|%d", err, id)
			t.Fail()
			break

		} else {
			// log.Printf("Query|SUCC|%d", id)
			j++
		}
	}

	_, err := snapshot.Query(19)
	if nil != err {
		log.Printf("Query|%s|%d", err, 19)
		t.Fail()

	}

	_, err = snapshot.Query(0)
	if nil != err {
		log.Printf("Query|%s|%d", err, 0)
		t.Fail()
	}

	run = false

	log.Printf("snapshot|%s|%d", snapshot, j)

	snapshot.Destory()
	cleanSnapshot("./snapshot/")
}

func BenchmarkDelete(t *testing.B) {
	t.Logf("BenchmarkDelete|Delete|Start...")
	t.StopTimer()
	cleanSnapshot("./snapshot/")
	snapshot := NewMessageStore("./snapshot/", 1, 10, traverse)
	snapshot.Start()

	for j := 0; j < 20; j++ {
		d := []byte(fmt.Sprintf("%d|hello snapshot", j))
		cmd := NewCommand(-1, fmt.Sprint(j), d, nil)
		<-snapshot.Append(cmd)
	}

	time.Sleep(2 * time.Second)
	t.StartTimer()

	i := 0
	for ; i < t.N; i++ {
		id := int64(rand.Intn(20))
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
	snapshot := NewMessageStore("./snapshot/", 1, 10, traverse)
	snapshot.Start()
	for j := 0; j < 20; j++ {
		d := []byte(fmt.Sprintf("%d|hello snapshot", j))
		cmd := NewCommand(-1, fmt.Sprint(j), d, nil)
		snapshot.Append(cmd)
	}

	time.Sleep(2 * time.Second)
	t.StartTimer()

	i := 0
	for ; i < t.N; i++ {
		id := int64(rand.Intn(20))
		_, err := snapshot.Query(id)
		if nil != err {
			log.Printf("Query|%s|%d", err, id)
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
	snapshot := NewMessageStore("./snapshot/", 1, 10, traverse)
	snapshot.Start()
	t.StartTimer()

	for i := 0; i < t.N; i++ {
		d := []byte(fmt.Sprintf("hello snapshot-%d", i))
		cmd := NewCommand(-1, fmt.Sprint(i), d, nil)
		<-snapshot.Append(cmd)
	}

	t.StopTimer()
	time.Sleep(5 * time.Second)
	snapshot.Destory()
	cleanSnapshot("./snapshot/")
	t.StartTimer()

}
