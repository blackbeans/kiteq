package file

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	log "github.com/blackbeans/log4go"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type ChunkFlag uint8

func (self ChunkFlag) String() string {

	switch self {
	case NORMAL:
		return "NORMAL"
	case DELETE:
		return "DELETE"
	case EXPIRED:
		return "EXPIRED"
	}
	return ""
}

var SEGMENT_LOG_SPLIT = []byte{'\r', '\n'}

const (
	MAX_SEGMENT_SIZE    = 128 * 1024 * 1024 //最大的分段大仙
	SEGMENT_PREFIX      = "segment"
	SEGMENT_LOG_SUFFIX  = ".log"
	SEGMENT_DATA_SUFFIX = ".data"

	CHUNK_HEADER = 4 + 4 + 8 //|length 4byte|checksum 4byte|id 8byte|data variant|

	//chunk flag
	NORMAL  ChunkFlag = 'n'
	DELETE  ChunkFlag = 'd'
	EXPIRED ChunkFlag = 'e'
)

//消息文件
type Segment struct {
	path          string
	name          string //basename_0000000000
	rf            *os.File
	wf            *os.File
	bw            *bufio.Writer //
	br            *bufio.Reader // data buffer
	sid           int64         //segment id
	offset        int64         //segment current offset
	byteSize      int32         //segment size
	chunks        Chunks
	isOpen        int32
	slog          *SegmentLog   //segment op log
	appendChannel chan [][]byte //append channel
	sync.RWMutex
	writeWg sync.WaitGroup
}

func newSegment(path, name string, sid int64, slog *SegmentLog) *Segment {
	return &Segment{
		path:          path,
		name:          name,
		sid:           sid,
		slog:          slog,
		chunks:        make(Chunks, 0, 5000),
		appendChannel: make(chan [][]byte, 500),
		writeWg:       sync.WaitGroup{}}
}

func (self *Segment) String() string {
	return fmt.Sprintf("Segment[%s,%d]", self.name, self.sid)
}

func (self *Segment) Open(do func(ol *oplog)) error {

	self.Lock()
	defer self.Unlock()
	if atomic.CompareAndSwapInt32(&self.isOpen, 0, 1) {

		// log.Info("Segment|Open|BEGIN|%s|%s", self.path, self.name)
		var rf *os.File
		var wf *os.File
		_, err := os.Stat(self.path)
		//file exist
		if os.IsNotExist(err) {
			_, err := os.Create(self.path)
			if nil != err {
				log.ErrorLog("kite_store", "Segment|Create|FAIL|%s|%s", err, self.path)
				return err
			}
		}

		//file not exist create file
		wf, err = os.OpenFile(self.path, os.O_RDWR|os.O_APPEND, os.ModePerm)
		if nil != err {
			log.ErrorLog("kite_store", "Segment|Open|FAIL|%s|%s", err, self.name)
			return err
		}

		rf, err = os.OpenFile(self.path, os.O_RDWR, os.ModePerm)
		if nil != err {
			log.ErrorLog("kite_store", "Segment|Open|FAIL|%s|%s", err, self.name)
			return err
		}

		self.rf = rf
		self.wf = wf
		//buffer
		self.br = bufio.NewReader(rf)
		//load
		self.loadCheck()
		// //seek
		// self.wf.Seek(self.offset, 0)
		self.bw = bufio.NewWriter(wf)
		//op segment log
		err = self.slog.Open()
		if nil != err {
			return err
		}
		//recover segment
		self.recover(do)

		self.writeWg.Add(1)
		total, n, d, e := self.stat()

		//start flush
		go self.flush()
		go self.compact()
		log.InfoLog("kite_store", "Segment|Open|SUCC|%s|total:%d,n:%d,d:%d,e:%d", self.name, total, n, d, e)

		return nil
	}
	return nil
}

//compact
func (self *Segment) compact() {
	//TODO
}

func (self *Segment) flush() {
	var chunks [][]byte
	ticker := time.NewTicker(1 * time.Second)
	for atomic.LoadInt32(&self.isOpen) == 1 {
		select {
		case chunks = <-self.appendChannel:
		case <-ticker.C:
			//timeout
		}
		if len(chunks) > 0 {
			for _, tmp := range chunks {
				for {
					l, err := self.bw.Write(tmp)
					if nil != err && err != io.ErrShortWrite {
						log.ErrorLog("kite_store", "Segment|flush|FAIL|%s|%d/%d", err, l, len(tmp))
						break
					} else if nil == err {
						break
					} else {
						self.bw.Reset(self.wf)
						log.ErrorLog("kite_store", "Segment|flush|FAIL|%s", err)
					}
					tmp = tmp[l:]
				}
			}
			//flush
			self.bw.Flush()
			chunks = chunks[:0]
		}
	}

	//flush left
	finished := false
	for !finished {
		select {
		case chunks = <-self.appendChannel:
		default:
			finished = true
		}

		if len(chunks) > 0 {
			for _, tmp := range chunks {
				for {
					l, err := self.bw.Write(tmp)
					if nil != err && err != io.ErrShortWrite {
						log.ErrorLog("kite_store", "Segment|flush|FAIL|%s|%d/%d", err, l, len(tmp))
						break
					} else if nil == err {
						break
					} else {
						self.bw.Reset(self.wf)
						log.ErrorLog("kite_store", "Segment|flush|FAIL|%s", err)
					}
					tmp = tmp[l:]
				}
			}
			//flush
			self.bw.Flush()
			chunks = chunks[:0]
		}
	}
	self.writeWg.Done()
}

// chunks stat
func (self *Segment) stat() (total, normal, del, expired int32) {
	if len(self.chunks) > 0 {
		for _, c := range self.chunks {
			total++
			switch c.flag {
			case NORMAL:
				normal++
			//create
			case DELETE:
				del++
			case EXPIRED:
				expired++
				//ignore
			}
		}
	}
	return
}

//load check
func (self *Segment) loadCheck() {

	chunkId := int64(0)
	fi, _ := self.rf.Stat()
	offset := int64(0)
	byteSize := int32(0)
	buff := make([]byte, 16*1024)

	//reset to header
	self.rf.Seek(0, 0)
	self.br.Reset(self.rf)
	for {

		hl, err := io.ReadFull(self.br, buff[:CHUNK_HEADER])
		if nil != err {
			if io.EOF != err {
				log.ErrorLog("kite_store", "Segment|Load Segment|Read Header|FAIL|%s|%s", err, self.name)
				continue
			}
			break
		}

		if hl != CHUNK_HEADER {
			log.ErrorLog("kite_store", "Segment|Load Segment|Read Header|FAIL|%s|%d", self.name, hl)
			break
		}

		//length
		length := binary.BigEndian.Uint32(buff[0:4])

		al := offset + int64(length)
		//checklength
		if al > fi.Size() {
			log.ErrorLog("kite_store", "Segment|Load Segment|FILE SIZE|%s|%d/%d|offset:%d|length:%d", self.name, al, fi.Size(), offset, length)
			break
		}

		//checksum
		checksum := binary.BigEndian.Uint32(buff[4:8])

		//read chunkid
		chunkId = int64(binary.BigEndian.Uint64(buff[8:16]))

		//read data
		l := length - CHUNK_HEADER

		if int(l) > cap(buff[16:]) {
			gl := int(l) - cap(buff[16:])
			grow := make([]byte, gl)
			buff = append(buff[0:cap(buff)], grow...)
		}
		dl, err := io.ReadFull(self.br, buff[16:length])
		if nil != err || dl < int(l) {
			log.ErrorLog("kite_store", "Segment|Load Segment|Read Data|FAIL|%s|%s|%d/%d", err, self.name, l, dl)
			break
		}

		csum := crc32.ChecksumIEEE(buff[16:length])
		//checkdata
		if csum != checksum {
			log.ErrorLog("kite_store", "Segment|Load Segment|Data Checksum|FAIL|%s|%d|%d/%d", self.name, chunkId, csum, checksum)
			break
		}

		//add byteSize
		byteSize += int32(length)

		//create chunk
		chunk := &Chunk{
			offset:   offset,
			length:   int32(length),
			checksum: checksum,
			id:       chunkId,
			sid:      self.sid}

		self.chunks = append(self.chunks, chunk)
		offset += int64(length)
		// log.DebugLog("Segment|Load Chunk|%s|%d|%s", self.name, chunkId, ChunkFlag(flag))
	}

	//sort chunks
	sort.Sort(self.chunks)
	self.byteSize = byteSize
	self.offset = offset
}

//recover message status
func (self *Segment) recover(do func(ol *oplog)) {

	//replay
	self.slog.Replay(func(ol *oplog) {
		switch ol.Op {
		//create
		case OP_C, OP_U:
			c := self.Get(ol.ChunkId)
			if nil != c &&
				c.flag != EXPIRED && c.flag != DELETE {
				c.flag = NORMAL
			}
		case OP_E:
			//expired
			self.Expired(ol.ChunkId)
		case OP_D:
			self.Delete(ol.ChunkId)
		}
		//do callback
		do(ol)

	})
}

//delete chunk
func (self *Segment) Delete(cid int64) bool {
	// idx := sort.Search(len(self.chunks), func(i int) bool {
	// 	return self.chunks[i].id >= cid
	// })

	idx := int(cid - self.sid)
	if idx < 0 {
		return false
	}

	if idx < len(self.chunks) {
		//mark delete
		s := self.chunks[idx]
		if s.flag != DELETE && s.flag != EXPIRED {
			s.flag = DELETE
			return true
		}
	} else {
		log.DebugLog("kite_store", "Segment|Delete|NO Chunk|chunkid:%d|%d", cid, idx, len(self.chunks))
	}

	return true
}

//expired data
func (self *Segment) Expired(cid int64) bool {

	// idx := sort.Search(len(self.chunks), func(i int) bool {
	// 	return self.chunks[i].id >= cid
	// })

	idx := int(cid - self.sid)
	if idx < 0 {
		return false
	}

	// log.Debug("Segment|Expired|chunkid:%d|%s\n", cid, idx)
	if idx < len(self.chunks) {
		//mark delete
		s := self.chunks[idx]
		if s.flag != EXPIRED && s.flag != DELETE {
			s.flag = EXPIRED
			return true
		}
	}
	return true
}

// Truncate: release chunk
func (self *Segment) Truncate() {
	self.Lock()
	defer self.Unlock()
	for _, c := range self.chunks {
		//release
		c.data = nil
	}
}

//get chunk by chunkid
func (self *Segment) Get(cid int64) *Chunk {

	// idx := sort.Search(len(self.chunks), func(i int) bool {
	// 	return self.chunks[i].id >= cid
	// })
	idx := int(cid - self.sid)
	if idx < 0 {
		return nil
	}

	//not exsit
	if idx >= len(self.chunks) {
		return nil
	} else if self.chunks[idx].id == cid {
		//delete data return nil
		if self.chunks[idx].flag == DELETE ||
			self.chunks[idx].flag == EXPIRED {
			// log.DebugLog("Segment|Get|Result|%d|%d|%s\n", idx, cid, self.chunks[idx].flag)
			return nil
		}
		return self.chunks[idx]

	} else {
		// log.DebugLog("Segment|Get|Result|%d|%d|%d|%d\n", idx, cid, self.chunks[idx].id, len(self.chunks))
		return nil
	}
}

func (self *Segment) loadChunk(c *Chunk) {
	if c.length-CHUNK_HEADER <= 0 {
		log.ErrorLog("kite_store", "Segment|LoadChunk|INVALID HEADER|%s|%d|%d", self.name, c.id, c.length)
		return
	}

	data := make([]byte, c.length-CHUNK_HEADER)
	//seek chunk
	self.rf.Seek(c.offset+CHUNK_HEADER, 0)
	self.br.Reset(self.rf)
	dl, err := io.ReadFull(self.br, data)
	if nil != err || dl != cap(data) {
		log.ErrorLog("kite_store", "Segment|LoadChunk|Read Data|FAIL|%s|%s|%d|%d/%d", err, self.name, c.id, c.length, dl)
		return
	}
	c.data = data
}

// Append: apend data
func (self *Segment) Append(chunks []*Chunk) error {

	//if closed
	if atomic.LoadInt32(&self.isOpen) == 0 {
		return errors.New(fmt.Sprintf("Segment Is Closed!|%s", self.name))
	}
	chunkBytes := make([][]byte, 0, len(chunks))
	length := int64(0)
	for _, c := range chunks {
		c.sid = self.sid
		c.offset = self.offset + length
		tmp := c.marshal()
		chunkBytes = append(chunkBytes, tmp)
		length += int64(len(tmp))
	}
	//async write
	self.appendChannel <- chunkBytes

	// log.DebugLog("Segment|Append|SUCC|%d/%d", l, len(buff))
	//tmp cache chunk
	if nil == self.chunks {
		self.chunks = make([]*Chunk, 0, 1000)
	}
	self.chunks = append(self.chunks, chunks...)

	//move offset
	self.offset += int64(length)
	self.byteSize += int32(length)
	return nil
}

func (self *Segment) Close() error {
	self.Lock()
	defer self.Unlock()
	if atomic.CompareAndSwapInt32(&self.isOpen, 1, 0) {

		//wait wirte finished
		self.writeWg.Wait()

		//close segment log
		self.slog.Close()

		//close segment
		err := self.bw.Flush()
		if nil != err {
			log.ErrorLog("kite_store", "Segment|Close|Writer|FLUSH|FAIL|%s|%s|%s\n", err, self.path, self.name)
		}
		//free chunk memory
		self.chunks = nil

		err = self.wf.Close()

		if nil != err {
			log.ErrorLog("kite_store", "Segment|Close|Write FD|FAIL|%s|%s|%s\n", err, self.path, self.name)
			return err
		} else {
			err = self.rf.Close()
			if nil != err {
				log.ErrorLog("kite_store", "Segment|Close|Read FD|FAIL|%s|%s|%s\n", err, self.path, self.name)
			}
			return err
		}

	}

	return nil

}

//
//-------------------------------------------------------------------------
//|length 4byte |checksum 4byte|id 8byte| data variant|
//-------------------------------------------------------------------------
//其中length的4个字节包括:
// length(4B)+checksum(4B)+id(8B)+len(data)
//总长度
//存储块
type Chunk struct {
	offset   int64
	length   int32
	checksum uint32
	id       int64
	sid      int64
	data     []byte // data
	flag     ChunkFlag
}

func (self *Chunk) marshal() []byte {

	buff := make([]byte, self.length)
	//encode
	binary.BigEndian.PutUint32(buff[0:4], uint32(self.length))
	binary.BigEndian.PutUint32(buff[4:8], self.checksum)
	binary.BigEndian.PutUint64(buff[8:16], uint64(self.id))
	copy(buff[16:], self.data)

	return buff
}

type Chunks []*Chunk

func (self Chunks) Len() int { return len(self) }
func (self Chunks) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}
func (self Chunks) Less(i, j int) bool {
	return self[i].id < self[j].id
}

type Segments []*Segment

func (self Segments) Len() int { return len(self) }
func (self Segments) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}
func (self Segments) Less(i, j int) bool {
	return self[i].sid < self[j].sid
}
