package file

import (
	"bufio"
	"encoding/binary"
	"fmt"
	log "github.com/blackbeans/log4go"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
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
	MAX_SEGMENT_SIZE = 64 * 1024 * 1024 //最大的分段大仙
	// MAX_CHUNK_SIZE      = 64 * 1024        //最大的chunk
	SEGMENT_PREFIX      = "segment"
	SEGMENT_LOG_SUFFIX  = ".log"
	SEGMENT_DATA_SUFFIX = ".data"

	CHUNK_HEADER           = 4 + 4 + 8 + 1 //|length 4byte|checksum 4byte|id 8byte|flag 1byte| data variant|
	NORMAL       ChunkFlag = 'n'
	DELETE       ChunkFlag = 'd'
	EXPIRED      ChunkFlag = 'e'
)

//消息文件
type Segment struct {
	path     string
	name     string //basename_0000000000
	rf       *os.File
	wf       *os.File
	bw       *bufio.Writer //
	br       *bufio.Reader // data buffer
	sid      int64         //segment id
	offset   int64         //segment current offset
	byteSize int32         //segment size
	chunks   []*Chunk
	isOpen   int32
	slog     *SegmentLog //segment op log
	latch    chan bool
	sync.RWMutex
}

func newSegment(path, name string, sid int64, slog *SegmentLog) *Segment {
	return &Segment{
		path:  path,
		name:  name,
		sid:   sid,
		slog:  slog,
		latch: make(chan bool, 1)}
}

func (self *Segment) String() string {
	return fmt.Sprintf("Segment[%s,%d]", self.name, self.sid)
}

func (self *Segment) Open() error {

	defer func() {
		<-self.latch
	}()
	self.latch <- true
	if atomic.CompareAndSwapInt32(&self.isOpen, 0, 1) {

		// log.Info("Segment|Open|BEGIN|%s|%s", self.path, self.name)
		var rf *os.File
		var wf *os.File

		//file exist
		if _, err := os.Stat(self.path); err == nil {

			wf, err = os.OpenFile(self.path, os.O_RDWR|os.O_APPEND, os.ModePerm)
			if nil != err {
				log.Error("MemorySnapshot|Load Segments|Open|FAIL|%s|%s", err, self.path)
				return err
			}

			rf, err = os.OpenFile(self.path, os.O_RDWR, os.ModePerm)
			if nil != err {
				log.Error("MemorySnapshot|Load Segments|Open|FAIL|%s|%s", err, self.path)
				return err
			}

		} else {
			//file not exist create file
			wf, err = os.OpenFile(self.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
			if nil != err {
				log.Error("MemorySnapshot|Load Segments|Open|FAIL|%s|%s", err, self.name)
				return err
			}

			rf, err = os.OpenFile(self.path, os.O_CREATE|os.O_RDWR, os.ModePerm)
			if nil != err {
				log.Error("MemorySnapshot|Load Segments|Open|FAIL|%s|%s", err, self.name)
				return err
			}
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
		log.Info("Segment|Open|SUCC|%s", self.name)
		//op segment log
		self.slog.Open()
		return nil
	}
	return nil
}

// chunks stat
func (self *Segment) stat() (total, normal, del, expired int32) {
	for _, v := range self.chunks {
		total += 1
		switch v.flag {
		case NORMAL:
			normal++
		case DELETE:
			del++
		case EXPIRED:
			expired++
		}
	}
	return
}

//load check
func (self *Segment) loadCheck() {

	header := make([]byte, CHUNK_HEADER)
	chunkId := int64(0)
	fi, _ := self.rf.Stat()
	offset := int64(0)
	byteSize := int32(0)
	for {

		hl, err := io.ReadFull(self.br, header)
		if nil != err {
			if io.EOF != err {
				log.Error("Segment|Load Segment|Read Header|FAIL|%s|%s", err, self.name)
				continue
			}
			break
		}

		if hl <= 0 || hl < CHUNK_HEADER {
			log.Error("Segment|Load Segment|Read Header|FAIL|%s|%d", self.name, hl)
			break
		}

		//length
		length := binary.BigEndian.Uint32(header[0:4])

		al := offset + int64(length)
		//checklength
		if al > fi.Size() {
			log.Error("Segment|Load Segment|FILE SIZE|%s|%d/%d|offset:%d|length:%d", self.name, al, fi.Size(), self.offset, length)
			break
		}

		//checksum
		checksum := binary.BigEndian.Uint32(header[4:8])

		//read data
		l := length - CHUNK_HEADER
		data := make([]byte, l)
		dl, err := io.ReadFull(self.br, data)
		if nil != err || dl < int(l) {
			log.Error("Segment|Load Segment|Read Data|FAIL|%s|%s|%d/%d", err, self.name, l, dl)
			break
		}

		csum := crc32.ChecksumIEEE(data)
		//checkdata
		if csum != checksum {
			log.Error("Segment|Load Segment|Data Checksum|FAIL|%s|%d/%d", self.name, csum, checksum)
			break
		}

		offset += int64(length)

		//read chunkid
		chunkId = int64(binary.BigEndian.Uint64(header[8:16]))

		//flag
		flag := header[16]

		//add byteSize
		byteSize += int32(length)

		//create chunk
		chunk := &Chunk{
			offset:   offset,
			length:   int32(length),
			checksum: checksum,
			id:       chunkId,
			sid:      self.sid,
			flag:     ChunkFlag(flag),
			data:     data}

		self.chunks = append(self.chunks, chunk)
		log.Error("Segment|Load Chunk|%s|%d", self.name, chunkId)

	}

	self.offset = offset
	self.byteSize = byteSize

}

func (self *Segment) Delete(cid int64) {
	idx := int(cid - self.sid)
	// log.Debug("Segment|Delete|chunkid:%d|%s\n", cid, idx)
	if idx < len(self.chunks) {
		//mark delete
		s := self.chunks[idx]
		// log.Debug("Segment|Delete|%s", s)
		if s.flag != DELETE {
			s.flag = DELETE
			//flush to file
			self.wf.WriteAt([]byte{byte(DELETE)}, CHUNK_HEADER-1)
		}
	}
}

//expired data
func (self *Segment) Expired(cid int64) bool {
	idx := int(cid - self.sid)
	// log.Debug("Segment|Expired|chunkid:%d|%s\n", cid, idx)
	if idx < len(self.chunks) {
		//mark delete
		s := self.chunks[idx]
		// log.Debug("Segment|Delete|%s", s)
		if s.flag != EXPIRED && s.flag != DELETE {
			s.flag = EXPIRED
			//flush to file
			self.wf.WriteAt([]byte{byte(EXPIRED)}, CHUNK_HEADER-1)
		}
	}
	return true
}

//get chunk by chunkid
func (self *Segment) Get(cid int64) *Chunk {
	// log.Debug("Segment|Get|%d\n", len(self.chunks))
	idx := sort.Search(len(self.chunks), func(i int) bool {
		// log.Debug("Segment|Get|%d|%d\n", i, cid)
		return self.chunks[i].id >= cid
	})

	// log.Debug("Segment|Get|Result|%d|%d|%d\n", idx, cid, len(self.chunks))
	//not exsit
	if idx >= len(self.chunks) {
		return nil
	} else if self.chunks[idx].id == cid {
		//delete data return nil
		if self.chunks[idx].flag == DELETE ||
			self.chunks[idx].flag == EXPIRED {
			return nil
		}
		return self.chunks[idx]
	} else {
		log.Debug("Segment|Get|Result|%d|%d|%d|%d\n", idx, cid, self.chunks[idx].id, len(self.chunks))
		return nil
	}
}

//apend data
func (self *Segment) Append(chunks []*Chunk) error {

	buff := make([]byte, 0, 2*1024)
	for _, c := range chunks {
		c.sid = self.sid
		buff = append(buff, c.marshal()...)
	}

	tmp := buff
	for {
		l, err := self.bw.Write(tmp)
		if nil != err && err != io.ErrShortWrite {
			log.Error("Segment|Append|FAIL|%s|%d/%d", err, l, len(tmp))
			return err
		} else if nil == err {
			break
		} else {
			self.bw.Reset(self.wf)
		}
		tmp = tmp[l:]
	}

	// log.Debug("Segment|Append|SUCC|%d/%d", l, len(buff))
	//tmp cache chunk
	if nil == self.chunks {
		self.chunks = make([]*Chunk, 0, 1000)
	}
	self.chunks = append(self.chunks, chunks...)

	//move offset
	self.offset += int64(len(buff))
	self.byteSize += int32(len(buff))
	return nil
}

func (self *Segment) Close() error {
	if atomic.CompareAndSwapInt32(&self.isOpen, 1, 0) {

		//close segment log
		self.slog.Close()

		//close segment
		err := self.bw.Flush()
		if nil != err {
			log.Error("Segment|Close|Writer|FLUSH|FAIL|%s|%s|%s\n", err, self.path, self.name)
		}
		//free chunk memory
		self.chunks = nil

		err = self.wf.Close()

		if nil != err {
			log.Error("Segment|Close|Write FD|FAIL|%s|%s|%s\n", err, self.path, self.name)
			return err
		} else {
			err = self.rf.Close()
			if nil != err {
				log.Error("Segment|Close|Read FD|FAIL|%s|%s|%s\n", err, self.path, self.name)
			}
			return err
		}

	} else if self.isOpen == 1 {
		return self.Close()
	}

	return nil

}

//----------------------------------------------------
//|length 4byte|checksum 4byte|id 8byte|flag 1byte| data variant|
//----------------------------------------------------
//存储块
type Chunk struct {
	offset   int64
	length   int32
	checksum uint32
	flag     ChunkFlag //chunk状态
	id       int64
	sid      int64
	data     []byte // data
}

func (self *Chunk) marshal() []byte {

	buff := make([]byte, self.length)
	//encode
	binary.BigEndian.PutUint32(buff[0:4], uint32(self.length))
	binary.BigEndian.PutUint32(buff[4:8], self.checksum)
	binary.BigEndian.PutUint64(buff[8:16], uint64(self.id))
	buff[16] = uint8(self.flag)
	copy(buff[17:], self.data)

	return buff
}

type Segments []*Segment

func (self Segments) Len() int { return len(self) }
func (self Segments) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}
func (self Segments) Less(i, j int) bool {
	return self[i].sid < self[j].sid
}
