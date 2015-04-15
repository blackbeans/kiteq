package memory

import (
	"bufio"
	"encoding/binary"
	"fmt"
	log "github.com/blackbeans/log4go"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

//内存的快照
type MemorySnapshot struct {
	filePath     string
	baseDir      *os.File
	basename     string
	segments     Segments
	offset       int64 //global  segment offset
	chunkId      int64
	writeChannel chan *Chunk
	running      bool
	waitSync     *sync.WaitGroup
}

func NewMemorySnapshot(filePath string, basename string) *MemorySnapshot {
	ms := &MemorySnapshot{
		filePath:     filePath,
		basename:     basename,
		segments:     make(Segments, 0, 50),
		writeChannel: make(chan *Chunk, 1000),
		running:      true,
		waitSync:     &sync.WaitGroup{}}
	ms.load()
	go ms.sync()
	return ms
}

func (self *MemorySnapshot) load() {
	log.Info("MemorySnapshot|Load Segments ...")

	if !dirExist(self.filePath) {
		err := os.MkdirAll(self.filePath, os.ModePerm)
		if nil != err {
			log.Error("MemorySnapshot|Load Segments|MKDIR|FAIL|%s|%s\n", err, self.filePath)
			panic(err)
		}
	}

	bashDir, err := os.Open(self.filePath)
	if nil != err {
		log.Error("MemorySnapshot|Load Segments|FAIL|%s|%s\n", err, self.filePath)
		panic(err)
	}

	self.baseDir = bashDir

	//fetch all Segement
	filepath.Walk(self.filePath, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			df, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
			if nil != err {
				log.Error("MemorySnapshot|Load Segments|Open|FAIL|%s|%s\n", err, path)
				return err
			}

			name := strings.TrimSuffix(f.Name(), SEGMENT_DATA_SUFFIX)
			split := strings.SplitN(name, "-", 2)
			sid := int64(0)
			if len(split) >= 2 {
				id, err := strconv.ParseInt(split[1], 10, 64)
				if nil != err {
					log.Error("MemorySnapshot|Load Segments|Parse SegmentId|FAIL|%s|%s\n", err, name)
					return err
				}
				sid = id
			}

			// create segement
			seg := &Segement{
				path: path,
				name: f.Name(),
				file: df,
				sid:  sid}

			self.segments = append(self.segments, seg)
		}

		return nil
	})

	//sort segments
	sort.Sort(self.segments)
	//recover snapshost
	self.recoverSnapshot()

	if len(self.segments) <= 0 {
		self.createSegment(0)
	}

	log.Info("MemorySnapshot|Load|SUCC|%s\n", self)
}

func (self MemorySnapshot) String() string {
	return fmt.Sprintf("\nfilePath:%s\noffset:%d\nchunkid:%d\nsegments:%d",
		self.filePath, self.offset, self.chunkId, len(self.segments))
}

func (self *MemorySnapshot) recoverSnapshot() {
	//current segmentid
	if len(self.segments) > 0 {
		s := self.segments[len(self.segments)-1]
		fi, _ := s.file.Stat()

		filesize := fi.Size()
		soffset := int64(0)
		//scan offset
		br := bufio.NewReader(s.file)
		header := make([]byte, CHUNK_HEADER)
		chunkId := int64(0)
		byteSize := int32(0)
		for {

			l, err := br.Read(header)
			if nil != err {
				if io.EOF != err {
					log.Error("MemorySnapshot|Load Segement|Read Header|FAIL|%s|%s\n", err, s.name)
				}
				break
			}

			if l <= 0 {
				break
			} else if l < CHUNK_HEADER {
				log.Error("MemorySnapshot|Load Segement|Read Header|FAIL|%s|%s|%d\n", err, s.name, l)
			}

			//length
			length := binary.BigEndian.Uint32(header[0:4])

			al := soffset + int64(length)
			//checklength
			if al > filesize {
				log.Error("MemorySnapshot|Load Segement|FILE SIZE|ERROR|%s|%s|%d/%d\n",
					err, s.name, al, filesize)
				break
			}

			//checksum
			checksum := binary.BigEndian.Uint32(header[4:8])

			//read data
			dl := length - CHUNK_HEADER
			data := make([]byte, dl)
			bl, err := br.Read(data)
			if nil != err || bl < int(dl) {
				log.Error("MemorySnapshot|Load Segement|Read Data|FAIL|%s|%s|%d/%d\n", err, s.name, bl, dl)
				break
			}

			// log.Debug("MemorySnapshot|Chunk:%s\n", string(data))
			csum := crc32.ChecksumIEEE(data)
			//checkdata
			if csum != checksum {
				log.Error("MemorySnapshot|Load Segement|Data Checksum|FAIL|%s|%s|%d/%d\n",
					err, s.name, csum, checksum)
				break
			}

			soffset += int64(length)

			//read chunkid
			chunkId = int64(binary.BigEndian.Uint64(header[8:16]))
			//add byteSize
			byteSize += int32(length)
		}

		s.offset = soffset
		s.byteSize = byteSize
		s.Open()

		//set snapshost status
		self.chunkId = chunkId
		self.offset = s.sid + s.offset

	}
}

//write
func (self *MemorySnapshot) Append(msg []byte) int64 {

	if self.running {
		//create chunk
		chunk := &Chunk{
			length:   int32(CHUNK_HEADER + len(msg)),
			id:       self.cid(),
			checksum: crc32.ChecksumIEEE(msg),
			data:     msg,
			flag:     NORMAL}
		//write to channel for async flush
		self.writeChannel <- chunk
		return chunk.id
	} else {
		close(self.writeChannel)
		return -1
	}

}

func (self *MemorySnapshot) sync() {

	batch := make([]byte, 0, 50)
	var popChunk *Chunk
	var lastSeg *Segement
	for self.running {

		//check roll
		lastSeg = self.checkRoll()
		//no batch / wait for data
		select {
		case popChunk = <-self.writeChannel:
		default:
			//no write data flush
			popChunk = nil
		}

		if nil != popChunk {
			batch = append(batch, popChunk.marshal()...)
		}

		//force flush
		if nil == popChunk && len(batch) > 0 {
			err := lastSeg.Append(batch)
			if nil != err {
				log.Error("MemorySnapshot|Append|FAIL|%s\n", err)
			}
			batch = batch[:0]
		}

	}

	self.waitSync.Add(1)
	// need flush left data
	for {
		select {
		case chunk := <-self.writeChannel:
			if nil != chunk {
				batch = append(batch, chunk.marshal()...)
			}

		default:
			if len(batch) > 0 {
				// log.Debug("MemorySnapshot|CLOSE|SYNC|FAIL|%s|%s\n", lastSeg, batch)
				//complete
				lastSeg.Append(batch)

			}
			lastSeg.Close()
			break
		}
	}
	self.waitSync.Done()
}

//create segemnt
func (self *MemorySnapshot) createSegment(nextStart int64) (*Segement, error) {
	name := SEGMENT_PREFIX + fmt.Sprintf("%d", nextStart) + SEGMENT_DATA_SUFFIX

	df, err := os.OpenFile(self.filePath+string(filepath.Separator)+name,
		os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
	if nil != err {
		log.Error("MemorySnapshot|Load Segments|Open|FAIL|%s|%s\n", err, name)
		return nil, err
	}

	news := &Segement{
		path:     self.filePath,
		name:     name,
		file:     df,
		sid:      nextStart,
		offset:   0,
		byteSize: 0}

	err = news.Open()
	if nil != err {
		log.Error("MemorySnapshot|currentSegement|Open Segement|FAIL\n", news.path)
		return nil, err
	} else {
		//append new
		self.segments = append(self.segments, news)
		return news, nil
	}

}

//check if
func (self *MemorySnapshot) checkRoll() *Segement {
	//if current segment bytesize is larger than max segment size
	//create a new segement for storage

	s := self.segments[len(self.segments)-1]
	if s.byteSize > MAX_SEGEMENT_SIZE {
		nextStart := s.sid + s.offset
		news, err := self.createSegment(nextStart)
		if nil == err {
			s.Close()
			s = news
		}
	}
	return s
}

//chunk id
func (self *MemorySnapshot) cid() int64 {
	return atomic.AddInt64(&self.chunkId, 1)
}

func (self *MemorySnapshot) Destory() {
	self.running = false
	self.waitSync.Wait()
	log.Info("MemorySnapshot|Destory...")
}

// 检查目录是否存在
func dirExist(dir string) bool {
	info, err := os.Stat(dir)
	if err == nil {
		return info.IsDir()
	} else {
		return !os.IsNotExist(err) && info.IsDir()
	}
}
