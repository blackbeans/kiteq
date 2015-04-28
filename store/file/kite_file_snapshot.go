package file

import (
	"container/list"
	"fmt"
	log "github.com/blackbeans/log4go"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

//的快照
type MessageStore struct {
	filePath     string
	baseDir      *os.File
	segments     Segments
	chunkId      int64
	writeChannel chan *command
	running      bool
	waitSync     *sync.WaitGroup
	batchSize    int
	segcacheSize int                //segment cache size
	segmentCache *list.List         //segment cached
	replay       func(oplog *oplog) //oplog replay

	sync.RWMutex
}

func NewMessageStore(filePath string, batchSize int, segcacheSize int, replay func(oplog *oplog)) *MessageStore {

	ms := &MessageStore{
		chunkId:      -1,
		filePath:     filePath,
		segments:     make(Segments, 0, 50),
		writeChannel: make(chan *command, 10000),
		running:      true,
		batchSize:    batchSize,
		segcacheSize: segcacheSize,
		segmentCache: list.New(),
		waitSync:     &sync.WaitGroup{},
		replay:       replay}

	return ms
}

func (self *MessageStore) Start() {
	self.load()
	go self.sync()
	self.waitSync.Add(1)
}

func (self *MessageStore) load() {
	log.Info("MessageStore|Load Segments ...")

	if !dirExist(self.filePath) {
		err := os.MkdirAll(self.filePath, os.ModePerm)
		if nil != err {
			log.Error("MessageStore|Load Segments|MKDIR|FAIL|%s|%s", err, self.filePath)
			panic(err)
		}
	}

	bashDir, err := os.Open(self.filePath)
	if nil != err {
		log.Error("MessageStore|Load Segments|FAIL|%s|%s", err, self.filePath)
		panic(err)
	}

	self.baseDir = bashDir

	//segment builder
	segmentBuilder := func(path string, f os.FileInfo) *Segment {

		logpath := strings.SplitN(path, ".", 2)[0] + "." + SEGMENT_LOG_SUFFIX

		name := strings.TrimSuffix(f.Name(), SEGMENT_DATA_SUFFIX)
		split := strings.SplitN(name, "-", 2)
		sid := int64(0)
		if len(split) >= 2 {
			id, err := strconv.ParseInt(split[1], 10, 64)
			if nil != err {
				log.Error("MessageStore|Load Segments|Parse SegmentId|FAIL|%s|%s", err, name)
				return nil
			}
			sid = id
		}

		//segment log
		sl := &SegmentLog{
			offset: 0,
			path:   logpath,
		}

		// create segment
		seg := &Segment{
			path: path,
			name: f.Name(),
			sid:  sid,
			slog: sl}
		return seg
	}

	//fetch all Segment
	filepath.Walk(self.filePath, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {

			split := strings.SplitN(f.Name(), ".", 2)
			suffix := split[1]
			//is data file or logfile
			if suffix == SEGMENT_DATA_SUFFIX {
				seg := segmentBuilder(path, f)
				self.segments = append(self.segments, seg)
				log.Info("MessageStore|load|segmentBuilder|%s", path)
			} else if suffix == SEGMENT_LOG_SUFFIX {
				//log suffix
				return nil
			}
		}

		return nil
	})

	//sort segments
	sort.Sort(self.segments)

	//check roll
	self.checkRoll()

	//recover snapshost
	self.recoverSnapshot()

	//load fixed num  segments into memory

	log.Info("MessageStore|Load|SUCC|%s", self)
}

func (self *MessageStore) recoverSnapshot() {
	//current segmentid
	if len(self.segments) > 0 {

		//replay log
		for i, s := range self.segments {
			err := s.Open()
			if nil != err {
				log.Error("MessageStore|recoverSnapshot|Fail|%s", err, s.slog.path)
				panic(err)
			}
			//replay segment log
			s.slog.Replay(self.replay)

			//last segments
			if i == len(self.segments)-1 {
				err := s.Open()
				if nil != err {
					panic("MessageStore|Load Last Segment|FAIL|" + err.Error())
				}

				//set snapshost status
				if len(s.chunks) > 0 {
					self.chunkId = s.chunks[len(s.chunks)-1].id
				}
			}

		}

	}
}

// query head data
func (self *MessageStore) Head() (int64, []*Chunk) {
	self.RLock()
	defer self.RUnlock()
	var first *Segment
	if len(self.segments) > 0 {
		first = self.segments[0]
		//check cid in cache
		for e := self.segmentCache.Front(); nil != e; e = e.Next() {
			s := e.Value.(*Segment)
			if s.sid == first.sid {
				return first.sid, s.LoadChunks()
			}
		}

		//not in cache load into cache
		self.loadSegment(0)
		return first.sid, first.LoadChunks()

	}

	return -1, nil

}

//query one chunk by  chunkid
func (self *MessageStore) Query(cid int64) *Chunk {

	curr := self.indexSegment(cid)
	if nil == curr {
		return nil
	}
	//find chunk
	return curr.Get(cid)
}

//index segment
func (self *MessageStore) indexSegment(cid int64) *Segment {
	var curr *Segment
	self.RLock()
	//check cid in cache
	for e := self.segmentCache.Front(); nil != e; e = e.Next() {
		s := e.Value.(*Segment)
		if s.sid <= cid && cid <= (s.sid+int64(len(s.chunks))) {
			curr = s
		}
	}
	self.RUnlock()

	// not exist In cache
	if nil == curr {
		self.Lock()
		idx := sort.Search(len(self.segments), func(i int) bool {
			s := self.segments[i]
			return s.sid >= cid
		})

		if idx >= len(self.segments) || self.segments[idx].sid != cid {
			idx = idx - 1
		}

		//load segment
		self.loadSegment(idx)
		curr = self.segments[idx]
		self.Unlock()

	}
	return curr
}

//return the front chunk
func (self *MessageStore) loadSegment(idx int) {

	// load n segments
	s := self.segments[idx]
	err := s.Open()
	if nil != err {
		log.Error("MessageStore|loadSegment|FAIL|%s|%s\n", err, s.name)
		return
	} else {
		//pop header
		for e := self.segmentCache.Front(); self.segmentCache.Len() > self.segcacheSize; {
			self.segmentCache.Remove(e)
		}
		//push to cache
		self.segmentCache.PushBack(s)
	}
	log.Info("MessageStore|loadSegment|SUCC|%s\n", s.name)
}

//append log
func (self *MessageStore) Update(c *command) {
	s := self.indexSegment(c.id)
	if nil != s {
		//append oplog
		ol := newOplog(OP_U, c.logicId, c.id, c.opbody)
		s.slog.Append(ol)
	}
}

//mark delete
func (self *MessageStore) Delete(c *command) {
	s := self.indexSegment(c.id)
	if nil != s {

		//append oplog
		ol := newOplog(OP_D, c.logicId, c.id, c.opbody)
		s.slog.Append(ol)
		//mark data delete
		s.Delete(c.id)

	} else {
		// log.Debug("MessageStore|Delete|chunkid:%d|%s\n", cid, s)
	}
}

type command struct {
	id      int64
	logicId string
	msg     []byte
	opbody  string
}

func NewCommand(logicId string, msg []byte, body string) *command {
	return &command{
		logicId: logicId,
		msg:     msg,
		opbody:  body}
}

//write
func (self *MessageStore) Append(cmd *command) int64 {

	if self.running {
		cmd.id = self.cid()
		//write to channel for async flush
		self.writeChannel <- cmd
		return cmd.id
	} else {
		close(self.writeChannel)
		return -1
	}

}

func (self *MessageStore) sync() {

	batch := make([]*Chunk, 0, self.batchSize)
	batchOPLogs := make([]*oplog, 0, self.batchSize)

	var cmd *command

	lastSeg := self.checkRoll()
	for self.running {

		//no batch / wait for data
		select {
		case cmd = <-self.writeChannel:
		default:
			//no write data flush

		}

		if nil != cmd {
			c := cmd
			//create chunk
			chunk := &Chunk{
				length:   int32(CHUNK_HEADER + len(c.msg)),
				id:       c.id,
				checksum: crc32.ChecksumIEEE(c.msg),
				data:     c.msg,
				flag:     NORMAL}
			batch = append(batch, chunk)

			ol := newOplog(OP_C, c.logicId, c.id, c.opbody)
			batchOPLogs = append(batchOPLogs, ol)
		}

		//force flush
		if nil == cmd && len(batch) > 0 || len(batch) >= cap(batch) {

			//write append log
			err := lastSeg.slog.BatchAppend(batchOPLogs)
			if nil != err {
				log.Error("MessageStore|AppendLog|FAIL|%s\n", err)
			} else {

				err := lastSeg.Append(batch)
				if nil != err {
					log.Error("MessageStore|Append|FAIL|%s\n", err)
				}
			}
			batch = batch[:0]
			batchOPLogs = batchOPLogs[:0]
		}

		cmd = nil
		//check roll
		lastSeg = self.checkRoll()
	}

	// need flush left data
outter:
	for {
		select {
		case c := <-self.writeChannel:
			if nil != c {
				//create chunk
				chunk := &Chunk{
					length:   int32(CHUNK_HEADER + len(c.msg)),
					id:       c.id,
					checksum: crc32.ChecksumIEEE(c.msg),
					data:     c.msg,
					flag:     NORMAL}
				batch = append(batch, chunk)
				ol := newOplog(OP_C, c.logicId, c.id, c.opbody)
				batchOPLogs = append(batchOPLogs, ol)
			}

		default:

			if len(batch) > 0 {
				//complete
				err := lastSeg.slog.BatchAppend(batchOPLogs)
				if nil != err {
					log.Error("MessageStore|AppendLog|FAIL|%s\n", err)
				} else {

					err := lastSeg.Append(batch)
					if nil != err {
						log.Error("MessageStore|Append|FAIL|%s\n", err)
					}
				}
				batch = batch[:0]
				batchOPLogs = batchOPLogs[:0]

			}
			break outter
		}

	}

	self.waitSync.Done()
	log.Info("MessageStore|SYNC|CLOSE...")
}

//check if
func (self *MessageStore) checkRoll() *Segment {
	//if current segment bytesize is larger than max segment size
	//create a new segment for storage

	var s *Segment
	if len(self.segments) <= 0 {
		news, err := self.createSegment(self.chunkId + 1)
		if nil == err {
			self.Lock()
			//append new
			self.segments = append(self.segments, news)

			self.Unlock()
			s = news

		} else {
			//panic  first segment fail
			panic(err)
		}
	} else {
		self.RLock()
		s = self.segments[len(self.segments)-1]
		self.RUnlock()
		if s.byteSize > MAX_SEGMENT_SIZE {
			self.Lock()
			news, err := self.createSegment(self.chunkId + 1)
			if nil == err {
				//left segments are larger than cached ,close current
				if len(self.segments) >= self.segcacheSize {
					s.Close()
				}
				//append new
				self.segments = append(self.segments, news)
				s = news
			}
			self.Unlock()
		}
	}
	return s
}

//create segemnt
func (self *MessageStore) createSegment(nextStart int64) (*Segment, error) {
	name := fmt.Sprintf("%s-%d", SEGMENT_PREFIX, nextStart) + SEGMENT_DATA_SUFFIX
	logname := fmt.Sprintf("%s-%d", SEGMENT_PREFIX, nextStart) + SEGMENT_LOG_SUFFIX

	sl := &SegmentLog{
		offset: 0,
		path:   self.filePath + logname}

	news := &Segment{
		slog:     sl,
		path:     self.filePath + name,
		name:     name,
		sid:      nextStart,
		offset:   0,
		byteSize: 0}

	err := news.Open()
	if nil != err {
		log.Error("MessageStore|currentSegment|Open Segment|FAIL|%s", news.path)
		return nil, err
	}
	return news, nil

}

//remove sid
func (self *MessageStore) Remove(sid int64) {

	//check cid in cache
	for e := self.segmentCache.Front(); nil != e; e = e.Next() {
		s := e.Value.(*Segment)
		if s.sid == sid {
			s.Close()
			os.Remove(s.path)

			//remove from segments
			for i, s := range self.segments {
				if s.sid == s.sid {
					self.segments = append(self.segments[0:i], self.segments[i+1:]...)
					break
				}
			}

			log.Info("MessageStore|Remove|Segment|%s", s.path)
			break
		}
	}
}

func (self *MessageStore) Destory() {
	self.running = false
	self.waitSync.Wait()
	//close all segment
	for _, s := range self.segments {
		err := s.Close()
		if nil != err {
			log.Error("MessageStore|Destory|Close|FAIL|%s|sid:%d", err, s.sid)
		}
	}

	self.baseDir.Close()
	log.Info("MessageStore|Destory...")

}

//chunk id
func (self *MessageStore) cid() int64 {
	return atomic.AddInt64(&self.chunkId, 1)
}

func (self MessageStore) String() string {
	return fmt.Sprintf("filePath:%s\tchunkid:%d\tsegments:%d",
		self.filePath, self.chunkId, len(self.segments))
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
