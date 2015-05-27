package file

import (
	"container/list"
	"errors"
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
	"time"
)

//opcommand
type command struct {
	id      int64
	logicId string
	msg     []byte
	opbody  []byte
	seg     *Segment
	idchan  chan int64
}

func NewCommand(id int64, logicId string, msg []byte, opbody []byte) *command {
	return &command{
		id:      id,
		logicId: logicId,
		msg:     msg,
		opbody:  opbody}
}

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
	checkPeriod  time.Duration
	sync.RWMutex
}

func NewMessageStore(filePath string, batchSize int, segcacheSize int,
	checkPeriod time.Duration, replay func(oplog *oplog)) *MessageStore {

	ms := &MessageStore{
		chunkId:      -1,
		filePath:     filePath,
		segments:     make(Segments, 0, 50),
		writeChannel: make(chan *command, 10000),
		batchSize:    batchSize,
		segcacheSize: segcacheSize,
		segmentCache: list.New(),
		waitSync:     &sync.WaitGroup{},
		replay:       replay,
		checkPeriod:  checkPeriod}

	return ms
}

func (self *MessageStore) Start() {
	self.running = true
	self.load()
	go self.sync()
	go self.evict()
	self.waitSync.Add(1)
}

//
func (self *MessageStore) evict() {
	//delete segment  if all chunks are deleted

	removeChan := make(chan *Segment, 100)
	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		defer wg.Done()
		for self.running {
			s := <-removeChan
			if nil != s {
				self.remove(s)
			}
		}
		//process left
		for {
			s := <-removeChan
			if nil != s {
				self.remove(s)
			} else {
				break
			}
		}
	}()

	// removeSegs := make([]*Segment, 0, 10)
	for self.running {
		time.Sleep(self.checkPeriod)
		stat := ""
		cache := ""
		self.RLock()
		//保证当前数据都是已经写入了至少一个分块
		if len(self.segments) < 2 {
			//do nothing
		} else {

			//check 0...n-1 segment stat
			for _, s := range self.segments[:len(self.segments)-1] {
				//try open
				s.RLock()
				total, normal, del, expired := s.stat()
				stat += fmt.Sprintf("|%s\t|%d\t|%d\t|%d\t|%d\t|\n", s.name, total, normal, del, expired)
				s.RUnlock()
				if normal <= 0 {
					removeChan <- s
				}
			}

			//cache
			for e := self.segmentCache.Front(); nil != e; e = e.Next() {
				cache += e.Value.(*Segment).name
				cache += ","
			}
		}
		self.RUnlock()

		if len(stat) > 0 {
			log.Info("---------------MessageStore-Stat--------------\n"+
				"cached-segments:[%s]\n"+
				"|segment\t\t|total\t|normal\t|delete\t|expired\t|\n%s", cache, stat)
		}
	}

	close(removeChan)
	wg.Wait()
}

//remove segment
func (self *MessageStore) remove(s *Segment) {

	self.Lock()
	//remove from segments
	for i, s := range self.segments {
		if s.sid == s.sid {
			self.segments = append(self.segments[0:i], self.segments[i+1:]...)
			break
		}
	}
	//remove from cache
	for e := self.segmentCache.Front(); nil != e; e = e.Next() {
		cs := e.Value.(*Segment)
		if cs.sid == s.sid {
			self.segmentCache.Remove(e)
			break
		}
	}

	//close segment
	s.Close()

	err := os.Remove(s.path)
	if nil != err {
		log.Warn("MessageStore|Remove|Segment|FAIL|%s|%s", err, s.path)
	}
	err = os.Remove(s.slog.path)
	if nil != err {
		log.Warn("MessageStore|Remove|SegmentLog|FAIL|%s|%s", err, s.slog.path)
	}

	self.Unlock()
	log.Info("MessageStore|Remove|Segment|%s", s.path)
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

		logpath := strings.SplitN(path, ".", 2)[0] + SEGMENT_LOG_SUFFIX

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
		sl := newSegmentLog(logpath)
		// create segment
		return newSegment(path, f.Name(), sid, sl)
	}

	//fetch all Segment
	filepath.Walk(self.filePath, func(path string, f os.FileInfo, err error) error {
		// log.Info("MessageStore|Walk|%s", path)
		if nil != f && !f.IsDir() {
			split := strings.SplitN(f.Name(), ".", 2)
			suffix := split[1]
			//is data file or logfile

			if strings.HasSuffix(SEGMENT_DATA_SUFFIX, suffix) {
				seg := segmentBuilder(path, f)
				self.segments = append(self.segments, seg)
				// log.Info("MessageStore|load|segmentBuilder|%s", path)
			} else if strings.HasSuffix(SEGMENT_LOG_SUFFIX, suffix) {
				//log suffix
				return nil
			}
		}

		return nil
	})

	//sort segments
	sort.Sort(self.segments)
	//recover snapshost
	self.recoverSnapshot()

	//check roll
	self.checkRoll()

	//load fixed num  segments into memory

	log.Info("MessageStore|Load|SUCC|%s", self)
}

func (self *MessageStore) recoverSnapshot() {
	//current segmentid
	if len(self.segments) > 0 {

		//replay log
		for i, s := range self.segments {
			err := s.Open(self.replay)
			if nil != err {
				log.Error("MessageStore|recoverSnapshot|Fail|%s", err, s.slog.path)
				panic(err)
			}

			//last segments
			if i == len(self.segments)-1 {
				if nil != err {
					panic("MessageStore|Load Last Segment|FAIL|" + err.Error())
				}

				//set snapshost status
				if len(s.chunks) > 0 {
					self.chunkId = s.chunks[len(s.chunks)-1].id
				}
			}
			log.Debug("MessageStore|recoverSnapshot|%s", s.name)
		}
	}
}

//query one chunk by  chunkid
func (self *MessageStore) Query(cid int64) ([]byte, error) {

	curr := self.indexSegment(cid)
	if nil == curr {
		return nil, errors.New(fmt.Sprintf("No Segement For %d", cid))
	}

	curr.Lock()
	defer curr.Unlock()
	//find chunk
	c := curr.Get(cid)

	if nil != c {
		if len(c.data) <= 0 {
			curr.loadChunk(c)
		}
		clone := make([]byte, len(c.data))
		copy(clone, c.data)
		return clone, nil

	}
	return nil, errors.New(fmt.Sprintf("No Chunk For [%s,%d]", curr.name, cid))
}

//index segment
func (self *MessageStore) indexSegment(cid int64) *Segment {

	var curr *Segment
	self.RLock()
	//check cid in cache
	for e := self.segmentCache.Front(); nil != e; e = e.Next() {
		s := e.Value.(*Segment)
		if s.sid <= cid && cid < s.sid+int64(len(s.chunks)) {
			curr = s
			break
		}
	}
	self.RUnlock()

	// not exist In cache
	if nil == curr {
		self.Lock()
		defer self.Unlock()
		//double check
		for e := self.segmentCache.Front(); nil != e; e = e.Next() {
			s := e.Value.(*Segment)
			if s.sid <= cid && cid < s.sid+int64(len(s.chunks)) {
				curr = s
				break
			}
		}

		idx := sort.Search(len(self.segments), func(i int) bool {
			s := self.segments[i]
			return s.sid >= cid
		})

		if idx >= len(self.segments) || self.segments[idx].sid != cid {
			idx = idx - 1
		}

		if idx <= 0 {
			idx = 0
		}

		if len(self.segments) > 0 {
			//load segment
			curr = self.loadSegment(idx)
		}
		// log.Debug("MessageStore|indexSegment|%d", curr.path)

	}
	return curr
}

//load segment
func (self *MessageStore) loadSegment(idx int) *Segment {

	// load n segments
	s := self.segments[idx]
	//remove
	if self.segmentCache.Len() >= self.segcacheSize {
		e := self.segmentCache.Back()
		v := self.segmentCache.Remove(e)
		//truncate data
		v.(*Segment).Truncate()
	}

	//push to cache
	self.segmentCache.PushFront(s)
	// log.Info("MessageStore|loadSegment|SUCC|%s|cache-Len:%d", s.name, self.segmentCache.Len())
	return s
}

//append log
func (self *MessageStore) Update(c *command) {
	s := self.indexSegment(c.id)
	if nil != s {
		s.Lock()
		defer s.Unlock()
		//append oplog
		ol := newOplog(OP_U, c.logicId, c.id, c.opbody)
		s.slog.Append(ol)
	}
}

//mark delete
func (self *MessageStore) Delete(c *command) bool {
	s := self.indexSegment(c.id)
	if nil != s {

		//append oplog
		ol := newOplog(OP_D, c.logicId, c.id, c.opbody)
		s.Lock()
		//mark data delete
		succ := s.Delete(c.id)
		if succ {
			s.slog.Append(ol)
		}
		s.Unlock()
		return succ

	} else {
		// log.Debug("MessageStore|Delete|chunkid:%d|%s\n", c.id, c.logicId)
		return false
	}
}

//mark data expired
func (self *MessageStore) Expired(c *command) bool {

	s := self.indexSegment(c.id)
	if nil != s {
		//append oplog logic delete
		ol := newOplog(OP_E, c.logicId, c.id, c.opbody)
		s.Lock()
		//mark data expired
		succ := s.Expired(c.id)
		if succ {
			s.slog.Append(ol)
		}
		s.Unlock()
		return succ

	} else {
		// log.Debug("MessageStore|Expired|chunkid:%d|%s\n", cid, s)
		return false
	}
}

//write
func (self *MessageStore) Append(cmd *command) chan int64 {

	if self.running {
		//make channel for id
		ch := make(chan int64, 1)
		cmd.idchan = ch
		//write to channel for async flush
		self.writeChannel <- cmd
		return ch
	}

	return nil
}

//check if
func (self *MessageStore) checkRoll() (*Segment, int64) {
	//if current segment bytesize is larger than max segment size
	//create a new segment for storage
	var s *Segment
	var cid int64 = 0
	self.Lock()
	defer self.Unlock()
	if len(self.segments) <= 0 {
		nextId := self.chunkId
		if nextId < 0 {
			nextId = 0
		}
		news, err := self.createSegment(nextId)
		if nil == err {
			//append new
			self.segments = append(self.segments, news)
			s = news

		} else {
			//panic  first segment fail
			panic(err)
		}
		cid = nextId
	} else {
		s = self.segments[len(self.segments)-1]
		nid := self.cid()
		cid = nid
		if s.byteSize > MAX_SEGMENT_SIZE {
			news, err := self.createSegment(nid)
			if nil == err {
				//left segments are larger than cached ,close current
				if len(self.segments) >= self.segcacheSize {
					//truncate data
					s.Truncate()
				}
				//append new
				self.segments = append(self.segments, news)
				s = news
			}
		}
	}

	return s, cid
}

//create segemnt
func (self *MessageStore) createSegment(nextStart int64) (*Segment, error) {
	name := fmt.Sprintf("%s-%d", SEGMENT_PREFIX, nextStart) + SEGMENT_DATA_SUFFIX
	logname := fmt.Sprintf("%s-%d", SEGMENT_PREFIX, nextStart) + SEGMENT_LOG_SUFFIX

	sl := &SegmentLog{
		offset: 0,
		path:   self.filePath + logname}

	news := newSegment(self.filePath+name, name, nextStart, sl)
	err := news.Open(self.replay)
	if nil != err {
		log.Error("MessageStore|createSegment|Open Segment|FAIL|%s", news.path)
		return nil, err
	}
	// log.Debug("MessageStore|createSegment|SUCC|%s", news.path)
	return news, nil

}

func (self *MessageStore) sync() {
	MAX_BYTES := 8 * 1024 // 8K
	batch := make([]*command, 0, self.batchSize)
	batchLog := make([]*oplog, 0, self.batchSize)
	chunks := make(Chunks, 0, self.batchSize)
	var cmd *command
	currBytes := 0
	ticker := time.NewTicker(5 * time.Millisecond)
	var curr *Segment
	for self.running {

		//no batch / wait for data
		select {
		case cmd = <-self.writeChannel:
		case <-ticker.C:
			//no write data flush
		}

		if nil != cmd {
			s, id := self.checkRoll()
			if nil == s {
				log.Error("MessageStore|sync|checkRoll|FAIL...")
				cmd.idchan <- -1
				close(cmd.idchan)
				continue
			}
			cmd.id = id
			cmd.seg = s
			c := cmd
			//init curr segment
			if nil == curr {
				curr = cmd.seg
			}

			//check if segment changed,and then flush data
			//else flush old data
			if curr.sid != c.seg.sid {
				//force flush
				flush(curr, chunks[:0], batchLog[:0], batch)
				batch = batch[:0]
				//change curr to  newsegment
				curr = c.seg
			}

			batch = append(batch, c)
			currBytes += CHUNK_HEADER + len(c.msg)
		}

		//force flush
		if nil == cmd && len(batch) > 0 || currBytes >= MAX_BYTES || len(batch) >= cap(batch) {
			flush(curr, chunks[:0], batchLog[:0], batch)
			batch = batch[:0]
		}
		cmd = nil
	}

	// need flush left data
outter:
	for {
		select {
		case c := <-self.writeChannel:
			if nil != c {
				if nil == curr {
					curr = c.seg
				}
				if c.seg.sid != curr.sid {
					flush(curr, chunks[:0], batchLog[:0], batch)
					batch = batch[:0]
				}
				batch = append(batch, c)
			} else {
				//channel close
				break outter
			}

		default:
			break outter
		}
	}

	//last flush
	flush(curr, chunks[:0], batchLog[:0], batch)

	ticker.Stop()
	self.waitSync.Done()
	log.Info("MessageStore|SYNC|CLOSE...")
}

func flush(s *Segment, chunks Chunks, logs []*oplog, cmds []*command) {
	if len(cmds) > 0 {

		for _, c := range cmds {
			//append log
			ol := newOplog(OP_C, c.logicId, c.id, c.opbody)
			logs = append(logs, ol)

			//create chunk
			chunk := &Chunk{
				flag:     NORMAL,
				length:   int32(CHUNK_HEADER + len(c.msg)),
				id:       c.id,
				checksum: crc32.ChecksumIEEE(c.msg),
				data:     c.msg}
			chunks = append(chunks, chunk)

		}

		s.Lock()
		//append logs
		s.slog.Appends(logs)
		//complete
		err := s.Append(chunks)
		if nil != err {
			log.Error("MessageStore|Append|FAIL|%s\n", err)
		}
		s.Unlock()

		//release
		for _, c := range cmds {
			c.idchan <- c.id
			close(c.idchan)
		}
	}
}

func (self *MessageStore) Destory() {
	self.Lock()
	defer self.Unlock()
	self.running = false
	close(self.writeChannel)
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
