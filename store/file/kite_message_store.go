package file

import (
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
	replay       func(oplog *oplog) //oplog replay
	checkPeriod  time.Duration
	sync.RWMutex
}

func NewMessageStore(filePath string, batchSize int,
	checkPeriod time.Duration, replay func(oplog *oplog)) *MessageStore {

	ms := &MessageStore{
		chunkId:      -1,
		filePath:     filePath,
		segments:     make(Segments, 0, 50),
		writeChannel: make(chan *command, 10000),
		batchSize:    batchSize,
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
	remove := make([]*Segment, 0, 10)
	for self.running {
		time.Sleep(self.checkPeriod)

		stat := ""
		func() {
			self.RLock()
			defer self.RUnlock()
			//保证当前数据都是已经写入了至少一个分块
			if len(self.segments) < 2 {
				//do nothing
			} else {

				//check 0...n-1 segment stat
				for _, s := range self.segments[:len(self.segments)-1] {
					func() {
						//sync
						s.RLock()
						defer s.RUnlock()
						total, normal, del, expired := s.stat()
						stat += fmt.Sprintf("|%s\t|%d\t|%d\t|%d\t|%d\t|\n", s.name, total, normal, del, expired)
						if normal <= 0 {
							remove = append(remove, s)
						}
					}()
				}

			}

		}()

		if len(stat) > 0 {
			log.InfoLog("kite_store", "\n---------------MessageStore-Stat--------------\n"+
				"|segment\t\t|total\t|normal\t|delete\t|expired\t|\n%s", stat)
		}

		if len(remove) > 0 {
			go self.remove(remove)
		}
		remove = remove[:0]
	}
}

//remove segment
func (self *MessageStore) remove(removes []*Segment) {

	func() {
		self.Lock()
		defer self.Unlock()
		copySeg := make([]*Segment, 0, 10)
		for _, s := range self.segments {
			exist := false
			for _, r := range removes {
				if s.sid == r.sid {
					//skip
					exist = true
					break
				} else {

				}
			}

			if !exist {
				copySeg = append(copySeg, s)
			}
		}
		self.segments = copySeg
	}()

	//remove from segments
	for _, s := range removes {
		//close segment
		s.Close()

		if _, err := os.Stat(s.path); nil == err {
			err := os.Remove(s.path)
			if nil != err {
				log.WarnLog("kite_store", "MessageStore|Remove|Segment|FAIL|%s|%s", err, s.path)
			}

		}
		if _, err := os.Stat(s.slog.path); nil == err {
			err = os.Remove(s.slog.path)
			if nil != err {
				log.WarnLog("kite_store", "MessageStore|Remove|SegmentLog|FAIL|%s|%s", err, s.slog.path)
			}
		}
		log.InfoLog("kite_store", "MessageStore|Remove|Segment|%s", s.path)
	}
}

func (self *MessageStore) load() {
	log.InfoLog("kite_store", "MessageStore|Load Segments ...")

	if !dirExist(self.filePath) {
		err := os.MkdirAll(self.filePath, os.ModePerm)
		if nil != err {
			log.ErrorLog("kite_store", "MessageStore|Load Segments|MKDIR|FAIL|%s|%s", err, self.filePath)
			panic(err)
		}
	}

	bashDir, err := os.Open(self.filePath)
	if nil != err {
		log.ErrorLog("kite_store", "MessageStore|Load Segments|FAIL|%s|%s", err, self.filePath)
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
				log.ErrorLog("kite_store", "MessageStore|Load Segments|Parse SegmentId|FAIL|%s|%s", err, name)
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

	log.InfoLog("kite_store", "MessageStore|Load|SUCC|%s", self)
}

func (self *MessageStore) recoverSnapshot() {
	//current segmentid
	if len(self.segments) > 0 {
		removes := make([]*Segment, 0, 10)
		removeCount := 0
		//replay log
		for i, s := range self.segments {
			err := s.Open(self.replay)
			if nil != err {
				log.ErrorLog("kite_store", "MessageStore|recoverSnapshot|Fail|%s", err, s.slog.path)
				panic(err)
			}

			total, normal, del, expired := s.stat()
			if normal <= 0 || total == (del+expired) {
				removes = append(removes, s)
				removeCount++
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

				if self.chunkId <= 0 {
					self.chunkId = s.sid - 1
				}
			}

			log.DebugLog("kite_store", "MessageStore|recoverSnapshot|%s", s.name)
		}

		if len(removes) > 0 {
			self.remove(removes)
		}
	}

	//check init chunkId
	if len(self.segments) <= 0 {
		self.chunkId = -1
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

	// not exist In cache
	if nil == curr {
		curr = func() *Segment {
			self.RLock()
			defer self.RUnlock()
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

			if len(self.segments) > 0 && idx < len(self.segments) {
				return self.segments[idx]
			}
			return nil
		}()

		// log.Debug("kite_store","MessageStore|indexSegment|%d", curr.path)

	}
	return curr
}

// Update: append log
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

// Delete: mark delete
func (self *MessageStore) Delete(c *command) bool {
	s := self.indexSegment(c.id)
	if nil != s {

		//append oplog
		ol := newOplog(OP_D, c.logicId, c.id, c.opbody)
		s.Lock()
		//make slog append
		err := s.slog.Append(ol)
		if nil == err {
			//mark data delete
			s.Delete(c.id)
		} else {
		}
		s.Unlock()
		return nil == err

	} else {
		// log.DebugLog("kite_store","MessageStore|Delete|chunkid:%d|%s\n", c.id, c.logicId)
		return false
	}
}

// Expired: mark data expired
func (self *MessageStore) Expired(c *command) bool {

	s := self.indexSegment(c.id)
	if nil != s {
		//append oplog logic delete
		ol := newOplog(OP_E, c.logicId, c.id, c.opbody)
		s.Lock()
		//mark data expired
		err := s.slog.Append(ol)
		if nil == err {
			s.Expired(c.id)
		}
		s.Unlock()
		return nil == err

	} else {
		// log.DebugLog("kite_store","MessageStore|Expired|chunkid:%d|%s\n", cid, s)
		return false
	}
}

// Append: write
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
	self.RLock()
	initial := (len(self.segments) <= 0)
	self.RUnlock()

	if !initial {

		nid := self.cid()
		cid = nid

		self.RLock()
		s = self.segments[len(self.segments)-1]
		create := (s.byteSize > MAX_SEGMENT_SIZE)
		self.RUnlock()

		func() {
			if create {
				self.Lock()
				defer self.Unlock()
				s = self.segments[len(self.segments)-1]
				create := (s.byteSize > MAX_SEGMENT_SIZE)
				if create {
					news, err := self.createSegment(nid)
					func() {
						if nil == err {
							//append new
							self.segments = append(self.segments, news)
							s = news
						} else {
							panic(err)
						}
					}()
				}

			} else {
				//don't need createSegment
			}
		}()
	} else {
		func() {
			self.Lock()
			defer self.Unlock()
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
		}()
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
		log.ErrorLog("kite_store", "MessageStore|createSegment|Open Segment|FAIL|%s", news.path)
		return nil, err
	}
	// log.Debug("MessageStore|createSegment|SUCC|%s", news.path)
	return news, nil

}

func (self *MessageStore) sync() {
	MAX_BYTES := 64 * 1024 // 64K
	batch := make([]*command, 0, self.batchSize)
	batchLog := make([]*oplog, 0, self.batchSize)
	chunks := make(Chunks, 0, self.batchSize)

	currBytes := 0
	ticker := time.NewTicker(1 * time.Second)
	var curr *Segment
	for self.running {
		var cmd *command
		//no batch / wait for data
		select {
		case cmd = <-self.writeChannel:
		case <-ticker.C:
			//no write data flush
		}

		if nil != cmd {

			s, id := self.checkRoll()
			if nil == s {
				log.ErrorLog("kite_store", "MessageStore|sync|checkRoll|FAIL...")
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
	}

	// need flush left data
outter:
	for {
		var cmd *command
		select {
		case cmd = <-self.writeChannel:

			if nil != cmd {
				if nil == cmd.seg {
					s, id := self.checkRoll()
					if nil == s {
						log.ErrorLog("kite_store", "MessageStore|sync|checkRoll|FAIL...")
						cmd.idchan <- -1
						close(cmd.idchan)
						continue
					}
					cmd.seg = s
					cmd.id = id
				}

				if nil == curr {
					curr = cmd.seg
				}

				if cmd.seg.sid != curr.sid {
					flush(curr, chunks[:0], batchLog[:0], batch)
					batch = batch[:0]
				}
				batch = append(batch, cmd)
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
	log.InfoLog("kite_store", "MessageStore|SYNC|CLOSE...")
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
			log.ErrorLog("kite_store", "MessageStore|Append|FAIL|%s\n", err)
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

	self.running = false
	close(self.writeChannel)
	self.waitSync.Wait()

	func() {
		self.Lock()
		defer self.Unlock()
		//close all segment
		for _, s := range self.segments {
			err := s.Close()
			if nil != err {
				log.ErrorLog("kite_store", "MessageStore|Destory|Close|FAIL|%s|sid:%d", err, s.sid)
			}
		}
		self.baseDir.Close()
	}()
	log.InfoLog("kite_store", "MessageStore|Destory...")
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
