package file

import (
	"bufio"
	_ "encoding/binary"
	"encoding/json"
	log "github.com/blackbeans/log4go"
	"io"
	"os"
	"sync/atomic"
	"time"
)

const (
	OP_C = 'c' //create
	OP_U = 'u' //update
	OP_D = 'd' //delete
	OP_E = 'e' //expired
)

type SegmentLog struct {
	offset int64 // log offset
	path   string
	rf     *os.File      //*
	wf     *os.File      //* log file
	bw     *bufio.Writer //*
	br     *bufio.Reader //* oplog buffer
	isOpen int32
}

func newSegmentLog(offset int64, path string) *SegmentLog {
	return &SegmentLog{
		offset: offset,
		path:   path}

}

func (self *SegmentLog) Open() error {
	var rf *os.File
	var wf *os.File

	if atomic.CompareAndSwapInt32(&self.isOpen, 0, 1) {
		//file exist
		if _, err := os.Stat(self.path); err == nil {
			wf, err = os.OpenFile(self.path, os.O_RDWR|os.O_APPEND, os.ModePerm)
			if nil != err {
				log.Error("SegmentLog|Open|FAIL|%s|%s", err, self.path)
				return err
			}

			rf, err = os.OpenFile(self.path, os.O_RDWR, os.ModePerm)
			if nil != err {
				log.Error("SegmentLog|Open|FAIL|%s|%s", err, self.path)
				return err
			}
		} else {
			//file not exist create file
			wf, err = os.OpenFile(self.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
			if nil != err {
				log.Error("SegmentLog|Open|FAIL|%s|%s", err, self.path)
				return err
			}

			rf, err = os.OpenFile(self.path, os.O_CREATE|os.O_RDWR, os.ModePerm)
			if nil != err {
				log.Error("SegmentLog|Open|FAIL|%s|%s", err, self.path)
				return err
			}
		}

		self.rf = rf
		self.wf = wf

		//buffer
		self.br = bufio.NewReader(rf)
		self.bw = bufio.NewWriter(wf)
		log.Info("SegmentLog|Open|SUCC|%s", self.path)
	}
	return nil
}

//traverse oplog
func (self *SegmentLog) Replay(do func(l *oplog)) {

	self.Open()
	offset := 0
	for {
		line, err := self.br.ReadBytes(SEGMENT_LOG_SPLIT)
		if nil != err || len(line) <= 0 {
			break
		} else {
			ol := &oplog{}

			err := ol.unmarshal(line)
			if nil != err {
				log.Error("SegmentLog|Traverse|unmarshal|oplog|FAIL|%s|%s", err, line)
				continue
			}
			do(ol)
			offset++
		}
	}
	self.offset = int64(offset)
}

//apend data
func (self *SegmentLog) BatchAppend(logs []*oplog) error {

	buff := make([]byte, 0, 2*1024)
	for _, l := range logs {
		buff = append(buff, l.marshal()...)
	}
	tmp := buff
	for {
		l, err := self.bw.Write(tmp)
		if nil != err && err != io.ErrShortWrite {
			log.Error("SegmentLog|BatchAppend|FAIL|%s|%d/%d", err, l, len(tmp))
			return err
		} else if nil == err {
			break
		} else {
			self.bw.Reset(self.wf)
		}
		tmp = tmp[l:]
	}

	//increase offset
	atomic.AddInt64(&self.offset, int64(len(buff)))
	return nil
}

//apend data
func (self *SegmentLog) Append(ol *oplog) error {
	buff := ol.marshal()
	tmp := buff
	for {
		l, err := self.bw.Write(tmp)
		if nil != err && err != io.ErrShortWrite {
			log.Error("SegmentLog|Append|FAIL|%s|%d/%d", err, l, len(tmp))
			return err
		} else if nil == err {
			break
		} else {
			self.bw.Reset(self.wf)
		}
		tmp = tmp[l:]
	}
	self.bw.Flush()

	//line
	atomic.AddInt64(&self.offset, 1)
	return nil
}

func (self *SegmentLog) Close() error {
	if atomic.CompareAndSwapInt32(&self.isOpen, 1, 0) {
		err := self.bw.Flush()
		if nil != err {
			log.Error("SegmentLog|Close|Writer|FLUSH|FAIL|%s|%s\n", err, self.path)
		}

		err = self.wf.Close()
		if nil != err {
			log.Error("SegmentLog|Close|Write FD|FAIL|%s|%s\n", err, self.path)
			return err
		} else {
			err = self.rf.Close()
			if nil != err {
				log.Error("SegmentLog|Close|Read FD|FAIL|%s|%s\n", err, self.path)
			}
			return err
		}
		return nil

	} else if self.isOpen == 1 {
		return self.Close()
	}

	return nil
}

//data operation log
type oplog struct {
	Time    int64  `json:"time"`
	Op      byte   `json:"op"`
	ChunkId int64  `json:"chunk_id"`
	LogicId string `json:"logic_id"`
	Body    []byte `json:"body"`
}

func newOplog(op byte, logicId string, chunkid int64, body []byte) *oplog {
	return &oplog{
		Time:    time.Now().Unix(),
		Op:      op,
		ChunkId: chunkid,
		LogicId: logicId,
		Body:    body}
}

//marshal oplog
func (self *oplog) marshal() []byte {
	d, err := json.Marshal(self)
	if nil != err {
		log.Error("oplog|marshal|fail|%s|%s", err, self)
		return nil
	}
	return append(d, SEGMENT_LOG_SPLIT)
}

//unmarshal data
func (self *oplog) unmarshal(data []byte) error {
	return json.Unmarshal(data, self)

}
