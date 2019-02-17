package file

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	log "github.com/blackbeans/log4go"
	"io"
	"os"
	"sync"
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
	sync.RWMutex
	isOpen int32
}

func newSegmentLog(path string) *SegmentLog {
	return &SegmentLog{
		path: path}

}

func (self *SegmentLog) Open() error {
	var rf *os.File
	var wf *os.File
	if atomic.CompareAndSwapInt32(&self.isOpen, 0, 1) {
		//file exist
		_, err := os.Stat(self.path)
		if os.IsNotExist(err) {
			_, err := os.Create(self.path)
			if nil != err {
				log.ErrorLog("kite_store", "SegmentLog|Create|FAIL|%s|%s", err, self.path)
				return err
			}
		}

		//file not exist create file
		wf, err = os.OpenFile(self.path, os.O_RDWR|os.O_APPEND, os.ModePerm)
		if nil != err {
			log.ErrorLog("kite_store", "SegmentLog|Open|FAIL|%s|%s", err, self.path)
			return err
		}

		rf, err = os.OpenFile(self.path, os.O_RDWR, os.ModePerm)
		if nil != err {
			log.ErrorLog("kite_store", "SegmentLog|Open|FAIL|%s|%s", err, self.path)
			return err
		}

		self.rf = rf
		self.wf = wf
		//buffer
		self.br = bufio.NewReader(rf)
		self.bw = bufio.NewWriter(wf)
		log.InfoLog("kite_store", "SegmentLog|Open|SUCC|%s", self.path)
	}
	return nil
}

// Replay: traverse oplog
func (self *SegmentLog) Replay(do func(l *oplog)) {

	self.Open()
	offset := int64(0)
	tmp := make([]byte, 1024)
	//seek to head
	self.rf.Seek(0, 0)
	self.br.Reset(self.rf)

	for {
		var length int32
		err := binary.Read(self.br, binary.BigEndian, &length)
		if nil != err {
			if err == io.EOF {
				self.br.Reset(self.rf)
				break
			}
			log.WarnLog("kite_store", "SegmentLog|Replay|LEN|%s|Skip...", err)
			continue
		}

		// log.Debug("SegmentLog|Replay|LEN|%d", length)

		if int(length) > cap(tmp) {
			grow := make([]byte, int(length)-cap(tmp))
			tmp = append(tmp[0:cap(tmp)], grow...)
		}

		err = binary.Read(self.br, binary.BigEndian, tmp[:int(length)-4])
		if nil != err {
			self.br.Reset(self.rf)
			log.ErrorLog("kite_store", "SegmentLog|Replay|Data|%s", err)
			break
		}

		var ol oplog
		r := bytes.NewReader(tmp[:int(length)-4])
		deco := gob.NewDecoder(r)
		err = deco.Decode(&ol)
		if nil != err {
			log.ErrorLog("kite_store", "SegmentLog|Replay|unmarshal|oplog|FAIL|%s", err)
			continue
		}
		// log.Debug("SegmentLog|Replay|oplog|%s", ol)
		do(&ol)
		//line
		offset += int64(length)

	}
	self.offset = int64(offset)
}

// Appends: apend data
func (self *SegmentLog) Appends(logs []*oplog) error {

	//if closed
	if self.isOpen == 0 {
		return errors.New(fmt.Sprintf("SegmentLog Is Closed!|%s", self.path))
	}

	length := int64(0)
	for _, lo := range logs {
		tmp := lo.marshal()
		for {
			l, err := self.bw.Write(tmp)
			length += int64(l)
			if nil != err && err != io.ErrShortWrite {
				log.ErrorLog("kite_store", "SegmentLog|Append|FAIL|%s|%d/%d", err, l, len(tmp))
				return err
			} else if nil == err {
				break
			} else {
				self.bw.Reset(self.wf)
				log.ErrorLog("kite_store", "SegmentLog|Append|FAIL|%s", err)
			}
			tmp = tmp[l:]

		}
	}

	//flush
	self.bw.Flush()

	//move offset
	atomic.AddInt64(&self.offset, int64(length))
	return nil
}

// Append: apend data
func (self *SegmentLog) Append(ol *oplog) error {

	//if closed
	if self.isOpen == 0 {
		return errors.New(fmt.Sprintf("SegmentLog Is Closed!|%s", self.path))
	}

	buff := ol.marshal()
	tmp := buff
	for {
		l, err := self.bw.Write(tmp)
		if nil != err && err != io.ErrShortWrite {
			log.ErrorLog("kite_store", "SegmentLog|Append|FAIL|%s|%d/%d", err, l, len(tmp))
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
	atomic.AddInt64(&self.offset, int64(len(buff)))
	return nil
}

func (self *SegmentLog) Close() error {
	if atomic.CompareAndSwapInt32(&self.isOpen, 1, 0) {
		err := self.bw.Flush()
		if nil != err {
			log.ErrorLog("kite_store", "SegmentLog|Close|Writer|FLUSH|FAIL|%s|%s\n", err, self.path)
		}

		err = self.wf.Close()
		if nil != err {
			log.ErrorLog("kite_store", "SegmentLog|Close|Write FD|FAIL|%s|%s\n", err, self.path)
			return err
		} else {
			err = self.rf.Close()
			if nil != err {
				log.ErrorLog("kite_store", "SegmentLog|Close|Read FD|FAIL|%s|%s\n", err, self.path)
			}
			return err
		}
		return nil

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

	buff := new(bytes.Buffer)

	binary.Write(buff, binary.BigEndian, int32(0))
	encoder := gob.NewEncoder(buff)
	err := encoder.Encode(self)
	if nil != err {
		log.ErrorLog("kite_store", "oplog|marshal|fail|%s|%s", err, self)
		return nil
	}
	b := buff.Bytes()
	binary.BigEndian.PutUint32(b, uint32(buff.Len()))
	return b
}

//unmarshal data
func (self *oplog) unmarshal(data []byte) error {
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r)
	return dec.Decode(self)

}
