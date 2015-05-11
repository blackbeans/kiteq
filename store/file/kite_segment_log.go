package file

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
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

func newSegmentLog(path string) *SegmentLog {
	return &SegmentLog{
		path: path}

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
	offset := int64(0)

	for {
		var length int32

		err := binary.Read(self.br, binary.BigEndian, &length)
		if nil != err {
			if err == io.EOF {
				break
			}
			log.Warn("SegmentLog|Replay|LEN|%s|Skip...", err)
			continue
		}

		// log.Debug("SegmentLog|Replay|LEN|%d", length)

		tmp := make([]byte, length-4)

		err = binary.Read(self.br, binary.BigEndian, tmp)
		if nil != err {
			log.Error("SegmentLog|Replay|Data|%s", err)
			break
		}

		var ol oplog
		r := bytes.NewReader(tmp)
		deco := gob.NewDecoder(r)
		err = deco.Decode(&ol)
		if nil != err {
			log.Error("SegmentLog|Replay|unmarshal|oplog|FAIL|%s", err)
			continue
		}
		// log.Error("SegmentLog|Replay|oplog|%s", ol)
		do(&ol)
		//line
		offset += int64(length)

	}
	self.offset = int64(offset)
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
	atomic.AddInt64(&self.offset, int64(len(buff)))
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

	buff := new(bytes.Buffer)

	binary.Write(buff, binary.BigEndian, int32(0))
	encoder := gob.NewEncoder(buff)
	err := encoder.Encode(self)
	if nil != err {
		log.Error("oplog|marshal|fail|%s|%s", err, self)
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
