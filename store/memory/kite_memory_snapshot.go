package memory

import (
	"bufio"
	"encoding/binary"
	log "github.com/blackbeans/log4go"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	MAX_SEGEMENT_SIZE = 64 * 1024 * 1024 //最大的分段大仙
	MAX_CHUNK_SIZE    = 128 * 1024       //最大的chunk
	STORE_IDX_SUFFIX  = ".idx"
	STORE_DATA_SUFFIX = ".kiteq"
	CHUNK_HEADER      = 4 + 4 + 8 + 1 //|length 4byte|checksum 4byte|id 8byte|flag 1byte| data variant|
)

//消息文件
type Segement struct {
	path     string
	name     string //basename_0000000000
	file     *os.File
	sid      int32 //segment id
	offset   int64 //segment current offset
	byteSize int32 //segement size
	chunks   []*Chunk
}

//----------------------------------------------------
//|length 4byte|checksum 4byte|id 8byte|flag 1byte| data variant|
//----------------------------------------------------
//存储块
type Chunk struct {
	offset   int64
	length   int64
	checksum string
	flag     byte //chunk状态
	id       int64
	data     []byte // data
}

type Segements []*Segement

func (self Segements) Len() int { return len(self) }
func (self Segements) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}
func (self Segements) Less(i, j int) bool {
	return self[i].sid < self[j].sid
}

//内存的快照
type MemorySnapshot struct {
	filePath     string
	baseDir      *os.File
	basename     string
	segements    Segements
	maxSegmentId int32
}

func NewMemorySnapshot(filePath string, basename string) *MemorySnapshot {
	ms := &MemorySnapshot{
		filePath:  filePath,
		basename:  basename,
		segements: make(Segements, 0, 50)}
	ms.load()

	return ms
}

func (self *MemorySnapshot) load() {
	log.Info("MemorySnapshot|Load Segements ...")

	if !dirExist(self.filePath) {
		err := os.MkdirAll(self.filePath, os.ModeDir)
		if nil != err {
			log.Error("MemorySnapshot|Load Segements|MKDIR|FAIL|%s|%s\n", err, self.filePath)
			panic(err)
		}
	}

	bashDir, err := os.OpenFile(self.filePath, os.O_RDWR|os.O_APPEND, os.ModePerm)
	if nil != err {
		log.Error("MemorySnapshot|Load Segements|FAIL|%s|%s\n", err, self.filePath)
		panic(err)
	}

	self.baseDir = bashDir

	//fetch all Segement
	filepath.Walk(self.filePath, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() && f.Mode() == os.ModeAppend {
			df, err := os.Open(path)
			if nil != err {
				log.Error("MemorySnapshot|Load Segements|Open|FAIL|%s|%s\n", err, path)
				return err
			}

			name := strings.TrimSuffix(f.Name(), STORE_DATA_SUFFIX)
			split := strings.SplitN(name, "_", 2)
			sid := int32(0)
			if len(split) >= 2 {
				id, err := strconv.ParseInt(split[1], 10, 32)
				if nil != err {
					log.Error("MemorySnapshot|Load Segements|Parse SegmentId|FAIL|%s|%s\n", err, name)
					return err
				}
				sid = int32(id)
			} else {
				log.Error("MemorySnapshot|Load Segements|Open|FAIL|%s|%s\n", err, path)
				return err
			}

			// create segement
			seg := &Segement{
				path: path,
				name: f.Name(),
				file: df,
				sid:  sid}
			self.segements = append(self.segements, seg)
		}

		return nil
	})

	//sort segements
	sort.Sort(self.segements)
	//current segmentid
	if len(self.segements) > 0 {
		s := self.segements[len(self.segements)-1]
		self.maxSegmentId = s.sid

		fi, _ := s.file.Stat()

		filesize := fi.Size()
		offset := int64(0)
		//scan offset
		br := bufio.NewReader(s.file)
		header := make([]byte, 0, CHUNK_HEADER)

		for {
			l, err := br.Read(header)
			if nil != err || l < CHUNK_HEADER {
				log.Error("MemorySnapshot|Load Segement|Read Header|FAIL|%s|%s|%d\n", err, s.name, l)
				break
			}

			//length
			length := binary.BigEndian.Uint32(header[0:4])

			al := offset + CHUNK_HEADER + int64(length)
			//checklength
			if al > filesize {
				log.Error("MemorySnapshot|Load Segement|FILE SIZE|ERROR|%s|%s|%d/%d\n",
					err, s.name, al, filesize)
				break
			}

			//checksum
			checksum := binary.BigEndian.Uint32(header[4:8])
			//read data
			data := make([]byte, 0, length)
			bl, err := br.Read(data)
			if nil != err || bl < int(length) {
				log.Error("MemorySnapshot|Load Segement|Read Data|FAIL|%s|%s|%d\n", err, s.name, bl)
				break
			}
			csum := crc32.ChecksumIEEE(data)
			//checkdata
			if csum != checksum {
				log.Error("MemorySnapshot|Load Segement|Data Checksum|FAIL|%s|%s|%d/%d\n",
					err, s.name, csum, checksum)
				break
			}

			offset += int64(length)

		}
	}
	log.Info("MemorySnapshot|Load Segements|FAIL|%s|%s\n", err, self.filePath)
}

//write
func (self *MemorySnapshot) Append(msg []byte) int64 {
	return 1
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
