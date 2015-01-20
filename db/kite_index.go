package db

import (
	"bytes"
	"encoding/binary"
	"github.com/datastream/btree"
	"log"
)

type KiteIndexItem struct {
	topic  string
	pageId int
}

func (self *KiteIndexItem) Marshal() []byte {
	length := 4 + len(self.topic)
	log.Println("index item len ", length, self.topic)
	buffer := make([]byte, 0, length)
	buff := bytes.NewBuffer(buffer)
	binary.Write(buff, binary.BigEndian, uint32(self.pageId))
	binary.Write(buff, binary.BigEndian, []byte(self.topic))
	return buff.Bytes()
}

func (self *KiteIndexItem) Unmarshal(b []byte) error {
	buff := bytes.NewReader(b)
	var pageId uint32
	binary.Read(buff, binary.BigEndian, &pageId)
	self.pageId = int(pageId)
	bs := make([]byte, len(b)-4)
	binary.Read(buff, binary.BigEndian, &bs)
	self.topic = string(bs)
	return nil
}

type KiteBtreeIndex struct {
	tree *btree.Btree
}

func NewIndex() *KiteBtreeIndex {
	ins := &KiteBtreeIndex{
		tree: btree.NewBtree(),
	}
	return ins
}

func (self *KiteBtreeIndex) Insert(messageId string, data *KiteIndexItem) error {
	log.Println("index ", data, data.Marshal())
	return self.tree.Insert([]byte(messageId), data.Marshal())
}

func (self *KiteBtreeIndex) Search(messageId string) (*KiteIndexItem, error) {
	b, err := self.tree.Search([]byte(messageId))
	log.Println("index result", b)
	if err != nil {
		return nil, err
	}
	ins := &KiteIndexItem{}
	ins.Unmarshal(b)
	log.Print("index unmarshal ", ins.topic)
	return ins, nil
}
