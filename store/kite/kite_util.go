package kite

import (
	"errors"
)

type Type *KiteDBPage

// 一个固定长度的环状队列
type KiteRingQueue struct {
	front int // 队头指针
	rear  int // 队尾指针
	size  int // 队列最大长度
	data  []Type
}

func (self *KiteRingQueue) Len() int {
	return (self.front - self.rear + self.size) % self.size
}

func NewKiteRingQueue(size int) *KiteRingQueue {
	return &KiteRingQueue{
		size:  size,
		front: 0,
		rear:  0,
		data:  [size + 1]Type{},
	}
}

func (self *KiteRingQueue) Enqueue(e Type) error {
	//牺牲一个存储单元判断队列为满
	if (self.rear+1)%self.size == self.front {
		return errors.New("queue is full")
	}
	self.data[self.rear] = e
	self.rear = (self.rear + 1) % self.size
	return nil
}

func (self *KiteRingQueue) Dequeue() (Type, error) {
	if self.rear == self.front {
		return nil, errors.New("queue is empty")
	}
	data := self.data[self.front]
	self.front = (self.front + 1) % self.size
	return data, nil
}
