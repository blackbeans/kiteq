package store

import (
	"github.com/golang/protobuf/proto"
	"kiteq/protocol"
)

type MockKiteStore struct {
}

func (self *MockKiteStore) Query(messageId string) *MessageEntity {
	entity := NewStringMessageEntity(buildStringMessage())
	return entity

}
func (self *MockKiteStore) Save(entity *MessageEntity) bool {
	return true
}
func (self *MockKiteStore) Commit(messageId string) bool {
	return true
}
func (self *MockKiteStore) Rollback(messageId string) bool {
	return true
}
func (self *MockKiteStore) UpdateEntity(entity *MessageEntity) bool {
	return true
}

func buildStringMessage() *protocol.StringMessage {
	//创建消息
	entity := &protocol.StringMessage{}
	entity.Header = &protocol.Header{
		MessageId:   proto.String("1234567"),
		Topic:       proto.String("trade"),
		MessageType: proto.String("pay-succ"),
		ExpiredTime: proto.Int64(13700000000),
		GroupId:     proto.String("go-kite-test"),
		Commit:      proto.Bool(true)}
	entity.Body = proto.String("hello go-kite")

	return entity
}
