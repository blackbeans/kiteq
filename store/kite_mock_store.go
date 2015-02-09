package store

import (
	"github.com/golang/protobuf/proto"
	"kiteq/protocol"
	"time"
)

type MockKiteStore struct {
}

func (self *MockKiteStore) Query(messageId string) *MessageEntity {
	entity := NewStringMessageEntity(buildStringMessage(messageId))
	return entity

}
func (self *MockKiteStore) Save(entity *MessageEntity) bool {
	return true
}
func (self *MockKiteStore) Commit(messageId string) bool {
	return true
}

func (self *MockKiteStore) Delete(messageId string) bool {
	return true
}
func (self *MockKiteStore) Rollback(messageId string) bool {
	return true
}
func (self *MockKiteStore) UpdateEntity(entity *MessageEntity) bool {
	return true
}

func buildStringMessage(id string) *protocol.StringMessage {
	//创建消息
	entity := &protocol.StringMessage{}
	entity.Header = &protocol.Header{
		MessageId:     proto.String(id),
		Topic:         proto.String("trade"),
		MessageType:   proto.String("pay-succ"),
		ExpiredTime:   proto.Int64(time.Now().Unix()),
		DeliveryLimit: proto.Int32(-1),
		GroupId:       proto.String("go-kite-test"),
		Commit:        proto.Bool(true)}
	entity.Body = proto.String("hello go-kite")

	return entity
}
