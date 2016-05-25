package store

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/golang/protobuf/proto"
	"time"
)

type MockKiteStore struct {
}

func NewMockKiteStore() *MockKiteStore {
	return &MockKiteStore{}
}

func (self *MockKiteStore) Start()          {}
func (self *MockKiteStore) Stop()           {}
func (self *MockKiteStore) Monitor() string { return "mock" }

func (self *MockKiteStore) RecoverNum() int {
	return 0
}

func (self *MockKiteStore) Length() map[string] /*topic*/ int {
	//TODO mysql中的未过期的消息数量

	return make(map[string] /*topic*/ int, 1)
}

func (self *MockKiteStore) AsyncUpdate(entity *MessageEntity) bool   { return true }
func (self *MockKiteStore) AsyncDelete(topic, messgeid string) bool  { return true }
func (self *MockKiteStore) AsyncCommit(topic, messageId string) bool { return true }
func (self *MockKiteStore) Expired(topic, messageId string) bool     { return true }

func (self *MockKiteStore) Query(topic, messageId string) *MessageEntity {
	entity := NewMessageEntity(protocol.NewQMessage(buildBytesMessage(messageId)))
	return entity

}
func (self *MockKiteStore) Save(entity *MessageEntity) bool {
	return true
}
func (self *MockKiteStore) Commit(topic, messageId string) bool {
	return true
}

func (self *MockKiteStore) Delete(topic, messageId string) bool {
	return true
}
func (self *MockKiteStore) BatchDelete(topic, messageId []string) bool {
	return true
}
func (self *MockKiteStore) Rollback(topic, messageId string) bool {
	return true
}

func (self *MockKiteStore) BatchUpdate(entity []*MessageEntity) bool {
	return true
}

func (self *MockKiteStore) MoveExpired() {

}

func (self *MockKiteStore) PageQueryEntity(hashKey string, kiteServer string, nextDeliveryTime int64, startIdx, limit int) (bool, []*MessageEntity) {
	recoverMessage := buildStringMessage(MessageId())
	entity := NewMessageEntity(protocol.NewQMessage(recoverMessage))
	entity.DeliverCount = 10
	entity.SuccGroups = []string{"a", "b"}
	entity.FailGroups = []string{"c", "d"}
	return false, []*MessageEntity{entity}
}

func buildStringMessage(id string) *protocol.StringMessage {
	//创建消息
	entity := &protocol.StringMessage{}
	entity.Header = &protocol.Header{
		MessageId:    proto.String(id),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
		DeliverLimit: proto.Int32(100),
		GroupId:      proto.String("go-kite-test"),
		Commit:       proto.Bool(true),
		Fly:          proto.Bool(false)}
	entity.Body = proto.String("hello go-kite")

	return entity
}

func buildBytesMessage(id string) *protocol.BytesMessage {
	//创建消息
	entity := &protocol.BytesMessage{}
	entity.Header = &protocol.Header{
		MessageId:    proto.String(id),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
		DeliverLimit: proto.Int32(100),
		GroupId:      proto.String("go-kite-test"),
		Commit:       proto.Bool(true),
		Fly:          proto.Bool(false)}
	entity.Body = []byte("hello go-kite")

	return entity
}
