package store

import (
	"fmt"
	"kiteq/protocol"
	"os"
)

var f, _ = os.OpenFile("/dev/urandom", os.O_RDONLY, 0)

//生成messageId uuid
func MessageId() string {
	b := make([]byte, 16)
	f.Read(b)
	return fmt.Sprintf("%x", b)
}

//用于持久化的messageEntity
type MessageEntity struct {
	Header *protocol.Header `kiteq:"header"`
	Body   []byte           `kiteq:"body"` //序列化后的消息
	//-----------------
	MsgType uint8 `kiteq:"msg_type"` //消息类型

	MessageId       string   `kiteq:"messageId"`
	Topic           string   `kiteq:"topic"`                   //Topic
	MessageType     string   `kiteq:"messageType"`             //MessageType
	PublishGroup    string   `kiteq:"publish_group"`           //发布的groupId
	Commit          bool     `kiteq:"commit"`                  //是否已提交
	ExpiredTime     int64    `kiteq:"expiredTime"`             //过期时间
	DeliverCount    int32    `kiteq:"deliver_count"`           //投递次数
	KiteServer      string   `kiteq:"kite_server"`             // 当前的处理kiteqserver地址
	FailGroups      []string `kiteq:"failGroups,omitempty"`    //投递失败的分组tags
	DeliverGroups   []string `kiteq:"deliverGroups,omitempty"` //投递成功的分组tags
	NextDeliverTime int64    `kiteq:"next_deliver_time"`       //下一次投递的时间

}

func (self *MessageEntity) String() string {
	return fmt.Sprintf("id:%s Topic:%s Commit:%t Body:%s", self.MessageId, self.Topic, self.Commit, string(self.Body))
}

func (self *MessageEntity) GetBody() []byte {
	return self.Body
}

//创建stringmessage
func NewStringMessageEntity(msg *protocol.StringMessage) *MessageEntity {
	entity := &MessageEntity{
		Header:       msg.GetHeader(),
		Body:         []byte(msg.GetBody()),
		MessageId:    msg.GetHeader().GetMessageId(),
		Topic:        msg.GetHeader().GetTopic(),
		MessageType:  msg.GetHeader().GetMessageType(),
		PublishGroup: msg.GetHeader().GetGroupId(),
		Commit:       msg.GetHeader().GetCommit(),
		ExpiredTime:  msg.GetHeader().GetExpiredTime(),
		DeliverCount: 0,
		//消息种类
		MsgType: protocol.CMD_STRING_MESSAGE}
	return entity

}

//创建bytesmessage的实体
func NewBytesMessageEntity(msg *protocol.BytesMessage) *MessageEntity {
	entity := &MessageEntity{
		Header:       msg.GetHeader(),
		Body:         msg.GetBody(),
		MessageId:    msg.GetHeader().GetMessageId(),
		Topic:        msg.GetHeader().GetTopic(),
		MessageType:  msg.GetHeader().GetMessageType(),
		PublishGroup: msg.GetHeader().GetGroupId(),
		Commit:       msg.GetHeader().GetCommit(),
		ExpiredTime:  msg.GetHeader().GetExpiredTime(),
		DeliverCount: 0,
		//消息种类
		MsgType: protocol.CMD_BYTES_MESSAGE}

	return entity
}

//kitestore存储
type IKiteStore interface {
	Query(messageId string) *MessageEntity
	Save(entity *MessageEntity) bool
	Commit(messageId string) bool
	Rollback(messageId string) bool
	UpdateEntity(entity *MessageEntity) bool
	Delete(messageId string) bool
}
