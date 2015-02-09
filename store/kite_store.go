package store

import (
	"fmt"
	"kiteq/protocol"
)

//用于持久化的messageEntity
type MessageEntity struct {
	Header *protocol.Header
	Body   []byte //序列化后的消息
	//-----------------
	MsgType uint8 //消息类型

	MessageId     string
	Topic         string   //Topic
	MessageType   string   //MessageType
	PublishGroup  string   //发布的groupId
	Commit        bool     //是否已提交
	ExpiredTime   int64    //过期时间
	DeliverCount  int32    //投递次数
	KiteQServer   string   // 当前的处理kiteqserver地址
	FailGroupTags []string //投递失败的分组tags
	// succGroupTags   []string // 投递成功的分组
	NextDeliverTime int64 //下一次投递的时间

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
		MessageId:    msg.GetHeader().GetMessageId(),
		Topic:        msg.GetHeader().GetTopic(),
		PublishGroup: msg.GetHeader().GetGroupId(),
		MessageType:  msg.GetHeader().GetMessageType(),
		Commit:       msg.GetHeader().GetCommit(),
		ExpiredTime:  msg.GetHeader().GetExpiredTime(),
		DeliverCount: 0,
		//消息种类
		MsgType: protocol.CMD_STRING_MESSAGE,
		Body:    []byte(msg.GetBody())}
	return entity

}

//创建bytesmessage的实体
func NewBytesMessageEntity(msg *protocol.BytesMessage) *MessageEntity {
	entity := &MessageEntity{
		Header:       msg.GetHeader(),
		MessageId:    msg.GetHeader().GetMessageId(),
		Topic:        msg.GetHeader().GetTopic(),
		PublishGroup: msg.GetHeader().GetGroupId(),
		MessageType:  msg.GetHeader().GetMessageType(),
		Commit:       msg.GetHeader().GetCommit(),
		ExpiredTime:  msg.GetHeader().GetExpiredTime(),
		DeliverCount: 0,
		//消息种类
		MsgType: protocol.CMD_BYTES_MESSAGE,
		Body:    msg.GetBody()}

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
