package store

import (
	"go-kite/protocol"
)

//用于持久化的messageEntity
type MessageEntity struct {
	Header       *protocol.Header
	MessageId    string
	Topic        string //topic
	messageType  string //messageType
	publishGroup string //发布的groupId
	Commited     bool   //是否已提交
	expiredTime  int64  //过期时间

	//-----------------
	msgType         uint8    //消息类型
	Body            []byte   //序列化后的消息
	failGroupTags   []string //投递失败的分组tags
	succGroupTags   []string // 投递成功的分组
	nextDeliverTime int64    //下一次投递的时间

}

//创建stringmessage
func NewStringMessageEntity(msg *protocol.StringMessage) *MessageEntity {
	entity := &MessageEntity{}
	entity.MessageId = msg.GetHeader().GetMessageId()
	entity.Header = msg.GetHeader()
	entity.Topic = msg.GetHeader().GetTopic()
	entity.publishGroup = msg.GetHeader().GetGroupId()
	entity.messageType = msg.GetHeader().GetMessageType()
	entity.Commited = msg.GetHeader().GetCommited()
	entity.expiredTime = msg.GetHeader().GetExpiredTime()

	//消息种类
	entity.msgType = protocol.CMD_TYPE_STRING_MESSAGE
	entity.Body = []byte(msg.GetBody())

	return entity

}

//创建bytesmessage的实体
func NewBytesMessageEntity(msg *protocol.BytesMessage) *MessageEntity {
	entity := &MessageEntity{}
	entity.Header = msg.GetHeader()
	entity.MessageId = msg.GetHeader().GetMessageId()
	entity.Topic = msg.GetHeader().GetTopic()
	entity.publishGroup = msg.GetHeader().GetGroupId()
	entity.messageType = msg.GetHeader().GetMessageType()
	entity.Commited = msg.GetHeader().GetCommited()
	entity.expiredTime = msg.GetHeader().GetExpiredTime()

	//消息种类
	entity.msgType = protocol.CMD_TYPE_BYTES_MESSAGE
	entity.Body = msg.GetBody()

	return entity
}

//kitestore存储
type IKiteStore interface {
	Query(messageId string) *MessageEntity
	Save(entity *MessageEntity) bool
	Commite(messageId string) bool
	Rollback(messageId string) bool
	UpdateEntity(entity *MessageEntity) bool
}
