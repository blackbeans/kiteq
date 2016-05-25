package store

import (
	"fmt"
	"github.com/blackbeans/go-uuid"
	"github.com/blackbeans/kiteq-common/protocol"
)

//生成messageId uuid
func MessageId() string {
	id := uuid.NewRandom()
	if id == nil || len(id) != 16 {
		return ""
	}
	b := []byte(id)
	return fmt.Sprintf("%08x%04x%04x%04x%012x",
		b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

//用于持久化的messageEntity
type MessageEntity struct {
	Id        int32            `kiteq:"id" db:"transient"`
	MessageId string           `kiteq:"messageId" db:"message_id,pk"`
	Header    *protocol.Header `kiteq:"header" db:"header"`
	Body      interface{}      `kiteq:"body" db:"body"` //序列化后的消息
	//-----------------
	MsgType         uint8    `kiteq:"msg_type" db:"msg_type"`           //消息类型
	Topic           string   `kiteq:"topic" db:"topic"`                 //Topic
	MessageType     string   `kiteq:"messageType" db:"message_type"`    //MessageType
	PublishGroup    string   `kiteq:"publish_group" db:"publish_group"` //发布的groupId
	Commit          bool     `kiteq:"commit" db:"commit"`               //是否已提交
	PublishTime     int64    `kiteq:"publish_time" db:"publish_time"`
	ExpiredTime     int64    `kiteq:"expiredTime" db:"expired_time"`            //过期时间
	DeliverCount    int32    `kiteq:"deliver_count" db:"deliver_count"`         //投递次数
	DeliverLimit    int32    `kiteq:"deliver_limit" db:"deliver_limit"`         //投递次数上线
	KiteServer      string   `kiteq:"kite_server" db:"kite_server"`             // 当前的处理kiteqserver地址
	FailGroups      []string `kiteq:"failGroups,omitempty" db:"fail_groups"`    //投递失败的分组tags
	SuccGroups      []string `kiteq:"succGroups,omitempty" db:"succ_groups"`    //投递成功的分组tags
	NextDeliverTime int64    `kiteq:"next_deliver_time" db:"next_deliver_time"` //下一次投递的时间

}

// func (self *MessageEntity) String() string {
// 	return fmt.Sprintf("id:%s Topic:%s Commit:%t Body:%s", self.MessageId, self.Topic, self.Commit, self.Body)
// }

func (self *MessageEntity) GetBody() interface{} {
	return self.Body
}

//创建stringmessage
func NewMessageEntity(msg *protocol.QMessage) *MessageEntity {
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
		DeliverLimit: msg.GetHeader().GetDeliverLimit(),

		//消息种类
		MsgType: msg.GetMsgType()}
	return entity

}

//kitestore存储
type IKiteStore interface {
	Start()
	Stop()
	Monitor() string
	Length() map[string] /*topic*/ int //堆积消息的数量

	MoveExpired() //将过期的消息迁移走

	//recover数量
	RecoverNum() int

	//批量提交channel
	AsyncUpdate(entity *MessageEntity) bool
	AsyncDelete(topic, messageId string) bool
	AsyncCommit(topic, messageId string) bool

	Query(topic, messageId string) *MessageEntity
	Save(entity *MessageEntity) bool
	Commit(topic, messageId string) bool
	Rollback(topic, messageId string) bool
	Delete(topic, messageId string) bool
	Expired(topic, messageId string) bool

	//根据kiteServer名称查询需要重投的消息 返回值为 是否还有更多、和本次返回的数据结果
	PageQueryEntity(hashKey string, kiteServer string, nextDeliveryTime int64, startIdx, limit int) (bool, []*MessageEntity)
}
