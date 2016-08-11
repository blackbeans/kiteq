package protocol

import (
	log "github.com/blackbeans/log4go"
	"github.com/golang/protobuf/proto"
)

type QMessage struct {
	message proto.Message
	msgType uint8
	header  *Header
	body    interface{}
}

func NewQMessage(msg interface{}) *QMessage {

	message, ok := msg.(proto.Message)
	if !ok {
		return nil
	}

	bm, bok := message.(*BytesMessage)
	if bok {
		return &QMessage{
			msgType: CMD_BYTES_MESSAGE,
			header:  bm.GetHeader(),
			body:    bm.GetBody(),
			message: message}
	} else {
		sm, mok := message.(*StringMessage)
		if mok {
			return &QMessage{
				msgType: CMD_STRING_MESSAGE,
				header:  sm.GetHeader(),
				body:    sm.GetBody(),
				message: message}
		}
	}
	return nil

}

func (self *QMessage) GetHeader() *Header {
	return self.header
}

func (self *QMessage) GetBody() interface{} {
	return self.body
}

func (self *QMessage) GetMsgType() uint8 {
	return self.msgType
}

func (self *QMessage) GetPbMessage() proto.Message {
	return self.message
}

func UnmarshalPbMessage(data []byte, msg proto.Message) error {
	return proto.Unmarshal(data, msg)
}

func MarshalPbString(s string) *string {
	return proto.String(s)
}

func MarshalInt32(i int32) *int32 {
	return proto.Int32(i)
}

func MarshalInt64(i int64) *int64 {
	return proto.Int64(i)
}

func MarshalPbMessage(message proto.Message) ([]byte, error) {
	return proto.Marshal(message)
}

func MarshalMessage(header *Header, msgType uint8, body interface{}) []byte {
	switch msgType {
	case CMD_BYTES_MESSAGE:
		message := &BytesMessage{}
		message.Header = header
		message.Body = body.([]byte)

		data, err := proto.Marshal(message)
		if nil != err {
			log.Error("MarshalMessage|%s|%d|%s\n", header, msgType, err)
		}
		return data
	case CMD_STRING_MESSAGE:
		message := &StringMessage{}
		message.Header = header
		message.Body = proto.String(body.(string))
		data, err := proto.Marshal(message)
		if nil != err {
			log.Error("MarshalMessage|%s|%d|%s\n", header, msgType, err)
		}
		return data
	}
	return nil
}

func MarshalConnMeta(groupId, secretKey string, warmingupSec int32) []byte {

	data, _ := MarshalPbMessage(&ConnMeta{
		GroupId:      proto.String(groupId),
		SecretKey:    proto.String(secretKey),
		WarmingupSec: proto.Int32(warmingupSec)})
	return data
}

func MarshalConnAuthAck(succ bool, feedback string) []byte {

	data, _ := MarshalPbMessage(&ConnAuthAck{
		Status:   proto.Bool(succ),
		Feedback: proto.String(feedback)})
	return data
}

func MarshalMessageStoreAck(messageId string, succ bool, feedback string) []byte {
	data, _ := MarshalPbMessage(&MessageStoreAck{
		MessageId: proto.String(messageId),
		Status:    proto.Bool(succ),
		Feedback:  proto.String(feedback)})
	return data
}

func MarshalTxACKPacket(header *Header, txstatus TxStatus, feedback string) []byte {
	data, _ := MarshalPbMessage(&TxACKPacket{
		Header:   header,
		Status:   proto.Int32(int32(txstatus)),
		Feedback: proto.String(feedback)})
	return data
}

func MarshalHeartbeatPacket(version int64) []byte {
	data, _ := MarshalPbMessage(&HeartBeat{
		Version: proto.Int64(version)})
	return data
}

func MarshalDeliverAckPacket(header *Header, status bool, err error) []byte {
	feedback := ""
	if nil != err {
		feedback = err.Error()
	}
	data, _ := MarshalPbMessage(&DeliverAck{
		MessageId:   proto.String(header.GetMessageId()),
		Topic:       proto.String(header.GetTopic()),
		MessageType: proto.String(header.GetMessageType()),
		GroupId:     proto.String(header.GetGroupId()),
		Status:      proto.Bool(status),
		Feedback:    proto.String(feedback)})
	return data
}

//事务处理类型
type TxResponse struct {
	MessageId   string
	Topic       string
	MessageType string
	properties  map[string]string
	status      TxStatus //事务状态 0; //未知  1;  //已提交 2; //回滚
	feedback    string   //回馈
}

func NewTxResponse(header *Header) *TxResponse {

	props := header.GetProperties()
	var properties map[string]string
	if nil != props && len(props) <= 0 {
		properties = make(map[string]string, len(props))
		for _, v := range props {
			properties[v.GetKey()] = v.GetValue()
		}
	}

	return &TxResponse{
		MessageId:   header.GetMessageId(),
		Topic:       header.GetTopic(),
		MessageType: header.GetMessageType(),
		properties:  properties}
}

//获取属性
func (self *TxResponse) GetProperty(key string) (string, bool) {
	k, v := self.properties[key]
	return k, v
}

func (self *TxResponse) Unknown(feedback string) {
	self.status = TX_UNKNOWN
	self.feedback = feedback
}

func (self *TxResponse) Rollback(feedback string) {
	self.status = TX_ROLLBACK
	self.feedback = feedback
}
func (self *TxResponse) Commit() {
	self.status = TX_COMMIT
}

func (self *TxResponse) ConvertTxAckPacket(packet *TxACKPacket) {
	packet.Status = proto.Int32(int32(self.status))
	packet.Feedback = proto.String(self.feedback)
}
