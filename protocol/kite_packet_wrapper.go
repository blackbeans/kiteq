package protocol

import (
	"encoding/json"
	"github.com/golang/protobuf/proto"
	"log"
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

func BuildMessage(header *Header, msgType uint8, body interface{}) []byte {
	switch msgType {
	case CMD_BYTES_MESSAGE:
		message := &BytesMessage{}
		message.Header = header
		message.Body = body.([]byte)
		data, err := proto.Marshal(message)
		if nil != err {
			log.Printf("MarshalMessage|%s|%d|%s\n", header, msgType, err)
		}
		return data
	case CMD_STRING_MESSAGE:
		message := &StringMessage{}
		message.Header = header
		message.Body = proto.String(body.(string))
		data, err := proto.Marshal(message)
		if nil != err {
			log.Printf("MarshalMessage|%s|%d|%s\n", header, msgType, err)
		}
		return data
	}
	return nil
}

type MarshalHelper interface {
	UnmarshalMessage(data []byte, msg proto.Message) error
	MarshalMessage(message proto.Message) ([]byte, error)
	MarshalConnMeta(groupId, secretKey string) []byte
	MarshalConnAuthAck(succ bool, feedback string) []byte
	MarshalMessageStoreAck(messageId string, succ bool, feedback string) []byte
	MarshalTxACKPacket(header *Header, txstatus TxStatus, feedback string) []byte
	MarshalHeartbeatPacket(version int64) []byte
	MarshalDeliverAckPacket(header *Header, status bool) []byte
}

type JsonMarshalHelper struct {
}

type PbMarshalHelper struct {
}

var JsonMarshaler = &JsonMarshalHelper{}
var PbMarshaler = &PbMarshalHelper{}

func (self *PbMarshalHelper) UnmarshalMessage(data []byte, msg proto.Message) error {
	return proto.Unmarshal(data, msg)
}

func (self *JsonMarshalHelper) UnmarshalMessage(data []byte, msg proto.Message) error {
	return json.Unmarshal(data, msg)
}

func (self *PbMarshalHelper) MarshalMessage(message proto.Message) ([]byte, error) {
	return proto.Marshal(message)
}

func (self *JsonMarshalHelper) MarshalMessage(message proto.Message) ([]byte, error) {
	return json.Marshal(message)
}

func (self *PbMarshalHelper) MarshalConnMeta(groupId, secretKey string) []byte {
	data, _ := self.MarshalMessage(&ConnMeta{
		GroupId:   proto.String(groupId),
		SecretKey: proto.String(secretKey)})
	return data
}

func (self *JsonMarshalHelper) MarshalConnMeta(groupId, secretKey string) []byte {
	data, _ := self.MarshalMessage(&ConnMeta{
		GroupId:   proto.String(groupId),
		SecretKey: proto.String(secretKey)})
	return data
}

func (self *PbMarshalHelper) MarshalConnAuthAck(succ bool, feedback string) []byte {
	data, _ := self.MarshalMessage(&ConnAuthAck{
		Status:   proto.Bool(succ),
		Feedback: proto.String(feedback)})
	return data

}

func (self *JsonMarshalHelper) MarshalConnAuthAck(succ bool, feedback string) []byte {
	data, _ := self.MarshalMessage(&ConnAuthAck{
		Status:   proto.Bool(succ),
		Feedback: proto.String(feedback)})
	return data

}

func (self *PbMarshalHelper) MarshalMessageStoreAck(messageId string, succ bool, feedback string) []byte {
	data, _ := self.MarshalMessage(&MessageStoreAck{
		MessageId: proto.String(messageId),
		Status:    proto.Bool(succ),
		Feedback:  proto.String(feedback)})
	return data
}

func (self *JsonMarshalHelper) MarshalMessageStoreAck(messageId string, succ bool, feedback string) []byte {
	data, _ := self.MarshalMessage(&MessageStoreAck{
		MessageId: proto.String(messageId),
		Status:    proto.Bool(succ),
		Feedback:  proto.String(feedback)})
	return data
}

func (self *PbMarshalHelper) MarshalTxACKPacket(header *Header, txstatus TxStatus, feedback string) []byte {
	data, _ := self.MarshalMessage(&TxACKPacket{
		Header:   header,
		Status:   proto.Int32(int32(txstatus)),
		Feedback: proto.String(feedback)})
	return data
}

func (self *JsonMarshalHelper) MarshalTxACKPacket(header *Header, txstatus TxStatus, feedback string) []byte {
	data, _ := self.MarshalMessage(&TxACKPacket{
		Header:   header,
		Status:   proto.Int32(int32(txstatus)),
		Feedback: proto.String(feedback)})
	return data
}

func (self *PbMarshalHelper) MarshalHeartbeatPacket(version int64) []byte {
	data, _ := self.MarshalMessage(&HeartBeat{
		Version: proto.Int64(version)})
	return data
}

func (self *JsonMarshalHelper) MarshalHeartbeatPacket(version int64) []byte {
	data, _ := self.MarshalMessage(&HeartBeat{
		Version: proto.Int64(version)})
	return data
}

func (self *PbMarshalHelper) MarshalDeliverAckPacket(header *Header, status bool) []byte {
	data, _ := self.MarshalMessage(&DeliverAck{
		MessageId:   proto.String(header.GetMessageId()),
		Topic:       proto.String(header.GetTopic()),
		MessageType: proto.String(header.GetMessageType()),
		GroupId:     proto.String(header.GetGroupId()),
		Status:      proto.Bool(status)})
	return data
}

func (self *JsonMarshalHelper) MarshalDeliverAckPacket(header *Header, status bool) []byte {
	data, _ := self.MarshalMessage(&DeliverAck{
		MessageId:   proto.String(header.GetMessageId()),
		Topic:       proto.String(header.GetTopic()),
		MessageType: proto.String(header.GetMessageType()),
		GroupId:     proto.String(header.GetGroupId()),
		Status:      proto.Bool(status)})
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
