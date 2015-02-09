package protocol

import (
	"github.com/golang/protobuf/proto"
	"log"
)

func UnmarshalPbMessage(data []byte, msg proto.Message) error {
	return proto.Unmarshal(data, msg)
}

func MarshalPbMessage(message proto.Message) ([]byte, error) {
	return proto.Marshal(message)
}

func MarshalMessage(header *Header, msgType uint8, body []byte) []byte {
	switch msgType {
	case CMD_BYTES_MESSAGE:
		message := &BytesMessage{}
		message.Header = header
		message.Body = body
		data, err := proto.Marshal(message)
		if nil != err {
			log.Printf("MarshalMessage|%s|%d|%s\n", header, msgType, err)
		}
		return data
	case CMD_STRING_MESSAGE:
		message := &StringMessage{}
		message.Header = header
		message.Body = proto.String(string(body))
		data, err := proto.Marshal(message)
		if nil != err {
			log.Printf("MarshalMessage|%s|%d|%s\n", header, msgType, err)
		}
		return data
	}
	return nil
}

func MarshalConnMeta(groupId, secretKey string) []byte {

	data, _ := MarshalPbMessage(&ConnMeta{
		GroupId:   proto.String(groupId),
		SecretKey: proto.String(secretKey)})
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
		MessageId:   proto.String(header.GetMessageId()),
		Topic:       proto.String(header.GetTopic()),
		MessageType: proto.String(header.GetMessageType()),
		Status:      proto.Int32(int32(txstatus)),
		Feedback:    proto.String(feedback)})
	return data
}

func MarshalHeartbeatPacket(version int64) []byte {
	data, _ := MarshalPbMessage(&HeartBeat{
		Version: proto.Int64(version)})
	return data
}

func MarshalDeliverAckPacket(header *Header, status bool) []byte {
	data, _ := MarshalPbMessage(&DeliverAck{
		MessageId:   proto.String(header.GetMessageId()),
		Topic:       proto.String(header.GetTopic()),
		MessageType: proto.String(header.GetMessageType()),
		GroupId:     proto.String(header.GetGroupId()),
		Status:      proto.Bool(status)})
	return data
}

//事务处理类型
type TxResponse struct {
	messageId string
	status    TxStatus //事务状态 0; //未知  1;  //已提交 2; //回滚
	feedback  string   //回馈
}

func NewTxResponse(messageId string) *TxResponse {
	return &TxResponse{
		messageId: messageId,
	}
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
