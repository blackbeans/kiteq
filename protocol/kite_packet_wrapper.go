package protocol

import (
	"github.com/golang/protobuf/proto"
)

func UnmarshalPbMessage(data []byte, msg proto.Message) error {
	return proto.Unmarshal(data, msg)
}

func MarshalPbMessage(message proto.Message) ([]byte, error) {
	return proto.Marshal(message)
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

func MarshalTxACKPacket(messageId string, txstatus TxStatus, feedback string) []byte {
	data, _ := MarshalPbMessage(&TxACKPacket{
		MessageId: proto.String(messageId),
		Status:    proto.Int32(int32(txstatus)),
		Feedback:  proto.String(feedback)})
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
