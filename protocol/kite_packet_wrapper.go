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

func MarshalTxACKPacket(messageId string, txstatus TxStatus) []byte {
	data, _ := MarshalPbMessage(&TxACKPacket{
		MessageId: proto.String(messageId),
		Status:    proto.Int32(int32(txstatus))})
	return data
}
