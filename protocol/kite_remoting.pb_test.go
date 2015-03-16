package protocol

import (
	"crypto/rand"
	"encoding/json"
	"github.com/golang/protobuf/proto"
	"io"
	"testing"
	"time"
)

var body []byte
var rander = rand.Reader // random function
func init() {
	body = make([]byte, 512, 512)
	// randomBits completely fills slice b with random data.
	if _, err := io.ReadFull(rander, body); err != nil {
		panic(err.Error()) // rand should never fail
	}
}

func buildStringMessage(id string) *StringMessage {
	//创建消息
	entity := &StringMessage{}
	entity.Header = &Header{
		MessageId:    proto.String(id),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Unix()),
		DeliverLimit: proto.Int32(-1),
		GroupId:      proto.String("go-kite-test"),
		Commit:       proto.Bool(true)}
	entity.Body = proto.String("hello go-kite")

	return entity
}

func buildBytesMessage(id string) *BytesMessage {
	//创建消息
	entity := &BytesMessage{}
	entity.Header = &Header{
		MessageId:    proto.String(id),
		Topic:        proto.String("trade"),
		MessageType:  proto.String("pay-succ"),
		ExpiredTime:  proto.Int64(time.Now().Unix()),
		DeliverLimit: proto.Int32(-1),
		GroupId:      proto.String("go-kite-test"),
		Commit:       proto.Bool(true)}
	entity.Body = body

	return entity
}

func BenchmarkPbStringMarshal(t *testing.B) {
	for i := 0; i < t.N; i++ {
		MarshalPbMessage(buildStringMessage("1234"))
	}
}

func BenchmarkPbStringUnmarshal(t *testing.B) {

	datas, _ := MarshalPbMessage(buildStringMessage("1234"))
	for i := 0; i < t.N; i++ {
		var msg StringMessage
		UnmarshalPbMessage(datas, &msg)
	}
}

func BenchmarkPbBytesMarshal(t *testing.B) {
	for i := 0; i < t.N; i++ {
		MarshalPbMessage(buildBytesMessage("1234"))
	}
}

func BenchmarkPbBytesUnmarshal(t *testing.B) {

	datab, _ := MarshalPbMessage(buildBytesMessage("1234"))
	for i := 0; i < t.N; i++ {
		var bm BytesMessage
		UnmarshalPbMessage(datab, &bm)
	}
}

func BenchmarkJsonStringMarshal(t *testing.B) {
	for i := 0; i < t.N; i++ {
		json.Marshal(buildStringMessage("1234"))
	}
}

func BenchmarkJsonStringUnmarshal(t *testing.B) {
	datas, _ := json.Marshal(buildStringMessage("1234"))
	for i := 0; i < t.N; i++ {
		var msg StringMessage
		json.Unmarshal(datas, &msg)
	}
}

func BenchmarkJsonBytesMarshal(t *testing.B) {
	for i := 0; i < t.N; i++ {
		json.Marshal(buildBytesMessage("1234"))
	}
}

func BenchmarkJsonBytesUnmarshal(t *testing.B) {
	datab, _ := json.Marshal(buildBytesMessage("1234"))
	for i := 0; i < t.N; i++ {
		var bm BytesMessage
		json.Unmarshal(datab, &bm)
	}
}
