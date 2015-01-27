package binding

import (
	"encoding/json"
)

type BindType uint8

var BIND_VERSION = "1.0.0"

const (
	BIND_DIRECT = BindType(0) // 直接订阅
	BIND_REGX   = BindType(1) //正则订阅
	BIND_FANOUT = BindType(2) //广播式订阅
)

//用于定义订阅关系的结构

type Binding struct {
	GroupId     string   `json:"groupId" 		toml:"groupId"`       //订阅的分组名称
	Topic       string   `json:"topic" 			toml:"topic"`          //订阅的topic
	MessageType string   `json:"messageType"	toml:"messageType"` // 消息的子分类
	BindType    BindType `json:"bindType"		toml:"bindType"`      //bingd类型
	Version     string   `json:"version"		toml:"version"`
	Watermark   int32    `json:"watermark"		toml:"watermark"`   //本分组订阅的流量
	Persistent  bool     `json:"persistent"		toml:"persistent"` //是否为持久订阅 即在客户端不在线的时候也需要推送消息
}

func UmarshalBind(bind []byte) (*Binding, error) {
	var b *Binding
	err := json.Unmarshal(bind, &b)
	return b, err
}

func MarshalBind(bind *Binding) ([]byte, error) {
	data, err := json.Marshal(bind)
	return data, err
}

//直接订阅
func Bind_Direct(groupId, topic, messageType string, watermark int32, persistent bool) *Binding {
	return binding(groupId, topic, messageType, BIND_DIRECT, watermark, persistent)
}

//正则订阅
func Bind_Regx(groupId, topic, messageType string, watermark int32, persistent bool) *Binding {
	return binding(groupId, topic, messageType, BIND_REGX, watermark, persistent)
}

//订阅
func binding(groupId, topic, messageType string, bindType BindType, watermark int32, persistent bool) *Binding {
	return &Binding{
		GroupId:     groupId,
		Topic:       topic,
		MessageType: messageType,
		BindType:    bindType,
		Watermark:   watermark,
		Version:     BIND_VERSION,
		Persistent:  persistent}
}
