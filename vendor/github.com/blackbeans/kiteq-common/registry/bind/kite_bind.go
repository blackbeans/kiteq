package bind

import (
	"encoding/json"
	"regexp"
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
	GroupId     string   `json:"groupId"`     //订阅的分组名称
	Topic       string   `json:"topic"`       //订阅的topic
	MessageType string   `json:"messageType"` // 消息的子分类
	BindType    BindType `json:"bindType"`    //bingd类型
	Version     string   `json:"version"`
	Watermark   int32    `json:"watermark"`  //本分组订阅的流量
	Persistent  bool     `json:"persistent"` //是否为持久订阅 即在客户端不在线的时候也需要推送消息
}

//只要两个的groupId和topic相同就认为是重复了，
//因为不应该存在一个groupId对应多个messagetype的情况
func (self *Binding) conflict(bind *Binding) bool {
	if self.GroupId == bind.GroupId &&
		self.Topic == bind.Topic {
		//如果两个都是正则类型的、或者其中一个是fanout的则直接返回冲突，
		if (bind.BindType == self.BindType &&
			bind.BindType == BIND_REGX) ||
			(bind.BindType == BIND_FANOUT || self.BindType == BIND_FANOUT) {
			return true
		} else {
			//self 或者bind 其中有一个是正则，则匹配是否有交叉
			if bind.BindType == BIND_REGX {
				conflict, err := regexp.MatchString(bind.MessageType, self.MessageType)
				if nil != err {
					return true
				}
				return conflict
			} else {
				conflict, err := regexp.MatchString(self.MessageType, bind.MessageType)
				if nil != err {
					return true
				}
				return conflict
			}
		}
	}
	return false
}

//是否与当前bind匹配
func (self *Binding) Matches(topic string, messageType string) bool {
	if self.BindType == BIND_FANOUT {
		return true
	} else if self.BindType == BIND_REGX {
		//正则匹配
		matcher, err := regexp.MatchString(self.MessageType, messageType)
		if nil != err || !matcher {
			return false
		}
		return matcher
	} else {
		//直接订阅的直接返回topic+messageType相同
		return self.Topic == topic && self.MessageType == messageType
	}
}

func UmarshalBinds(bind []byte) ([]*Binding, error) {
	var b []*Binding
	err := json.Unmarshal(bind, &b)
	return b, err
}

func MarshalBinds(bind []*Binding) ([]byte, error) {
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

func Bind_Fanout(groupId, topic string, watermark int32, persistent bool) *Binding {
	return binding(groupId, topic, "*", BIND_FANOUT, watermark, persistent)
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
