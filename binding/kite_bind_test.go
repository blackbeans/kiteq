package binding

import (
	"testing"
)

func TestBinding(t *testing.T) {
	bind := Bind_Direct("s-trade-a", "trace", "pay-200", 1000, true)
	data, err := MarshalBinds([]*Binding{bind})
	if nil != err {
		t.Fail()
		return
	}

	t.Log(string(data))

	binds, err := UmarshalBinds(data)
	if nil != err {
		t.Fail()
		return
	}
	ubind := binds[0]
	if bind.BindType == ubind.BindType &&
		bind.GroupId == ubind.GroupId &&
		bind.Topic == ubind.Topic &&
		bind.MessageType == ubind.MessageType {

	} else {
		t.Fail()
	}

}
