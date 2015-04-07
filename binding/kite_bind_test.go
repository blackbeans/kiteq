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

	db := Bind_Direct("s-trade-a", "trade", "pay-200", 1000, true)
	mb, _ := MarshalBinds([]*Binding{db})
	t.Logf("TestBinding|Direct|%s\n", string(mb))

	rb := Bind_Regx("s-trade-a", "trade", "pay-\\d+", 1000, true)
	mb, _ = MarshalBinds([]*Binding{rb})
	t.Logf("TestBinding|Regx|%s\n", string(mb))

	if !rb.matches("trade", "pay-500") {
		t.Fail()
		t.Logf("TestBinding|Regx|FAIL|%s\n", string(mb))
	}

	fb := Bind_Fanout("s-trade-a", "trade", 1000, true)
	mb, _ = MarshalBinds([]*Binding{fb})
	t.Logf("TestBinding|Faout|%s\n", string(mb))

	if !fb.matches("trade", "pay-500") {
		t.Fail()
		t.Logf("TestBinding|Fanout|FAIL|%s\n", string(mb))
	}

}
