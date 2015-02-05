package binding

import (
	"testing"
	"time"
)

func filter(b *Binding) bool {

	return false
}

func TestSubscribeBindings(t *testing.T) {

	zkmanager := NewZKManager("localhost:2181")
	bindings := []*Binding{Bind_Direct("s-trade-001", "trade", "trade-succ-200", -1, true),
		Bind_Regx("s-trade-001", "feed", "feed-geo-*", -1, true)}

	err := zkmanager.PublishBindings("s-trade-001", bindings)
	if nil != err {
		t.Logf("SubscribeTopic|FAIL|%s|%s\n", err, "s-trade-001")
		return
	}

	exchanger := NewBindExchanger("localhost:2181")
	//推送一下当前服务器的可用的topics列表
	topics := []string{"trade", "feed"}
	succ := exchanger.PushQServer("localhost:13800", topics)
	if !succ {
		t.Fail()
	}

	tradeBind := exchanger.FindBinds("trade", "trade-succ-200", filter)
	if len(tradeBind) != 1 {
		t.Fail()
		return
	}

	t.Logf("trade trade-succ-200|%t\n", tradeBind)
	if !tradeBind[0].matches("trade", "trade-succ-200") {
		t.Fail()
		return
	}

	feedBindU := exchanger.FindBinds("feed", "feed-geo-update", filter)

	if len(feedBindU) != 1 {
		t.Fail()
		return
	}

	t.Logf("feed feed-geo-update|%t\n", feedBindU)

	if !feedBindU[0].matches("feed", "feed-geo-update") {
		t.Fail()
		return
	}

	feedBindD := exchanger.FindBinds("feed", "feed-geo-delete", filter)
	if len(feedBindD) != 1 {
		t.Fail()
		return
	}

	t.Logf("feed feed-geo-delete|%t\n", feedBindD)

	if !feedBindD[0].matches("feed", "feed-geo-delete") {
		t.Fail()
		return
	}

	//"s-trade-001", "trade"
	//删除掉topic+groupId
	path := KITEQ_SUB + "/trade/s-trade-001-bind"
	zkmanager.session.Delete(path, -1)

	time.Sleep(5 * time.Second)

	tradeBind = exchanger.FindBinds("trade", "trade-succ-200", filter)
	if len(tradeBind) != 0 {
		t.Fail()
		return
	}

	t.Logf("trade trade-succ-200|no binding |%t\n", tradeBind)

	zkmanager.Close()
	exchanger.Shutdown()
}
