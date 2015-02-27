package binding

import (
	"log"
	"strings"
	"testing"
	"time"
)

func filter(b *Binding) bool {

	return false
}

type MockWatcher struct {
}

func (self *MockWatcher) DataChange(path string, binds []*Binding) {

	//订阅关系变更才处理
	if strings.HasPrefix(path, KITEQ_SUB) {

		split := strings.Split(path, "/")
		//如果不是bind级别的变更则忽略
		if len(split) < 5 || strings.LastIndex(split[4], "-bind") <= 0 {
			return
		}

		//开始处理变化的订阅关系
		log.Printf("MockWatcher|DataChange|SUB节点变更|%s|%s\n", path, binds)
	} else {
		log.Printf("MockWatcher|DataChange|非SUB节点变更|%s\n", path)
	}

}

//订阅关系topic下的group发生变更
func (self *MockWatcher) NodeChange(path string, eventType ZkEvent, childNode []string) {

	//如果是订阅关系变更则处理
	if strings.HasPrefix(path, KITEQ_SUB) {
		//获取topic
		split := strings.Split(path, "/")
		if len(split) < 4 {
			//不合法的订阅璐姐
			log.Printf("MockWatcher|NodeChange|INVALID SUB PATH |%s|%t\n", path, childNode)
			return
		}
		log.Printf("MockWatcher|NodeChange|SUB节点变更|%s|%s\n", path, childNode)
	} else {
		log.Printf("MockWatcher|NodeChange|非SUB节点变更|%s|%s\n", path, childNode)
	}
}

func TestSubscribeBindings(t *testing.T) {

	watcher := &MockWatcher{}
	zkmanager := NewZKManager("localhost:2181", watcher)
	cleanUp(t, zkmanager, "/kiteq")

	bindings := []*Binding{Bind_Direct("s-trade-001", "trade", "trade-succ-200", -1, true),
		Bind_Regx("s-trade-001", "feed", "feed-geo-*", -1, true)}

	err := zkmanager.PublishBindings("s-trade-001", bindings)
	if nil != err {
		t.Logf("SubscribeTopic|FAIL|%s|%s\n", err, "s-trade-001")
		return
	}
	zkmanager.Close()

	exchanger := NewBindExchanger("localhost:2181", "127.0.0.1:13800")
	//推送一下当前服务器的可用的topics列表
	topics := []string{"trade", "feed"}
	succ := exchanger.PushQServer("localhost:13800", topics)
	if !succ {
		t.Fail()
		t.Logf("PushQServer|FAIL|%s\n", succ)
		return
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

	cleanUp(t, zkmanager, "/kiteq")
	zkmanager.Close()
	exchanger.Shutdown()
}
