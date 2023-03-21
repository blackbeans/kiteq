package exchange

import (
	"context"
	"github.com/blackbeans/go-zookeeper/zk"
	"github.com/blackbeans/kiteq-common/registry"

	"log"
	"testing"
	"time"
)

func filter(b *registry.Binding) bool {

	return false
}

func TestSubscribeBindings(t *testing.T) {
	conn, _, _ := zk.Connect([]string{"localhost:2181"}, 10*time.Second)
	cleanUp(t, conn, "/kiteq")

	exchanger := NewBindExchanger(context.TODO(), "zk://localhost:2181", "127.0.0.1:13800")
	//推送一下当前服务器的可用的topics列表
	topics := []string{"trade", "feed"}
	succ := exchanger.PushQServer("localhost:13800", topics)
	if !succ {
		t.Fail()
		t.Logf("PushQServer|FAIL|%s", succ)
		return
	}

	t.Log("TestSubscribeBindings|PushQServer|SUCC|.....")

	bindings := []*registry.Binding{registry.Bind_Direct("s-trade-001", "trade", "trade-succ-200", -1, true),
		registry.Bind_Regx("s-trade-001", "feed", "feed-geo-*", -1, true)}

	err := exchanger.registryCenter.PublishBindings("s-trade-001", bindings)
	if nil != err {
		t.Logf("TestSubscribeBindings|FAIL|%s|%s", err, "s-trade-001")
		return
	}

	time.Sleep(10 * time.Second)

	tradeBind, _ := exchanger.FindBinds("trade", "trade-succ-200", filter)
	t.Logf("trade trade-succ-200|%t", tradeBind)
	if len(tradeBind) != 1 {
		t.Fail()
		return
	}

	if !tradeBind[0].Matches("trade", "trade-succ-200") {
		t.Fail()
		return
	}

	feedBindU, _ := exchanger.FindBinds("feed", "feed-geo-update", filter)

	if len(feedBindU) != 1 {
		t.Fail()
		return
	}

	t.Logf("feed feed-geo-update|%t", feedBindU)

	if !feedBindU[0].Matches("feed", "feed-geo-update") {
		t.Fail()
		return
	}

	feedBindD, _ := exchanger.FindBinds("feed", "feed-geo-delete", filter)
	if len(feedBindD) != 1 {
		t.Fail()
		return
	}

	t.Logf("feed feed-geo-delete|%t", feedBindD)

	if !feedBindD[0].Matches("feed", "feed-geo-delete") {
		t.Fail()
		return
	}

	log.Println("start delete rade/s-trade-001-bind ....")

	time.Sleep(5 * time.Second)
	//"s-trade-001", "trade"
	//删除掉topic+groupId
	path := registry.KITEQ_SUB + "/trade/s-trade-001-bind"
	conn.Delete(path, -1)
	nodes, _, _ := conn.Children(registry.KITEQ_SUB + "/trade")
	t.Logf("trade trade-succ-200|delete|s-trade-001-bind|%t", nodes)
	time.Sleep(5 * time.Second)

	tradeBind, _ = exchanger.FindBinds("trade", "trade-succ-200", filter)
	t.Logf("trade trade-succ-200|no binding |%t", tradeBind)
	if len(tradeBind) != 0 {
		t.Fail()
		return
	}
	cleanUp(t, conn, "/kiteq")
	conn.Close()
	exchanger.Shutdown()
}

func cleanUp(t *testing.T, conn *zk.Conn, path string) {

	children, _, _ := conn.Children(path)

	//循环遍历当前孩子节点并删除
	for _, v := range children {
		tchildren, _, _ := conn.Children(path + "/" + v)
		if len(tchildren) <= 0 {
			//开始删除
			conn.Delete(path+"/"+v, -1)
			time.Sleep(2 * time.Second)
			t.Logf("cleanUp|%s", path+"/"+v)
		} else {
			cleanUp(t, conn, path+"/"+v)
		}
	}

	//删除当前节点
	conn.Delete(path, -1)
}
