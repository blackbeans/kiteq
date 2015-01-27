package binding

import (
	"fmt"
	"testing"
	"time"
)

type DefaultWatcher struct {
}

func (self *DefaultWatcher) EventNotify(path string, eventType ZkEvent) {
	fmt.Printf("DefaultWatcher|EventNotify|%s|%t\n", path, eventType)
}
func (self *DefaultWatcher) ChildWatcher(path string, childNode []string) {
	fmt.Printf("DefaultWatcher|ChildWatcher|%s|%t\n", path, childNode)
}

func TestPublishQServer(t *testing.T) {
	zkmanager := NewZKManager("localhost:2181")

	watcher := NewWatcher(&DefaultWatcher{})

	topics := []string{"trade", "feed", "comment"}
	err := zkmanager.PublishQServer("localhost:13800", topics)
	if nil != err {
		t.Fail()
		t.Log(err)
		return
	}

	for _, topic := range topics {
		servers, err := zkmanager.GetQServerAndWatch(topic, watcher)
		if nil != err {
			t.Fail()
			t.Logf("%s|%s", err, topic)
			return
		}
		if len(servers) != 1 {
			t.Fail()
			return
		}
		t.Logf("TestPublishQServer|GetQServerAndWatch|%s|%s\n", topic, servers)
	}

	//主动删除一下
	zkmanager.session.Delete("/kiteq/server/trade/localhost:13800", -1)
	time.Sleep(10 * time.Second)
	zkmanager.Close()

}

func TestPublishTopic(t *testing.T) {
	// topics := []string{"trade", "feed", "comment"}

}
