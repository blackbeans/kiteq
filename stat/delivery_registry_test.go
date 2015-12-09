package stat

import (
	"kiteq/store"
	"testing"
	"time"
)

func BenchmarkDeliveryRegistry(t *testing.B) {
	t.StopTimer()
	registry := NewDeliveryRegistry(10 * 10000)

	t.SetParallelism(8)
	t.StartTimer()
	t.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msgId := store.MessageId()
			succ := registry.Registe(msgId, 5*time.Second)
			if !succ {
				t.Fail()
			}
		}
	})
}

func TestDeliveryRegistry(t *testing.T) {
	registry := NewDeliveryRegistry(10 * 10000)

	msgId := store.MessageId()
	succ := registry.Registe(msgId, 5*time.Second)
	if !succ {
		t.Fail()
		t.Logf("TestDeliveryRegistry|FirstRegist|FAIL|%s", msgId)
	}

	succ = registry.Registe(msgId, 5*time.Second)
	if succ {
		t.Fail()
		t.Logf("TestDeliveryRegistry|SecondRegist|FAIL|%s", msgId)
	}

	time.Sleep(5 * time.Second)
	succ = registry.Registe(msgId, 5*time.Second)
	if !succ {
		t.Fail()
		t.Logf("TestDeliveryRegistry|ThirdRegist|FAIL|%s", msgId)
	}
}
