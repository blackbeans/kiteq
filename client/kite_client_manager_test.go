package client

import (
	// "kiteq/binding"
	"log"
	"testing"
)

func TestNew(t *testing.T) {
	// zk := binding.NewZKManager("")
	// zk.PublishQServer(":180001", []string{"topic1"})
	manager := NewKiteClientManager()
	log.Println(manager)
}
