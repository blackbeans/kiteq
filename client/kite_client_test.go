package client

import (
	"testing"
)

func TestNewClient(t *testing.T) {
	client := NewKitClient(":13802", ":13800", "go-kite-test", "123456")
	client.SendMessage(buildStringMessage())
}
