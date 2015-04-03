package server

import (
	"github.com/blackbeans/turbo"
	"strings"
	"testing"
	"time"
)

func TestParse(t *testing.T) {

	rc := turbo.NewRemotingConfig(
		"remoting-localhost:13800",
		2000, 16*1024,
		16*1024, 10000, 10000,
		10*time.Second, 160000)

	kc := NewKiteQConfig("kiteq-localhost:138000", "localhost:138000",
		"localhost:2181", 1*time.Second, 8000, 5*time.Second,
		strings.Split("trade", ","),
		"mysql://localhost:3306,localhost:3306?db=kite&username=root", rc)

	store := parseDB(kc)
	store.Delete("123456")
}
