package server

import (
	"github.com/blackbeans/turbo"
	"os"
	"testing"
	"time"
)

func TestParse(t *testing.T) {

	rc := turbo.NewRemotingConfig(
		"remoting-localhost:13800",
		2000, 16*1024,
		16*1024, 10000, 10000,
		10*time.Second, 160000)
	so := MockServerOption()
	so.db = "mysql://localhost:3306,localhost:3306?db=kite&username=root"
	kc := NewKiteQConfig(MockServerOption(), rc)
	kiteqName, _ := os.Hostname()
	store := parseDB(kc, kiteqName)
	store.Delete("", "123456")
}
