package server

import (
	"testing"
)

func TestLoadToml(t *testing.T) {
	so := &ServerOption{}
	err := loadTomlConf("../conf/cluster.toml", "default", ":13000", 13001, so)
	if nil != err {
		t.Fail()

	}
	t.Log(so)

}
