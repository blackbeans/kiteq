package server

import (
	"testing"
)

func TestParse(t *testing.T) {
	//
	store := parseDB("mysql://localhost:3306,localhost:3306?db=kite&username=root")
	store.Delete("123456")
}
