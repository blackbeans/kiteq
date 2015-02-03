package protocol

import (
	"fmt"
	"testing"
)

func TestA(t *testing.T) {
	a := []byte{0, 0, 0, 5, 16, 0, 0, 0, 13, 10}
	fmt.Println(a)
	fmt.Println(UnmarshalTLV(a))
}
