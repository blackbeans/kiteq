package main

import (
	"bufio"
	"bytes"
	"fmt"
)

func main() {
	buff := []byte("helloworldabcd\n123\r\n456")
	fmt.Println(buff)
	reader := bytes.NewReader(buff)
	br := bufio.NewReader(reader)

	line, err := br.ReadBytes('\n')
	packet := bytes.TrimRight(line, "\r\n")

	fmt.Println(line)
	fmt.Println(string(packet))

	line, err = br.ReadBytes('\n')

	fmt.Printf("----%s\n", line)

	line, err = br.ReadBytes('\n')

	fmt.Printf("----%s:%s\n", line, err)

}
