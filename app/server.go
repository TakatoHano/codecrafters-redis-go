package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	if err != nil {
		fmt.Println("Error Listening TCP: ", err.Error())
		os.Exit(1)
	}

	f := func(c net.Conn) {
		defer c.Close()
		for {
			msg := read(c)
			if msg == "" {
				return
			}
			fmt.Println("recieve msg:", msg)
			// TODO: parse request message
			c.Write([]byte("+PONG\r\n"))

		}
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go f(conn)
	}
}

func read(conn net.Conn) string {
	var msg bytes.Buffer
	reader := bufio.NewReader(conn)

	b, err := reader.ReadBytes('\n')
	if err == io.EOF {
		return msg.String()
	}
	if err != nil {
		fmt.Println("Error reading string: ", err.Error())
		os.Exit(1)
	}

	op, _ := strconv.Atoi(string(b[1]))
	msg.Write(b)
	for num := 0; num < op*2; num++ {
		c, err := reader.ReadBytes('\n')
		if err != nil {
			fmt.Println("Error reading string: ", err.Error())
			os.Exit(1)
		}
		msg.Write(c)
	}

	return msg.String()
}
