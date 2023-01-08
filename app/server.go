package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
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
			msg, error := read(c)
			if error == io.EOF || msg == "" {
				return
			}
			fmt.Println("recieve msg:", msg)
			// TODO: parse request message
			write(msg, c)

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

func read(conn net.Conn) (string, error) {
	var msg bytes.Buffer
	reader := bufio.NewReader(conn)

	b, err := reader.ReadBytes('\n')
	if err == io.EOF {
		return msg.String(), io.EOF
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

	return msg.String(), nil
}

func write(msg string, conn net.Conn) {

	var num, len int
	var response string
	scanner := bufio.NewScanner(strings.NewReader(msg))
	for scanner.Scan() {
		l := scanner.Text()
		l = strings.TrimRight(l, "\n")
		l = strings.TrimRight(l, "\r")
		fmt.Println("Line:", l)
		switch parse(l) {
		case 0:
			num, _ = strconv.Atoi(l[1:])
			fmt.Println("Num:", num)
		case 1:
			len, _ = strconv.Atoi(l[1:])
			fmt.Println("Len:", len)
		case 2:
			num--
			txt, typ := run(l[0:len])
			fmt.Println("Type:", typ)
			switch typ {
			case 0:
				response = "+" + txt + "\r\n"
			case 1:
				if num == 0 {
					response = "+PONG\r\n"
				}
			case 2:
			default:
			}
		}
	}
	conn.Write([]byte(response))
}

func parse(line string) int {

	switch line[0] {
	case '*':
		return 0
	case '$':
		return 1
	default:
		return 2
	}
}

func run(msg string) (string, int) {
	switch msg {
	case "ping", "PING":
		return "", 1
	case "echo", "ECHO":
		return msg, 2
	default:
		return msg, 0
	}
}
