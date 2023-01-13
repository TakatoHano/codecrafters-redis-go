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
	"sync"
	"time"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	var mu sync.Mutex

	m := Kvs{map[string]string{}, map[string]int64{}}

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
			mu.Lock()
			write(msg, c, m)
			mu.Unlock()

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

func write(msg string, conn net.Conn, m Kvs) {

	var num, len int
	var response, op, key, val string
	var px bool
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
				fmt.Println("OP:", op)
				switch op {
				case "SET":
					fmt.Println("num:", num)
					switch num {
					case 3:
						fmt.Println("Key:", txt)
						key = txt
					case 2:
						fmt.Println("Val:", txt)
						val = txt
					case 1:
						fmt.Println("Key:", txt)
						key = txt
					case 0:
						if px {
							fmt.Println("PX:", txt)
							ms, _ := strconv.ParseInt(txt, 10, 64)
							m.Set(key, val, ms)
						} else {
							fmt.Println("Val:", txt)
							m.Set(key, txt, -1)
						}
						m.List()
					default:
					}

				case "GET":
					value, ok := m.Get(txt)
					fmt.Println("Val:", value)
					if ok != nil {
						response = "$-1\r\n"
					} else {
						response = "+" + value + "\r\n"
					}

				default:
					response = "+" + txt + "\r\n"
				}
			case 1:
				if num == 0 {
					response = "+PONG\r\n"
				}
			case 2:
			case 3:
				op = "SET"
				response = "+OK\r\n"
			case 4:
				op = "GET"
			case 5:
				px = true
			default:
			}
		}
	}
	fmt.Println("End. write:", response)
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
	case "set", "SET":
		return msg, 3
	case "get", "GET":
		return msg, 4
	case "px", "PX":
		return msg, 5
	default:
		return msg, 0
	}
}

type Kvs struct {
	value     map[string]string
	expire_at map[string]int64
}

func (k Kvs) Set(key string, val string, ms int64) {
	fmt.Println("Set {Key:", key, " Value:", val, "px:", ms, "}")
	k.value[key] = val
	if ms < 0 {
		k.expire_at[key] = -1
	} else {
		k.expire_at[key] = time.Now().UnixMilli() + ms
	}
	fmt.Println("Set OK. value:", k.value, "expire_at:", k.expire_at)
}

func (k Kvs) Get(key string) (string, error) {
	val, ok := k.value[key]
	if !ok {
		return "", fmt.Errorf("not found")
	}
	ex := k.expire_at[key]
	if ex > 0 && ex < time.Now().UnixMilli() {
		delete(k.value, key)
		delete(k.expire_at, key)
		return "", fmt.Errorf("expired")

	}
	return val, nil
}

func (k Kvs) List() {
	fmt.Println(k.value)
}
