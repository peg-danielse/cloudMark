package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// saveRedis polls DBSIZE until it settles, then SAVEs.
func TestSaveRedis(t *testing.T) {
	defer func(d time.Duration) { redisPoll = d }(redisPoll)
	redisPoll = 5 * time.Millisecond
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	saved := make(chan bool, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		rd := bufio.NewReader(c)
		size := 0
		for {
			line, err := rd.ReadString('\n') // *1
			if err != nil {
				return
			}
			if !strings.HasPrefix(line, "*") {
				continue
			}
			rd.ReadString('\n') // $N
			name, _ := rd.ReadString('\n')
			switch strings.TrimSpace(name) {
			case "DBSIZE":
				if size < 30 {
					size += 10 // still filling for a few polls
				}
				fmt.Fprintf(c, ":%d\r\n", size)
			case "SAVE":
				saved <- size == 30
				fmt.Fprint(c, "+OK\r\n")
			}
		}
	}()
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	raw, _ := json.Marshal(map[string]any{"addr": host, "port": atoi(port)})
	if err := saveRedis(serviceConfig{"home-timeline-redis": raw}); err != nil {
		t.Fatal(err)
	}
	if !<-saved {
		t.Fatal("SAVE before the key count settled")
	}
}

func atoi(s string) int {
	n := 0
	fmt.Sscan(s, &n)
	return n
}
