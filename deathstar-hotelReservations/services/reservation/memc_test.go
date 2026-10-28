package reservation

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

func TestMemcGetMultiRetriesClosedConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	var accepts atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
				buf := make([]byte, 4096)
				_, _ = conn.Read(buf)
				if accepts.Add(1) == 1 {
					return
				}
				_, _ = conn.Write([]byte("END\r\n"))
			}(conn)
		}
	}()

	client := memcache.New(ln.Addr().String())
	client.Timeout = time.Second
	items, err := memcGetMulti(client, []string{"1_cap", "1_cap", "2_cap"})
	if err != nil {
		t.Fatalf("GetMulti after a closed connection: %v", err)
	}
	if items == nil {
		t.Fatal("nil item map")
	}
	if got := accepts.Load(); got < 2 {
		t.Fatalf("accepts = %d, want a retry dial", got)
	}
}

func TestMemcConnClosed(t *testing.T) {
	if !memcConnClosed(io.EOF) {
		t.Fatal("EOF should be retried")
	}
	if memcConnClosed(io.ErrNoProgress) {
		t.Fatal("unrelated errors should not be retried")
	}
}
