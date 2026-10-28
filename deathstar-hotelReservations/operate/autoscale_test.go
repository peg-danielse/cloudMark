package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDaemonHomes(t *testing.T) {
	homes, err := daemonHomes(15)
	if err != nil {
		t.Fatal(err)
	}
	if homes[0] != 1 || homes[14] != 15 {
		t.Fatalf("homes = %v", homes)
	}
	if _, err := daemonHomes(clientFirstCPU); err == nil {
		t.Fatal("homes ran into the loader cpus")
	}
}

func TestRampClients(t *testing.T) {
	clients := rampClients(1024)
	if clients[0] != 1 || clients[len(clients)-1] != 1 {
		t.Fatalf("ramp does not return to 1: %v", clients)
	}
	peak := 0
	at := -1
	for i, n := range clients {
		if n > peak {
			peak = n
			at = i
		}
		if n > 1024 {
			t.Fatalf("clients %d exceed 1024", n)
		}
	}
	if peak != 1024 || at != 10 {
		t.Fatalf("peak %d at %d, clients %v", peak, at, clients)
	}
	for i := 1; i <= at; i++ {
		if clients[i] != clients[i-1]*2 {
			t.Fatalf("ascent %v", clients)
		}
	}
	for i := at + 1; i < len(clients); i++ {
		if clients[i]*2 != clients[i-1] {
			t.Fatalf("descent %v", clients)
		}
	}

	odd := rampClients(1000)
	if odd[0] != 1 || odd[len(odd)-1] != 1 || odd[10] != 1000 {
		t.Fatalf("ramp to 1000: %v", odd)
	}
	if got := rampClients(1); len(got) != 1 || got[0] != 1 {
		t.Fatalf("ramp to 1: %v", got)
	}
}

func TestCPUSetList(t *testing.T) {
	lines := []tuple{
		{path: "/tmp/11211", id: "hotel-11211"},
		{path: "/tmp/mongod", id: "hotel-mongod"},
		{path: "/tmp/geo", id: "hotel-geo"},
	}
	got := cpuSetList(lines, func(id string) string {
		return strings.TrimPrefix(id, "hotel-")
	})
	if got != "11211=11211 mongod=mongod geo=geo" {
		t.Fatalf("cpusets %q", got)
	}
}

func TestHotelDeploymentHomes(t *testing.T) {
	lines, err := parseFile(root + "/mercury-hotel.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 15 {
		t.Fatalf("deployment lines = %d", len(lines))
	}
	homes, err := daemonHomes(len(lines))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"11211", "11212", "11213", "11214", "mongod",
		"geo", "profile", "rate", "recommendation", "user",
		"reservation", "search", "review", "attractions", "frontend",
	}
	for i, line := range lines {
		base := line.path[strings.LastIndex(line.path, "/")+1:]
		if base != want[i] {
			t.Fatalf("line %d = %s, want %s", i, base, want[i])
		}
		if homes[i] != i+1 {
			t.Fatalf("home %s = %d", base, homes[i])
		}
		if line.id != "hotel-"+base {
			t.Fatalf("id %s", line.id)
		}
	}
	for _, name := range hotelServiceNames {
		if _, ok := findLine(lines, name); !ok {
			t.Fatalf("missing route %s", name)
		}
	}
}

func TestDaemonEnv(t *testing.T) {
	lines, err := parseFile(root + "/mercury-hotel.txt")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range lines {
		env := daemonEnv(line, true)
		base := line.path[strings.LastIndex(line.path, "/")+1:]
		switch {
		case strings.Contains(line.path, "/memcached/"):
			if len(env) != 1 || env["MEMCACHED_THREADS"] != "16" {
				t.Fatalf("%s env %v", base, env)
			}
		case base == "mongod":
			if env["MONGO_WT_READ_TRANSACTIONS"] != "16" || env["MONGO_WT_WRITE_TRANSACTIONS"] != "16" || len(env) != 2 {
				t.Fatalf("mongod env %v", env)
			}
		default:
			if len(env) != 2 || env["GOMAXPROCS"] != "16" || env["HOTEL_COLD_CACHE"] != "1" {
				t.Fatalf("%s env %v", base, env)
			}
		}
	}
	for _, line := range lines {
		if strings.Contains(line.path, "/memcached/") || filepath.Base(line.path) == "mongod" {
			continue
		}
		env := daemonEnv(line, false)
		if env["HOTEL_COLD_CACHE"] != "0" || env["GOMAXPROCS"] != "16" {
			t.Fatalf("%s cold off env %v", filepath.Base(line.path), env)
		}
	}
}

func TestReadCPUSetMissing(t *testing.T) {
	if got := readCPUSet("missing-daemon-for-test"); got != "?" {
		t.Fatalf("missing cpuset %q", got)
	}
}

func TestIdleWorkersStayParked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rec := startLoad(ctx, func(context.Context, int) error { return nil }, 4096, 1)
	deadline := time.Now().Add(time.Second)
	for rec.snapshot().done == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if rec.snapshot().done == 0 {
		cancel()
		rec.wg.Wait()
		t.Fatal("the one active worker made no calls")
	}
	rec.setActive(32)
	time.Sleep(30 * time.Millisecond)
	if rec.snapshot().done < 32 {
		cancel()
		rec.wg.Wait()
		t.Fatalf("done %d after waking 32 workers", rec.snapshot().done)
	}
	cancel()
	finished := make(chan struct{})
	go func() {
		rec.wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("parked workers did not stop")
	}
}

func TestWriteHist(t *testing.T) {
	var buf bytes.Buffer
	histOut = &buf
	defer func() { histOut = nil }()
	var d snap
	d.sent, d.done = 4, 3
	d.hist[bucketOf(2_000_000)] = 3
	writeHist([]liveRoute{{name: "geo"}}, []snap{d}, 2, 1, 8, 100, 200)
	var row struct {
		Route   string     `json:"route"`
		Clients int        `json:"clients"`
		T0      int64      `json:"t0_ns"`
		T1      int64      `json:"t1_ns"`
		Done    int        `json:"done"`
		Hist    [][2]int64 `json:"hist"`
	}
	if err := json.Unmarshal(buf.Bytes(), &row); err != nil {
		t.Fatalf("%v: %s", err, buf.String())
	}
	if row.Route != "geo" || row.Clients != 8 || row.Done != 3 || row.T0 != 100 || row.T1 != 200 || len(row.Hist) != 1 || row.Hist[0][1] != 3 ||
		row.Hist[0][0] < 2_000_000 || row.Hist[0][0] > 2_300_000 {
		t.Fatalf("row %+v from %s", row, buf.String())
	}
}
