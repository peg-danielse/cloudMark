package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/boot"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCPUSpec(t *testing.T) {
	if got := cpuSpec(1); got != "1" {
		t.Fatalf("cpuSpec(1) = %q", got)
	}
	if got := cpuSpec(4); got != "1-4" {
		t.Fatalf("cpuSpec(4) = %q", got)
	}
	if got := cpuSpec(32); got != "1-32" {
		t.Fatalf("cpuSpec(32) = %q", got)
	}
}

func TestPerSecond(t *testing.T) {
	if got := perSecond(1000, time.Second); got != 1000 {
		t.Fatalf("perSecond = %d", got)
	}
	if got := perSecond(500, 2*time.Second); got != 250 {
		t.Fatalf("perSecond = %d", got)
	}
	if got := perSecond(0, time.Second); got != 0 {
		t.Fatalf("perSecond = %d", got)
	}
}

func TestPercentile(t *testing.T) {
	samples := make([]uint64, 100)
	for i := range samples {
		samples[i] = uint64(i + 1)
	}
	if got := percentile(samples, 0.95); got != 95 {
		t.Fatalf("p95 = %d", got)
	}
	if got := percentile(nil, 0.95); got != 0 {
		t.Fatalf("empty p95 = %d", got)
	}
}

func TestReadWindow(t *testing.T) {
	rec := newRecorder()
	before := rec.snapshot()
	for i, us := range []uint64{1000, 2000, 3000, 4000} {
		sh := rec.shard(i)
		sh.sent.Add(1)
		sh.add(us * uint64(time.Microsecond))
	}
	rec.shard(9).sent.Add(96)
	rec.shard(9).errs.Add(2)
	for i := 0; i < 92; i++ {
		rec.shard(i).done.Add(1) // counted but outside the histogram: avg uses sum/done
	}
	got := readWindow(before, rec.snapshot(), time.Second)
	if got.arrivalRate != 100 || got.serviceRate != 96 || got.errors != 2 {
		t.Fatalf("rates %+v", got)
	}
	if math.Abs(got.avgUs-10000.0/96) > 0.1 {
		t.Fatalf("avg_us %v", got.avgUs)
	}
	// 4000us sits in a bucket at most 12.5% wide.
	if got.p95Us < 4000 || got.p95Us > 4000*1.125 {
		t.Fatalf("p95_us %v", got.p95Us)
	}
}

func TestParseRoutes(t *testing.T) {
	defer func(e string) { loadEntry = e }(loadEntry)
	loadEntry = entryServices
	all, err := parseRoutes("")
	if err != nil || len(all) != len(hotelServiceNames) {
		t.Fatalf("empty list: %v %v", all, err)
	}
	got, err := parseRoutes("search, geo")
	if err != nil || strings.Join(got, ",") != "geo,search" {
		t.Fatalf("search,geo: %v %v", got, err)
	}
	if _, err := parseRoutes("geo,nope"); err == nil {
		t.Fatal("unknown route accepted")
	}
}

func TestParseRoutesFrontend(t *testing.T) {
	defer func(e string) { loadEntry = e }(loadEntry)
	loadEntry = entryFrontend
	all, err := parseRoutes("")
	if err != nil || len(all) != len(frontendRoutes) {
		t.Fatalf("frontend default: %v %v", all, err)
	}
	if _, err := parseRoutes("geo"); err == nil {
		t.Fatal("a service route was accepted with -entry frontend")
	}
	got, err := parseRoutes("reservation,hotels")
	if err != nil || strings.Join(got, ",") != "hotels,reservation" {
		t.Fatalf("hotels,reservation: %v %v", got, err)
	}
}

// TestFrontendRoute calls every frontend route against a fake frontend: valid parameters reach it, and a 200 with
// a failed login counts as an error.
func TestFrontendRoute(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]url.Values{}
	fail := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.Path] = r.URL.Query()
		f := fail
		mu.Unlock()
		if f {
			fmt.Fprint(w, `"Failed. Please check your username and password. "`)
			return
		}
		fmt.Fprint(w, `{"message":"Logged-in successfully!"}`)
	}))
	defer srv.Close()
	cfg := boot.Config{Services: map[string]string{"frontend": strings.TrimPrefix(srv.URL, "http://")}}
	need := map[string][]string{
		"hotels": {"inDate", "outDate", "lat", "lon"}, "recommendations": {"require", "lat", "lon"},
		"user": {"username", "password"}, "review": {"username", "password", "hotelId"},
		"restaurants": {"hotelId"}, "museums": {"hotelId"}, "cinema": {"hotelId"},
		"reservation": {"inDate", "outDate", "hotelId", "customerName", "username", "password", "number"},
	}
	for _, route := range frontendRoutes {
		l, err := openFrontendRoute(cfg, route, 4)
		if err != nil {
			t.Fatal(err)
		}
		if err := l.call(context.Background(), 0); err != nil {
			t.Fatalf("%s: %v", route, err)
		}
		q := seen["/"+route]
		for _, k := range need[route] {
			if q.Get(k) == "" {
				t.Fatalf("%s: missing %s in %v", route, k, q)
			}
		}
		if u := q.Get("username"); u != "" {
			id, _ := hex.DecodeString(strings.TrimPrefix(u, "Cornell_"))
			if q.Get("password") != strings.Repeat(string(id), 10) {
				t.Fatalf("%s: password %q does not match user %q", route, q.Get("password"), u)
			}
		}
		l.close()
	}
	mu.Lock()
	fail = true
	mu.Unlock()
	l, _ := openFrontendRoute(cfg, "user", 4)
	if err := l.call(context.Background(), 0); err == nil {
		t.Fatal("a failed login counted as served")
	}
	if _, err := openFrontendRoute(cfg, "geo", 4); err == nil {
		t.Fatal("unknown frontend route accepted")
	}
}
