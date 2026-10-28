package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestWriteHist(t *testing.T) {
	var buf bytes.Buffer
	histOut = &buf
	defer func() { histOut = nil }()
	var d snap
	d.sent, d.done = 4, 3
	d.hist[bucketOf(2_000_000)] = 3
	writeHist([]liveRoute{{name: "compose", weight: 0.5}}, []snap{d}, 2, 1,
		func(r liveRoute) int { return routeUsers(16, r.weight) }, 100, 200)
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
	if row.Route != "compose" || row.Clients != 8 || row.T0 != 100 || row.T1 != 200 || row.Done != 3 ||
		len(row.Hist) != 1 || row.Hist[0][1] != 3 || row.Hist[0][0] < 2_000_000 || row.Hist[0][0] > 2_300_000 {
		t.Fatalf("row %+v from %s", row, buf.String())
	}
}
