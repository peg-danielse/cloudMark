package main

// Tests for jaegercp.go (shared verbatim by hotel operate and social goperate).
// testdata/jaeger-v3-hotel.json is a recorded-format Jaeger v3 /api/v3/traces
// response with hand-built traces whose critical paths are known exactly.

import (
	"encoding/csv"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const fixtureT0 = int64(1792395000000000000) // window start of the fixtures

// genericModel mirrors the hotel model so this file does not depend on the app.
func genericModel() traceModel {
	return traceModel{
		route: func(root *traceSpan) string {
			if root.service == "search" || root.service == "geo" {
				return root.service
			}
			return ""
		},
		service: func(s string) string { return s },
		backend: func(s *traceSpan) string {
			switch {
			case strings.HasPrefix(s.name, "memcached"):
				return "11212"
			case strings.HasPrefix(s.name, "mongo"):
				return "mongod"
			}
			return ""
		},
	}
}

func loadFixture(t *testing.T, name string) map[string][]*traceSpan {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	traces, err := parseV3(f)
	if err != nil {
		t.Fatal(err)
	}
	return traces
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func ms(ns int64) float64 { return float64(ns) / 1e6 }

func TestParseV3(t *testing.T) {
	traces := loadFixture(t, "jaeger-v3-hotel.json")
	if len(traces) != 5 {
		t.Fatalf("got %d traces, want 5", len(traces))
	}
	a := traces["0000000000000000000000000000000a"]
	if len(a) != 6 {
		t.Fatalf("trace a has %d spans", len(a))
	}
	for _, s := range a {
		if s.spanID == "0000000000000002" {
			if s.service != "search" || s.kind != 3 || s.parentID != "0000000000000001" || s.end-s.start != 3e6 {
				t.Fatalf("bad span %+v", *s)
			}
		}
	}
	empty, err := parseV3(strings.NewReader(`{"error":{"httpCode":404,"message":"No traces found"}}`))
	if err != nil || len(empty) != 0 {
		t.Fatalf("404 doc: %v %v", empty, err)
	}
	if _, err := parseV3(strings.NewReader(`{"error":{"httpCode":400,"message":"bad"}}`)); err == nil {
		t.Fatal("400 doc should fail")
	}
	k := rawInt([]byte(`"SPAN_KIND_CLIENT"`))
	if k != 3 {
		t.Fatalf("string kind %d", k)
	}
}

func TestCriticalPathSequential(t *testing.T) {
	traces := loadFixture(t, "jaeger-v3-hotel.json")
	r, ok := analyzeTrace(traces["0000000000000000000000000000000a"], genericModel())
	if !ok || r.route != "search" || r.dur != 10e6 {
		t.Fatalf("got %+v %v", r, ok)
	}
	want := map[string]float64{"search": 2.5, "geo": 3, "rate": 2.5, "11212": 2}
	var sum int64
	for d, v := range r.crit {
		sum += v
		if !near(ms(v), want[d]) {
			t.Errorf("%s crit %.3f ms, want %.3f", d, ms(v), want[d])
		}
	}
	if sum != r.dur {
		t.Errorf("crit sums to %d, want %d", sum, r.dur)
	}
	if !near(ms(r.rpc["geo"]), 1) || !near(ms(r.rpc["rate"]), 1) || r.rpc["search"] != 0 {
		t.Errorf("rpc %v", r.rpc)
	}
	if r.spans["search"] != 1 || r.spans["geo"] != 2 || r.spans["rate"] != 2 || r.spans["11212"] != 1 {
		t.Errorf("span counts %v", r.spans)
	}
}

func TestCriticalPathParallel(t *testing.T) {
	traces := loadFixture(t, "jaeger-v3-hotel.json")
	r, ok := analyzeTrace(traces["0000000000000000000000000000000b"], genericModel())
	if !ok {
		t.Fatal("not analysed")
	}
	// rate finishes last (108) so it is on the path; geo only covers 101-102,
	// before rate's client span starts.
	want := map[string]float64{"search": 3, "rate": 6, "geo": 1}
	for d, w := range want {
		if !near(ms(r.crit[d]), w) {
			t.Errorf("%s crit %.3f ms, want %.3f", d, ms(r.crit[d]), w)
		}
	}
	if !near(ms(r.rpc["geo"]), 0.5) || !near(ms(r.rpc["rate"]), 1) {
		t.Errorf("rpc %v", r.rpc)
	}
}

func TestCriticalPathNonRoute(t *testing.T) {
	traces := loadFixture(t, "jaeger-v3-hotel.json")
	if _, ok := analyzeTrace(traces["0000000000000000000000000000000e"], genericModel()); ok {
		t.Fatal("jaeger's own trace counted as a route")
	}
	// orphan child (parent missing) becomes the root; a cycle must not hang
	cyc := []*traceSpan{
		{spanID: "1", parentID: "2", service: "geo", start: 0, end: 10},
		{spanID: "2", parentID: "1", service: "geo", start: 1, end: 5},
	}
	if _, ok := analyzeTrace(cyc, genericModel()); ok {
		t.Fatal("cycle without root should not be analysed")
	}
}

func TestWindowRows(t *testing.T) {
	traces := loadFixture(t, "jaeger-v3-hotel.json")
	var rs []traceResult
	for _, sp := range traces {
		if r, ok := analyzeTrace(sp, genericModel()); ok && r.start >= fixtureT0 {
			rs = append(rs, r)
		}
	}
	rows := windowRows(rs)
	got := map[string]cpRow{}
	for _, r := range rows {
		got[r.route+"/"+r.daemon] = r
	}
	want := map[string][2]float64{ // share, rpc
		"search/search": {0.275, 0}, "search/geo": {0.2, 0.075}, "search/rate": {0.425, 0.1},
		"search/11212": {0.1, 0}, "geo/geo": {1, 0},
	}
	if len(got) != len(want) {
		t.Fatalf("rows %v", rows)
	}
	for k, w := range want {
		r, ok := got[k]
		if !ok || !near(r.share, w[0]) || !near(r.rpc, w[1]) {
			t.Errorf("%s: %+v, want share %.3f rpc %.3f", k, r, w[0], w[1])
		}
	}
	s := got["search/search"]
	if s.samples != 2 || !near(s.p50, 2.5) || !near(s.p95, 3) || !near(s.mean, 2.75) || !near(s.rootP95, 10) {
		t.Errorf("search/search stats %+v", s)
	}
	if rows[0].route != "geo" || rows[1].daemon != "rate" {
		t.Errorf("order: %v", rows)
	}
}

func TestTraceMarkRoundTrip(t *testing.T) {
	w := traceWindow{idx: 7, level: 1, wi: 3, clients: 64, start: time.UnixMicro(1792395000000000), end: time.UnixMicro(1792395001000123)}
	got, ok := parseTraceMark(strings.TrimSpace(formatTraceMark(w)))
	if !ok || got != w {
		t.Fatalf("got %+v %v", got, ok)
	}
	for _, bad := range []string{"route search clients 1", "window 1 level 0 wi 0 clients 1 start_us x end_us 2", "window 1"} {
		if _, ok := parseTraceMark(bad); ok {
			t.Errorf("parsed %q", bad)
		}
	}
	var b strings.Builder
	traceMarks = false
	var m markCounter
	m.emit(&b, 0, 0, 1, time.Now(), time.Now())
	if b.Len() != 0 {
		t.Fatal("marker printed with tracing off")
	}
	traceMarks = true
	defer func() { traceMarks = false }()
	m.emit(&b, 0, 0, 1, time.Now(), time.Now())
	m.emit(&b, 0, 1, 1, time.Now(), time.Now())
	if !strings.HasPrefix(b.String(), "window 0 level 0 wi 0 ") || !strings.Contains(b.String(), "window 1 level 0 wi 1 ") {
		t.Fatalf("markers %q", b.String())
	}
}

func TestLineSniffer(t *testing.T) {
	var out strings.Builder
	var lines []string
	s := &lineSniffer{w: lockedWriter{mu: &sync.Mutex{}, w: &out}, on: func(l string) { lines = append(lines, l) }}
	for _, chunk := range []string{"route a 1\nwin", "dow 0 level", " 0\n", "tail"} {
		s.Write([]byte(chunk))
	}
	if out.String() != "route a 1\nwindow 0 level 0\ntail" {
		t.Fatalf("passthrough %q", out.String())
	}
	if len(lines) != 2 || lines[1] != "window 0 level 0" {
		t.Fatalf("lines %q", lines)
	}
}

func TestTracerAgainstFakeJaeger(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("testdata", "jaeger-v3-hotel.json"))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var queries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		queries = append(queries, r.URL.RawQuery)
		mu.Unlock()
		if r.URL.Path != "/api/v3/traces" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("query.startTimeMin") == time.Unix(0, fixtureT0).UTC().Format(time.RFC3339Nano) {
			w.Header().Set("Content-Type", "application/json")
			w.Write(body)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":{"httpCode":404,"message":"No traces found"}}`))
	}))
	defer srv.Close()
	dir := t.TempDir()
	path := filepath.Join(dir, "traces.csv")
	var cp strings.Builder
	tr, err := newCPTracer(srv.URL, genericModel(), path, lockedWriter{mu: &sync.Mutex{}, w: &cp}, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	w0 := traceWindow{idx: 0, level: 0, wi: 0, clients: 4, start: time.Unix(0, fixtureT0), end: time.Unix(0, fixtureT0+int64(time.Second))}
	w1 := traceWindow{idx: 1, level: 0, wi: 1, clients: 4, start: time.Unix(0, fixtureT0-int64(time.Hour)), end: time.Unix(0, fixtureT0-int64(time.Hour-time.Second))}
	tr.onLine(strings.TrimSpace(formatTraceMark(w0)))
	tr.onLine("route search clients 4 ...")
	tr.onLine(strings.TrimSpace(formatTraceMark(w1)))
	summary := tr.close()
	if !strings.Contains(summary, "windows 2 samples 3 failed 0 truncated 1") {
		t.Errorf("summary %q", summary)
	}
	f, _ := os.Open(path)
	recs, err := csv.NewReader(f).ReadAll()
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(recs[0], ",") != strings.Join(traceCSVHeader, ",") {
		t.Fatalf("header %v", recs[0])
	}
	if len(recs) != 1+5 {
		t.Fatalf("got %d rows: %v", len(recs)-1, recs)
	}
	col := map[string]int{}
	for i, h := range recs[0] {
		col[h] = i
	}
	for _, r := range recs[1:] {
		if r[col["window"]] != "0" || r[col["clients"]] != "4" || r[col["fetched"]] != "5" || r[col["truncated"]] != "1" {
			t.Errorf("row %v", r)
		}
		if r[col["route"]] == "search" && r[col["daemon"]] == "rate" {
			if v, _ := strconv.ParseFloat(r[col["crit_share"]], 64); !near(v, 0.425) {
				t.Errorf("rate share %v", v)
			}
			if r[col["sample_count"]] != "2" || r[col["span_count"]] != "4" {
				t.Errorf("rate counts %v", r)
			}
		}
	}
	if !strings.Contains(cp.String(), "cp window 0 clients 4 route search samples 2 root_p95_ms 10.000 rate=0.425 search=0.275 geo=0.200 11212=0.100\n") {
		t.Errorf("cp lines %q", cp.String())
	}
	if len(queries) != 2 || !strings.Contains(queries[0], "query.searchDepth=3") {
		t.Errorf("queries %v", queries)
	}
}

func TestJaegerOpts(t *testing.T) {
	o := &jaegerOpts{on: true, mode: jaegerModeBare, cpus: "62", host: "127.0.0.1", offset: 10000,
		sample: 0.02, transport: "http", storage: "memory", maxTraces: 200000, depth: 5000}
	if err := o.validate(); err != nil {
		t.Fatal(err)
	}
	if o.queryBase() != "http://127.0.0.1:26686" || o.collectorHTTP() != "http://127.0.0.1:24268/api/traces" || o.agentUDP() != "127.0.0.1:16831" {
		t.Fatalf("%s %s %s", o.queryBase(), o.collectorHTTP(), o.agentUDP())
	}
	o.addr = "10.0.0.1:9999"
	if o.queryBase() != "http://10.0.0.1:9999" {
		t.Fatal(o.queryBase())
	}
	if got := o.csvPath("/x/run1/loadgen.out", "/out"); got != "/x/run1/traces.csv" {
		t.Fatal(got)
	}
	if got := o.csvPath("", "/out"); !strings.HasPrefix(got, "/out/traces-") {
		t.Fatal(got)
	}
	for _, bad := range []jaegerOpts{{on: true, mode: "x"}, {on: true, mode: jaegerModeBare, sample: 2}, {on: true, mode: jaegerModeBare, sample: 0.1, transport: "http", storage: "memory", maxTraces: 10, depth: 10, cpus: "1"}} {
		b := bad
		if b.validate() == nil {
			t.Errorf("accepted %+v", b)
		}
	}
	avoid := jaegerOpts{on: true, mode: jaegerModeBare, cpus: "62", host: "127.0.0.1", offset: 13863, sample: 0.02,
		transport: "http", storage: "memory", maxTraces: 200000, depth: 5000, avoid: "8080,18180-18199"}
	if err := avoid.validate(); err == nil || !strings.Contains(err.Error(), "port 18180") {
		t.Errorf("offset 13863 (4317 -> 18180) not rejected: %v", err)
	}
	avoid.offset = 20000
	if err := avoid.validate(); err != nil {
		t.Errorf("offset 20000: %v", err)
	}
	var off *jaegerOpts
	if off.enabled() || off.validate() != nil {
		t.Fatal("nil opts must mean off")
	}
}
