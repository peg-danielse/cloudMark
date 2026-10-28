package main

import "testing"

func TestHotelTraceModel(t *testing.T) {
	traces := loadFixture(t, "jaeger-v3-hotel.json")
	m := hotelTraceModel()
	r, ok := analyzeTrace(traces["0000000000000000000000000000000a"], m)
	if !ok || r.route != "search" {
		t.Fatalf("%+v %v", r, ok)
	}
	if !near(ms(r.crit["11212"]), 2) || !near(ms(r.crit["rate"]), 2.5) || !near(ms(r.crit["geo"]), 3) {
		t.Fatalf("crit %v", r.crit)
	}
	for svc, want := range map[string]string{"profile": "11211", "review": "11214", "attractions": "memcached"} {
		if got := m.backend(&traceSpan{service: svc, name: "memcached_get_x"}); got != want {
			t.Errorf("%s memcached -> %s, want %s", svc, got, want)
		}
	}
	if m.backend(&traceSpan{service: "attractions", name: "mongo_cinema"}) != "mongod" || m.backend(&traceSpan{service: "geo", name: "/geo.Geo/Nearby"}) != "" {
		t.Error("backend mapping")
	}
	for _, n := range hotelServiceNames {
		if m.route(&traceSpan{service: n}) != n {
			t.Errorf("route %s", n)
		}
	}
	if m.route(&traceSpan{service: "jaeger"}) != "" {
		t.Error("jaeger is not a route")
	}
}

func TestHotelTraceEnv(t *testing.T) {
	defer func(old *jaegerOpts) { tracing = old }(tracing)
	svc := tuple{path: "/b/deathstar-hotelReservations/bin/search", id: "hotel-search"}
	mc := tuple{path: "/b/runtime/memcached/11211", id: "hotel-11211"}
	tracing = nil
	env := map[string]string{}
	addTraceEnv(svc, env)
	if len(env) != 0 {
		t.Fatalf("tracing off changed env: %v", env)
	}
	tracing = &jaegerOpts{on: true, host: "127.0.0.1", offset: 100, sample: 0.05}
	addTraceEnv(svc, env)
	if env["JAEGER_SAMPLE_RATIO"] != "0.05" || env["JAEGER_ENDPOINT"] != "http://127.0.0.1:14368/api/traces" {
		t.Fatalf("env %v", env)
	}
	env = map[string]string{}
	addTraceEnv(mc, env)
	if len(env) != 0 {
		t.Fatalf("memcached got %v", env)
	}
}

// testdata/jaeger-v3-hotel-recorded.json: two real traces per route recorded
// from Jaeger v2.22.0 during a PC autoscale run (1% sampling, host attributes
// scrubbed).
func TestHotelRecordedTraces(t *testing.T) {
	traces := loadFixture(t, "jaeger-v3-hotel-recorded.json")
	m := hotelTraceModel()
	routes := map[string]int{}
	allowed := map[string]map[string]bool{
		"search":  {"search": true, "geo": true, "rate": true, "11212": true, "mongod": true},
		"geo":     {"geo": true},
		"profile": {"profile": true, "11211": true, "mongod": true},
	}
	for id, spans := range traces {
		r, ok := analyzeTrace(spans, m)
		if !ok {
			t.Errorf("trace %s not analysed", id)
			continue
		}
		routes[r.route]++
		var sum int64
		for d, v := range r.crit {
			sum += v
			if a, ok := allowed[r.route]; ok && !a[d] {
				t.Errorf("%s trace charged %s", r.route, d)
			}
		}
		if sum != r.dur {
			t.Errorf("%s: critical path %d ns, request %d ns", id, sum, r.dur)
		}
	}
	for _, n := range hotelServiceNames {
		if routes[n] != 2 {
			t.Errorf("route %s: %d traces, want 2", n, routes[n])
		}
	}
}
