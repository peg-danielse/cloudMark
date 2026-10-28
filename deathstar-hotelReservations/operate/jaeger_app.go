package main

// Hotel side of the Jaeger integration (shared machinery in jaegercp.go).
//
// Sampling: the Go services build their tracer in tracing.Init with
// jaeger-client-go's FromEnv, so /start passes JAEGER_SAMPLE_RATIO
// (-trace-sample, probabilistic at the root service) and JAEGER_ENDPOINT
// (the jaeger.thrift HTTP collector at -jaeger-host:14268+offset). Nothing
// in config.json changes. Without -jaeger the services keep their built-in
// default (ratio 0.01 to 127.0.0.1:14268, which is normally not listening).

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
)

// hotelMemcached: which memcached instance (program basename = port) each
// service's memcached_* spans talk to (config.json memc-*).
var hotelMemcached = map[string]string{
	"profile":     "11211",
	"rate":        "11212",
	"reservation": "11213",
	"review":      "11214",
}

// hotelTraceModel: the route is the root span's service (the loadgen calls
// each service directly, untraced, so that service's server span is the root);
// tracer service names equal the program basenames; memcached_* and mongo*
// spans are charged to the store.
func hotelTraceModel() traceModel {
	routes := map[string]bool{}
	for _, n := range hotelServiceNames {
		routes[n] = true
	}
	return traceModel{
		route: func(root *traceSpan) string {
			if routes[root.service] {
				return root.service
			}
			return ""
		},
		service: func(s string) string { return s },
		backend: func(s *traceSpan) string {
			n := strings.ToLower(s.name)
			switch {
			case strings.HasPrefix(n, "memcached"):
				if d, ok := hotelMemcached[s.service]; ok {
					return d
				}
				return "memcached"
			case strings.HasPrefix(n, "mongo"):
				return "mongod"
			}
			return ""
		},
	}
}

// addTraceEnv adds the tracing variables to a Go service's /start env.
func addTraceEnv(line tuple, env map[string]string) {
	if !tracing.enabled() {
		return
	}
	if strings.Contains(line.path, "/memcached/") || filepath.Base(line.path) == "mongod" {
		return
	}
	env["JAEGER_SAMPLE_RATIO"] = tracing.sampleString()
	env["JAEGER_ENDPOINT"] = tracing.collectorHTTP()
	env["JAEGER_REPORTER_MAX_QUEUE_SIZE"] = "10000"
}

func hotelOutputDir() string { return filepath.Join(root, "deathstar-hotelReservations", "output") }

// jaegerLogPath sits next to the traces CSV.
func jaegerLogPath() string {
	return strings.TrimSuffix(tracing.out, filepath.Ext(tracing.out)) + ".jaeger.log"
}

// beginTraceAnalysis returns the writer autoscale prints route lines to and a
// finish function. With tracing on, the writer sniffs window markers and the
// analyser appends traces.csv rows (and cp lines) asynchronously.
func beginTraceAnalysis() (io.Writer, func(), error) {
	if !tracing.enabled() {
		return appOut, func() {}, nil
	}
	out := lockedWriter{mu: &sync.Mutex{}, w: appOut}
	var cpOut io.Writer
	if tracing.cpLines {
		cpOut = out
	}
	t, err := newCPTracer(tracing.queryBase(), hotelTraceModel(), tracing.out, cpOut, tracing.lag, tracing.depth)
	if err != nil {
		return nil, nil, err
	}
	fmt.Printf("traces csv %s query %s sample %s lag %s depth %d\n", tracing.out, tracing.queryBase(), tracing.sampleString(), tracing.lag, tracing.depth)
	return &lineSniffer{w: out, on: t.onLine}, func() { fmt.Println(t.close()) }, nil
}
