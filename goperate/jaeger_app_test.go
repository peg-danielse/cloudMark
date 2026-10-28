package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSocialServiceDaemon(t *testing.T) {
	for _, bin := range []string{"ComposePostService", "UserMentionService", "UniqueIdService", "UrlShortenService", "WriteHomeTimelineService"} {
		if got := socialServiceDaemon(serviceKey(bin)); got != bin {
			t.Errorf("%s -> %s -> %s", bin, serviceKey(bin), got)
		}
	}
}

// testdata/jaeger-v3-social.json: compose-post (nested RPCs, Mongo, an async
// RabbitMQ consumer that outlives the request), read-home-timeline (Redis,
// RPC to post-storage, memcached) and login (memcached, Mongo).
func TestSocialCriticalPath(t *testing.T) {
	traces := loadFixture(t, "jaeger-v3-social.json")
	m := socialTraceModel()
	want := map[string]map[string]float64{
		"compose-post": {"ComposePostService": 8, "TextService": 3, "UrlShortenService": 1, "PostStorageService": 2,
			"mongod": 4, "rabbitmq-server": 2},
		"read-home-timeline": {"HomeTimelineService": 1, "PostStorageService": 1, "11312": 1, "redis-server": 1},
		"login":              {"UserService": 1, "11311": 0.5, "mongod": 1.5},
	}
	seen := map[string]bool{}
	for _, spans := range traces {
		r, ok := analyzeTrace(spans, m)
		if !ok {
			t.Fatalf("trace %s not analysed", spans[0].traceID)
		}
		seen[r.route] = true
		w := want[r.route]
		var sum int64
		for d, v := range r.crit {
			sum += v
			if !near(ms(v), w[d]) {
				t.Errorf("%s %s: %.3f ms, want %.3f", r.route, d, ms(v), w[d])
			}
		}
		for d, v := range w {
			if !near(ms(r.crit[d]), v) {
				t.Errorf("%s %s missing: %.3f ms, want %.3f", r.route, d, ms(r.crit[d]), v)
			}
		}
		if sum != r.dur {
			t.Errorf("%s: path %d != request %d", r.route, sum, r.dur)
		}
		if r.route == "compose-post" {
			if r.spans["WriteHomeTimelineService"] != 1 || r.crit["WriteHomeTimelineService"] != 0 || r.spans["redis-server"] != 1 {
				t.Errorf("async consumer: spans %v crit %v", r.spans, r.crit)
			}
			if !near(ms(r.rpc["TextService"]), 1) || !near(ms(r.rpc["UrlShortenService"]), 0.4) || !near(ms(r.rpc["PostStorageService"]), 1) {
				t.Errorf("rpc %v", r.rpc)
			}
		}
	}
	if len(seen) != 3 {
		t.Fatalf("routes %v", seen)
	}
}

func TestSocialRunDir(t *testing.T) {
	defer func(old *jaegerOpts) { tracing = old }(tracing)
	dir := t.TempDir()
	tracing = &jaegerOpts{on: true, host: "127.0.0.1", offset: 10, sample: 0.03, transport: "http", out: filepath.Join(dir, "run", "traces.csv")}
	svcCfg := filepath.Join(dir, "service-config.json")
	os.WriteFile(svcCfg, []byte("{}"), 0o644)
	if err := prepareSocialRunDir(socialRunDir(), svcCfg); err != nil {
		t.Fatal(err)
	}
	if err := prepareSocialRunDir(socialRunDir(), svcCfg); err != nil { // idempotent
		t.Fatal(err)
	}
	y, err := os.ReadFile(filepath.Join(dir, "run", "social-cwd", "config", "jaeger-config.yml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"disabled: false", "type: probabilistic", "param: 0.03", "endpoint: http://127.0.0.1:14278/api/traces", "bufferFlushInterval: 1"} {
		if !strings.Contains(string(y), s) {
			t.Errorf("config lacks %q:\n%s", s, y)
		}
	}
	if target, _ := os.Readlink(filepath.Join(dir, "run", "social-cwd", "config", "service-config.json")); target != svcCfg {
		t.Errorf("link -> %s", target)
	}
	tracing.transport = "udp"
	if !strings.Contains(socialJaegerYAML(tracing), "localAgentHostPort: 127.0.0.1:6841") {
		t.Error("udp reporter")
	}
	env := addTraceEnv(tuple{path: "/b/deathstar-socialNetwork/run/TextService"}, nil)
	if env[envSocialRunDir] != filepath.Join(dir, "run", "social-cwd") {
		t.Errorf("env %v", env)
	}
	if env := addTraceEnv(tuple{path: "/b/runtime/memcached/11311"}, map[string]string{"MEMCACHED_THREADS": "16"}); len(env) != 1 {
		t.Errorf("memcached env %v", env)
	}
	tracing = nil
	if env := addTraceEnv(tuple{path: "/b/run/TextService"}, nil); env != nil {
		t.Errorf("tracing off: %v", env)
	}
}
