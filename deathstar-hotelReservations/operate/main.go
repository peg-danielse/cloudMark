// Command operate submits mercury-hotel.txt to a running Mercury process.
//
// Mercury is started separately:
//
//	mercury <port> <cpu> [cpu...]
//
// Its working directory must be deathstar-hotelReservations so each program
// reads config.json. This command does not start Mercury. It POSTs /start
// for each line, waits until that program is listening, and can run the
// hotel checks.
//
//	go run ./operate -mercury 127.0.0.1:8080 -cpu 2 test
//	go run ./operate -mercury 127.0.0.1:8080 -cpu 2 scale
//	go run ./operate -mercury 127.0.0.1:8080 -cpu 2 autoscale
//	go run ./operate -mercury 127.0.0.1:8080 -cpu 2 start
//	go run ./operate -mercury 127.0.0.1:8080 stop
//
// scale requires Mercury pinned to Linux CPU 0. It widens each hotel service
// across 1, 2, 4, 8, 16, and 32 cores and prints arrival_rate, service_rate,
// and the average and p95 service times.
//
// autoscale -client mercury (default) runs the load generator as Mercury daemon
// hotel-loadgen (bin/loadgen, build with go build -o bin/loadgen ./operate) on
// -gen-cpu with saturate -gen-min..-gen-max and copies its output here.
// -client inproc keeps the generator in this process on CPUs 60-63.
// -conns sets the gRPC connections each route shares (0 = one per worker).
//
// autoscale attaches Mercury's saturate policy at 16 cores to every program
// in mercury-hotel.txt, then raises every route together from 1 caller to
// -users and back down.
//
// -cold-cache leaves odd hotel ids out of memcached, so those reads stay on
// MongoDB. -cold-cache=false caches every hotel id. Go services receive that
// choice as HOTEL_COLD_CACHE.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/boot"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/probe"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// root is this repository's benchmarks directory: DSB_ROOT, else the directory
// above the built binary, else the source tree (go test / go run).
var root = benchmarksRoot()

func benchmarksRoot() string {
	if r := os.Getenv("DSB_ROOT"); r != "" {
		return r
	}
	if exe, err := os.Executable(); err == nil {
		if exe, err = filepath.EvalSymlinks(exe); err == nil {
			if r := findBenchmarks(filepath.Dir(exe)); r != "" {
				return r
			}
		}
	}
	if _, file, _, ok := runtime.Caller(0); ok {
		if r := findBenchmarks(filepath.Dir(file)); r != "" {
			return r
		}
	}
	fmt.Fprintln(os.Stderr, "cannot find the benchmarks directory; set DSB_ROOT")
	os.Exit(1)
	return ""
}

func findBenchmarks(start string) string {
	dir := start
	for {
		hotel := filepath.Join(dir, "deathstar-hotelReservations")
		runtimeDir := filepath.Join(dir, "runtime")
		if info, err := os.Stat(hotel); err == nil && info.IsDir() {
			if info, err = os.Stat(runtimeDir); err == nil && info.IsDir() {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func resolveExec(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(benchmarksRoot(), path)
}

func main() {
	if os.Getenv(envRole) == roleGen {
		if err := runLoadgen(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	prefix := flag.String("prefix", "hotel", "daemon id prefix (<prefix>-geo, <prefix>-loadgen, ...); give each hotel sharing a Mercury its own")
	flag.StringVar(&readyFile, "ready-file", "", "autoscale: after every daemon listens, write this file and wait for -go-file before any load (run-demo.sh port checks)")
	flag.StringVar(&goFile, "go-file", "", "with -ready-file: the file to wait for (default <ready-file>.go)")
	mercury := flag.String("mercury", "127.0.0.1:8080", "Mercury host:port")
	cpu := flag.Int("cpu", 0, "daemon CPU number Mercury was given (not the CPU Mercury pinned itself to)")
	users := flag.Int("users", 1024, "maximum concurrent users on each autoscale route")
	coldCache := flag.Bool("cold-cache", true, "leave odd hotel ids uncached so requests still read MongoDB (HOTEL_COLD_CACHE)")
	file := flag.String("file", root+"/mercury-hotel.txt", "deployment list")
	configPath := flag.String("config", root+"/deathstar-hotelReservations/config.json", "hotel address file")
	client := flag.String("client", clientMercury, "autoscale load generator: mercury (bin/loadgen as a Mercury daemon) or inproc (this process pinned to CPUs 60-63)")
	entryFlag := flag.String("entry", entryFrontend, "autoscale routes: frontend (the frontend's HTTP endpoints: hotels, recommendations, user, review, restaurants, museums, cinema, reservation) or services (each service directly over gRPC, as the earlier sweeps)")
	histFlag := flag.Bool("hist", false, "autoscale: write every window's latency histogram per route to <gen-out stem>.hist.jsonl (deadline miss rates offline)")
	routesFlag := flag.String("routes", "all", "autoscale: comma list of routes to drive (geo,profile,rate,recommendation,user,reservation,search,review,attractions,frontend); the other services start but stay idle")
	conns := flag.Int("conns", 16, "gRPC connections each route shares among its workers; 0 = one per worker (old behaviour)")
	genBin := flag.String("gen-bin", root+"/deathstar-hotelReservations/bin/loadgen", "loadgen binary for -client mercury")
	genOut := flag.String("gen-out", "", "loadgen output file (default output/loadgen-<unix>.out)")
	genCPU := flag.Int("gen-cpu", defaultGenCPU, "loadgen home Linux CPU")
	genMin := flag.Int("gen-min", defaultGenMin, "loadgen saturate min cores")
	genMax := flag.Int("gen-max", defaultGenMax, "loadgen saturate max cores")
	genProcs := flag.Int("gen-procs", defaultGenProcs, "loadgen GOMAXPROCS (keep >= gen-max so busy threads can exceed cores)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: operate [flags] start|stop|test|scale|autoscale\n\n")
		flag.PrintDefaults()
	}
	svcMin := flag.Int("svc-min", 1, "autoscale: per-service min cores")
	svcMax := flag.Int("svc-max", 16, "autoscale: per-service max cores")
	pin := flag.Bool("pin", true, "with -policy gnn: pin busy threads to CPUs (false = only size cpusets)")
	window := flag.Int("window-ms", 1000, "autoscale measurement window in ms (4 windows per user step)")
	policy := flag.String("policy", "saturate", "autoscale policy for the service daemons: saturate or gnn (the load generator stays on saturate)")
	tickMs := flag.Int("tick-ms", 10, "gnn decision tick in ms (sent with -policy gnn)")
	svcFixedFlag := flag.String("svc-fixed", "", "autoscale: per-program fixed cores, e.g. geo=1,mongod=2 (min = max for those daemons)")
	placement := flag.String("placement", "least_loaded", "core hand-out for /scale: least_loaded or nearest_first")
	addLoadFlags("hotel")          // -load sine, -users-min/-max, -period, -phase, -step, -duration, -ramp-in, -homes: demoload.go
	tracing = addJaegerFlags(root) // -jaeger*, -trace-*: see jaegercp.go and jaeger_app.go
	flag.Parse()
	idPrefix, loadgenID = *prefix, *prefix+"-loadgen"
	demoLoad.app = *prefix
	if err := tracing.validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	cmd := "test"
	if flag.NArg() > 0 {
		cmd = flag.Arg(0)
	}
	if flag.NArg() > 1 || (cmd != "start" && cmd != "stop" && cmd != "test" && cmd != "scale" && cmd != "autoscale") {
		flag.Usage()
		os.Exit(2)
	}
	if *client != clientMercury && *client != clientInproc {
		fmt.Fprintf(os.Stderr, "-client must be %s or %s\n", clientMercury, clientInproc)
		os.Exit(2)
	}
	if *conns < 0 || *genMin < 1 || *genMax < *genMin || *genProcs < 1 {
		fmt.Fprintln(os.Stderr, "need -conns >= 0, 1 <= -gen-min <= -gen-max, -gen-procs >= 1")
		os.Exit(2)
	}
	if *policy != "saturate" && *policy != "gnn" || *placement != "least_loaded" && *placement != "nearest_first" || *tickMs < 10 {
		fmt.Fprintln(os.Stderr, "need -policy saturate|gnn, -placement least_loaded|nearest_first, -tick-ms >= 10")
		os.Exit(2)
	}
	if *window < 100 {
		fmt.Fprintln(os.Stderr, "need -window-ms >= 100")
		os.Exit(2)
	}
	windowMs = *window
	if err := demoLoad.validate(windowMs); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if demoLoad.sine() && *client == clientMercury && *genOut == "" {
		*genOut = defaultGenOut() // the windows CSV sits next to it
	}
	servicePin = *pin
	svcMinCores, autoMaxCores = *svcMin, *svcMax
	servicePolicy, serviceTickMs, servicePlacement = *policy, *tickMs, *placement
	if err := parseSvcFixed(*svcFixedFlag); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	connsPerRoute = *conns
	var err error
	if *entryFlag != entryFrontend && *entryFlag != entryServices {
		fmt.Fprintf(os.Stderr, "-entry must be %s or %s\n", entryFrontend, entryServices)
		os.Exit(2)
	}
	loadEntry = *entryFlag
	if loadRoutes, err = parseRoutes(*routesFlag); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if *histFlag {
		if *genOut == "" {
			*genOut = defaultGenOut()
		}
		histPath = strings.TrimSuffix(*genOut, ".out") + ".hist.jsonl"
		if *client == clientInproc { // this process drives the routes itself
			hf, err := os.Create(histPath)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
			defer hf.Close()
			histOut = hf
		}
	}
	if tracing.enabled() {
		if *client == clientMercury && *genOut == "" {
			*genOut = defaultGenOut()
		}
		tracing.out = tracing.csvPath(*genOut, hotelOutputDir())
	}
	opts := clientOpts{
		mode: *client, bin: *genBin, out: *genOut, config: *configPath, homeCPU: *genCPU,
		genMin: *genMin, genMax: *genMax, genProcs: *genProcs, conns: *conns,
	}
	if err := run(*mercury, *cpu, *users, *file, *configPath, cmd, *coldCache, opts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type tuple struct {
	path     string
	hostname string
	id       string
}

func run(mercury string, cpu, users int, file, configPath, cmd string, coldCache bool, opts clientOpts) error {
	lines, err := parseFile(file)
	if err != nil {
		return err
	}
	cfg, err := boot.LoadFile(configPath)
	if err != nil {
		return err
	}
	for svc, addr := range cfg.Memcached { // trace attribution follows this instance's memcached ports
		if _, port, err := net.SplitHostPort(addr); err == nil {
			hotelMemcached[svc] = port
		}
	}
	base := mercuryURL(mercury)
	if err := ping(base); err != nil {
		return err
	}

	switch cmd {
	case "stop":
		if tracing.enabled() && tracing.mode == jaegerModeMercury {
			_ = kill(base, idPrefix+"-jaeger")
		}
		return stopAll(base, lines)
	case "start", "test", "scale", "autoscale":
		if cpu < 1 {
			return fmt.Errorf("-cpu is required: pass a daemon CPU from the mercury command line")
		}
		if cmd == "autoscale" && users < 1 {
			return fmt.Errorf("-users must be at least 1")
		}
	default:
		return fmt.Errorf("unknown command %s", cmd)
	}

	var (
		startedMu sync.Mutex
		started   []tuple
		jaeger    *jaegerProc
	)
	track := func(t tuple) {
		startedMu.Lock()
		started = append(started, t)
		startedMu.Unlock()
	}
	stopStarted := func() {
		startedMu.Lock()
		defer startedMu.Unlock()
		for i := len(started) - 1; i >= 0; i-- {
			if err := kill(base, started[i].id); err != nil {
				fmt.Fprintf(os.Stderr, "kill %s: %v\n", started[i].id, err)
			}
		}
		started = nil
		jaeger.stop()
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		stopStarted()
		os.Exit(1)
	}()

	if tracing.enabled() {
		if cmd == "start" && tracing.mode == jaegerModeBare {
			fmt.Fprintln(os.Stderr, "warning: -jaeger-mode bare Jaeger stops when operate exits; use mercury or external with start")
		}
		if jaeger, err = startJaeger(tracing, base, cpu, idPrefix, jaegerLogPath(), track); err != nil {
			stopStarted()
			return err
		}
	}

	for _, line := range lines {
		addr, err := listenAddr(line, cfg)
		if err != nil {
			stopStarted()
			return err
		}
		if err := portFree(addr); err != nil {
			stopStarted()
			return fmt.Errorf("%s: %w", line.id, err)
		}
		env := daemonEnv(line, coldCache)
		addTraceEnv(line, env)
		pid, err := start(base, line, cpu, env)
		if err != nil {
			stopStarted()
			return fmt.Errorf("%s: %w", line.id, err)
		}
		fmt.Printf("started %s daemon_id=%s pid %d %s\n", filepath.Base(line.path), line.id, pid, formatEnv(env))
		track(line)
		if err := waitPort(addr, 60*time.Second); err != nil {
			stopStarted()
			return fmt.Errorf("%s did not listen on %s: %w", line.id, addr, err)
		}
		if filepath.Base(line.path) == "mongod" {
			if err := dropHotelDatabases(cfg.Mongo); err != nil {
				stopStarted()
				return err
			}
		}
	}

	if cmd == "autoscale" && readyFile != "" {
		if err := waitGo(readyFile, goFile); err != nil {
			stopStarted()
			return err
		}
	}
	if cmd == "start" {
		fmt.Printf("left %d daemons running\n", len(started))
		return nil
	}
	switch cmd {
	case "scale":
		err = scaleServices(base, cfg, append([]tuple(nil), started...))
	case "autoscale":
		err = autoscaleServices(base, cfg, append([]tuple(nil), started...), users, opts, track)
	default:
		err = probe.Run(cfg)
	}
	stopStarted()
	return err
}

func parseFile(path string) ([]tuple, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var lines []tuple
	seen := map[string]bool{}
	for n, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("%s:%d: want exec_path and hostname", path, n+1)
		}
		id := idPrefix + "-" + filepath.Base(fields[0])
		if seen[id] {
			return nil, fmt.Errorf("duplicate daemon id %s", id)
		}
		seen[id] = true
		lines = append(lines, tuple{path: resolveExec(fields[0]), hostname: fields[1], id: id})
	}
	if len(lines) == 0 {
		return nil, fmt.Errorf("%s has no programs", path)
	}
	return lines, nil
}

func listenAddr(line tuple, cfg boot.Config) (string, error) {
	base := filepath.Base(line.path)
	if _, err := strconv.Atoi(base); err == nil {
		return net.JoinHostPort("127.0.0.1", base), nil
	}
	if base == "mongod" {
		return cfg.Mongo, nil
	}
	name := base
	if name != "frontend" {
		name = "srv-" + name
	}
	return cfg.Address(name)
}

func mercuryURL(addr string) string {
	if strings.Contains(addr, "://") {
		return strings.TrimRight(addr, "/")
	}
	return "http://" + addr
}

func ping(base string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/ping", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("mercury %s is not answering: %w", base, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64))
	if resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("pong")) {
		return fmt.Errorf("mercury %s /ping: %s %s", base, resp.Status, bytes.TrimSpace(body))
	}
	return nil
}

// daemonEnv is sent as Mercury's /start "env" object. Go reads GOMAXPROCS
// while the runtime starts, before it samples the one-core cpuset. Memcached
// and mongod do not, so their wrappers turn these variables into -t and
// WiredTiger's concurrent-transaction caps.
// threadCapCores caps service threads (memcached -t, WiredTiger, GOMAXPROCS) independently of -svc-max (gnn-slo-v1 sweep).
const threadCapCores = 16

func daemonEnv(line tuple, coldCache bool) map[string]string {
	limit := strconv.Itoa(threadCapCores) // not -svc-max: fixed-allocation sweeps keep the same thread counts
	switch {
	case strings.Contains(line.path, "/memcached/"):
		return map[string]string{"MEMCACHED_THREADS": limit}
	case filepath.Base(line.path) == "mongod":
		return map[string]string{
			"MONGO_WT_READ_TRANSACTIONS":  limit,
			"MONGO_WT_WRITE_TRANSACTIONS": limit,
		}
	default:
		cold := "0"
		if coldCache {
			cold = "1"
		}
		return map[string]string{
			"GOMAXPROCS":       limit,
			"HOTEL_COLD_CACHE": cold,
		}
	}
}

func formatEnv(env map[string]string) string {
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, key := range keys {
		parts[i] = key + "=" + env[key]
	}
	return strings.Join(parts, " ")
}

func start(base string, line tuple, cpu int, env map[string]string) (int, error) {
	var out struct {
		Pid int `json:"pid"`
	}
	status, body, err := post(base, "/start", map[string]any{
		"daemon_id": line.id,
		"exec_path": line.path,
		"cpu":       cpu,
		"env":       env,
	})
	if err != nil {
		return 0, err
	}
	if status != http.StatusOK || json.Unmarshal(body, &out) != nil || out.Pid <= 0 {
		return 0, fmt.Errorf("POST /start: %d %s", status, strings.TrimSpace(string(body)))
	}
	return out.Pid, nil
}

func kill(base, id string) error {
	status, body, err := post(base, "/kill", map[string]string{"daemon_id": id})
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("POST /kill %s: %d %s", id, status, strings.TrimSpace(string(body)))
	}
	fmt.Printf("killed %s\n", id)
	return nil
}

func stopAll(base string, lines []tuple) error {
	var failed error
	for i := len(lines) - 1; i >= 0; i-- {
		if err := kill(base, lines[i].id); err != nil && failed == nil {
			failed = err
		}
	}
	return failed
}

func post(base, path string, payload any) (int, []byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, body, err
}

func portFree(addr string) error {
	c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return nil
	}
	c.Close()
	return fmt.Errorf("%s is already in use", addr)
}

func waitPort(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return nil
		}
		last = err
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out: %v", last)
}

func dropHotelDatabases(mongoAddr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://"+mongoAddr))
	if err != nil {
		return err
	}
	defer client.Disconnect(context.Background())
	for _, name := range []string{
		"geo-db", "profile-db", "rate-db", "reservation-db",
		"recommendation-db", "user-db", "review-db", "attractions-db",
	} {
		if err := client.Database(name).Drop(ctx); err != nil {
			return fmt.Errorf("drop %s: %w", name, err)
		}
	}
	return nil
}

var readyFile, goFile string

// waitGo writes ready and blocks until the go file exists (30 min at most), so a
// caller can check ports and entry points with every daemon up but no load yet.
func waitGo(ready, goPath string) error {
	if goPath == "" {
		goPath = ready + ".go"
	}
	if err := os.WriteFile(ready, []byte(fmt.Sprintf("%s ready %d\n", idPrefix, time.Now().UnixMilli())), 0644); err != nil {
		return err
	}
	fmt.Printf("%s: daemons up, waiting for %s\n", idPrefix, goPath)
	for deadline := time.Now().Add(30 * time.Minute); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if _, err := os.Stat(goPath); err == nil {
			return nil
		}
	}
	return fmt.Errorf("%s never appeared", goPath)
}
