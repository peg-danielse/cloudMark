package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/boot"
	"golang.org/x/sys/unix"
)

// -svc-min / -svc-max: service scale bounds (-svc-min N -svc-max N = fixed N cores).
var (
	autoMaxCores = 16
	svcMinCores  = 1
)

const (
	autoWindows = 4

	// Linux CPU 0 is Mercury. Homes begin at CPU 1, one per line of
	// mercury-hotel.txt. The loader keeps clientFirstCPU through clientLastCPU.
	homeFirstLinux = 1
)

// autoscaleServices places every daemon from the deployment file on its own
// core, caps each one at 16 cores with the saturate policy, and drives every
// hotel route at the same time from 1 caller up to maxUsers and back down.
//
// On this 64-CPU machine start Mercury with the daemon pool only:
//
//	mercury 8080 1 2 3 ... 60
//	go run ./operate -mercury 127.0.0.1:8080 -cpu 2 -users 1024 autoscale
//
// CPU 0 stays with Mercury. The 15 programs in mercury-hotel.txt take Linux
// CPUs 1-15 as their home cores. Saturate grows a daemon by taking the lowest
// pool CPU that daemon does not already have, up to 16, so those sets overlap
// inside CPUs 1-59. All ten routes run together, so the programs grow at the
// same time and share that pool. CPUs 60-63 are this process and are left off
// Mercury's command line.
//
// /start passes GOMAXPROCS=16 to each Go service, MEMCACHED_THREADS=16 to each
// cache, and MONGO_WT_READ_TRANSACTIONS=16 plus MONGO_WT_WRITE_TRANSACTIONS=16
// to mongod. The saturate max is the same 16.
// clientOpts chooses how autoscale drives load.
//
// client "mercury" (default) starts bin/loadgen as a Mercury daemon with
// POST /start, sets its home cpuset with POST /cpus, and attaches saturate
// between genMin and genMax cores. The orchestrator then copies loadgen's
// output file to stdout until loadgen exits.
//
// client "inproc" is the original client: this process pins itself to
// clientFirstCPU-clientLastCPU and drives the routes itself.
type clientOpts struct {
	mode     string
	bin      string
	out      string
	config   string
	homeCPU  int
	genMin   int
	genMax   int
	genProcs int
	conns    int
}

const (
	clientMercury = "mercury"
	clientInproc  = "inproc"

	defaultGenCPU   = 60
	defaultGenMin   = 2
	defaultGenMax   = 8
	defaultGenProcs = 8
)

func autoscaleServices(base string, cfg boot.Config, lines []tuple, maxUsers int, opts clientOpts, track func(tuple)) error {
	maxUsers = demoLoad.workers(maxUsers)
	homes, err := daemonHomes(len(lines))
	if err != nil {
		return err
	}
	if opts.mode == clientInproc {
		if err := pinTasks(clientCPUs()); err != nil {
			return err
		}
		runtime.GOMAXPROCS(len(clientCPUs()))
		fmt.Printf("layout linux_cpus=64 mercury=0 homes=%d-%d max_cores=%d clients=%d-%d\n",
			homes[0], homes[len(homes)-1], autoMaxCores, clientFirstCPU, clientLastCPU)
	} else {
		fmt.Printf("layout linux_cpus=64 mercury=0 homes=%d-%d max_cores=%d clients=mercury:%s home=%d saturate=%d-%d gomaxprocs=%d conns=%d\n",
			homes[0], homes[len(homes)-1], autoMaxCores, loadgenID, opts.homeCPU, opts.genMin, opts.genMax, opts.genProcs, opts.conns)
	}
	fmt.Print("homes")
	for i, line := range lines {
		spec := strconv.Itoa(homes[i])
		if err := setCpus(base, line.id, spec); err != nil {
			return err
		}
		mn, mx := svcRange(line, svcMinCores, autoMaxCores)
		if err := enableScale(base, line.id, servicePolicy, mn, mx); err != nil {
			return err
		}
		fmt.Printf(" %s=%s", filepath.Base(line.path), spec)
	}
	fmt.Println()
	fmt.Printf("policy %s placement %s tick_ms %d min %d max %d users 1-%d\n", servicePolicy, servicePlacement, serviceTickMs, svcMinCores, autoMaxCores, maxUsers)

	for _, name := range hotelServiceNames {
		if _, ok := findLine(lines, name); !ok {
			return fmt.Errorf("deployment has no %s", name)
		}
	}
	fmt.Println(demoLoad.describe())
	closeWindows, err := startWindowLog(opts.out, hotelOutputDir())
	if err != nil {
		return err
	}
	defer closeWindows()
	w, finishTraces, err := beginTraceAnalysis()
	if err != nil {
		return err
	}
	defer finishTraces()
	if opts.mode == clientInproc {
		traceMarks = tracing.enabled()
		gen := &genMeter{cpus: inprocCPUs, busy: selfBusy, max: len(clientCPUs())}
		return driveRoutes(cfg, maxUsers, w, lines, gen)
	}
	return runMercuryLoadgen(base, lines, maxUsers, opts, track, w)
}

// driveRoutes ramps every hotel route together and prints one line per route
// per window. Columns up to cpus match the earlier autoscaleN.out files; the
// gen_* columns describe the load generator during that window.
func driveRoutes(cfg boot.Config, maxUsers int, w io.Writer, lines []tuple, gen *genMeter) error {
	routes, err := openRoutes(cfg, maxUsers)
	defer stopRoutes(routes)
	if err != nil {
		return err
	}

	var marks markCounter
	next := demoLoad.levels(maxUsers, windowMs, time.Now) // -load ramp or sine (demoload.go)
	for {
		lv, ok := next()
		if !ok {
			break
		}
		level, clients := lv.level, lv.clients
		for _, route := range routes {
			route.rec.setLevel(clients)
		}
		for window := 0; window < lv.windows; window++ {
			g0 := gen.begin()
			t0 := time.Now()
			readings, diffs, m0, m1 := measureRoutes(routes)
			t1 := time.Now()
			gw := gen.end(g0)
			cpus := cpuSetList(lines, readCPUSet)
			marks.emit(w, level, window, clients, t0, t1) // -jaeger only: window marker for the trace analyser
			writeHist(routes, diffs, level, window, clients, m0, m1)
			for i, route := range routes {
				reading := readings[i]
				fmt.Fprintf(w, "route %s clients %d arrival_rate %d service_rate %d avg_us %.1f p95_us %.1f p99_us %.1f errors %d cpus %s %s%s\n",
					route.name, clients, reading.arrivalRate, reading.serviceRate, reading.avgUs, reading.p95Us, reading.p99Us, reading.errors,
					cpus, gw, lv.suffix())
			}
		}
	}
	return nil
}

type liveRoute struct {
	name   string
	work   load
	rec    *recorder
	cancel context.CancelFunc
}

func openRoutes(cfg boot.Config, maxUsers int) ([]liveRoute, error) {
	routes := make([]liveRoute, 0, len(loadRoutes))
	for _, name := range loadRoutes {
		work, err := openLoad(cfg, name, maxUsers)
		if err != nil {
			return routes, err
		}
		ctx, cancel := context.WithCancel(context.Background())
		var rec *recorder
		if demoLoad.poisson() {
			rec = startPoisson(ctx, work.call, demoLoad.inflight, demoLoad.timeout(), routeSeed(name))
			rec.setLevel(readyRate) // until the schedule starts
		} else {
			rec = startLoad(ctx, work.call, maxUsers, 1)
		}
		routes = append(routes, liveRoute{name: name, work: work, rec: rec, cancel: cancel})
		if err := waitReady(rec, name); err != nil {
			return routes, err
		}
	}
	return routes, nil
}

func stopRoutes(routes []liveRoute) {
	for _, route := range routes {
		if route.cancel != nil {
			route.cancel()
		}
	}
	for _, route := range routes {
		if route.rec != nil {
			route.rec.wg.Wait()
		}
		if route.work.close != nil {
			route.work.close()
		}
	}
}

// windowMs is the autoscale measurement window (-window-ms, LOADGEN_WINDOW_MS); default 1 s.
var windowMs = 1000

// measureRoutes returns one reading per route for a window, the per-route counter deltas of that
// window (for -hist), and the window's CLOCK_MONOTONIC bounds (the clock of threadtrace).
func measureRoutes(routes []liveRoute) ([]reading, []snap, int64, int64) {
	befores := make([]snap, len(routes))
	for i, route := range routes {
		befores[i] = route.rec.snapshot()
	}
	m0 := monoNs()
	began := time.Now()
	time.Sleep(time.Duration(windowMs) * time.Millisecond)
	elapsed := time.Since(began)
	m1 := monoNs()
	out := make([]reading, len(routes))
	diffs := make([]snap, len(routes))
	for i, route := range routes {
		after := route.rec.snapshot()
		out[i] = readWindow(befores[i], after, elapsed)
		diffs[i] = after.sub(befores[i])
	}
	return out, diffs, m0, m1
}

// histOut receives one JSON line per route per window when -hist is on (LOADGEN_HIST in bin/loadgen):
// the window's monotonic bounds, counts, and every non-empty latency bucket as [upper_ns, count],
// so a deadline miss rate can be computed afterwards for any deadline.
var histOut io.Writer

func writeHist(routes []liveRoute, diffs []snap, level, window, clients int, m0, m1 int64) {
	if histOut == nil {
		return
	}
	for i, route := range routes {
		d := diffs[i]
		var b strings.Builder
		fmt.Fprintf(&b, `{"route":%q,"level":%d,"window":%d,"clients":%d,"t0_ns":%d,"t1_ns":%d,"sent":%d,"done":%d,"errors":%d,"hist":[`,
			route.name, level, window, clients, m0, m1, d.sent, d.done, d.errs)
		first := true
		for k, c := range d.hist {
			if c == 0 {
				continue
			}
			if !first {
				b.WriteByte(',')
			}
			first = false
			fmt.Fprintf(&b, "[%d,%d]", bucketUpper(k), c)
		}
		b.WriteString("]}\n")
		io.WriteString(histOut, b.String())
	}
}

func monoNs() int64 {
	var ts unix.Timespec
	if unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts) != nil {
		return 0
	}
	return ts.Nano()
}

func daemonHomes(n int) ([]int, error) {
	if n < 1 {
		return nil, fmt.Errorf("no daemons")
	}
	if demoLoad.homes != "" {
		return listHomes(demoLoad.homes, n) // -homes: demo splits the pool between apps
	}
	homes := make([]int, n)
	for i := range homes {
		cpu := homeFirstLinux + i
		if cpu >= clientFirstCPU {
			return nil, fmt.Errorf("daemon %d needs linux cpu %d, loader owns %d-%d", i, cpu, clientFirstCPU, clientLastCPU)
		}
		homes[i] = cpu
	}
	return homes, nil
}

func rampClients(maxUsers int) []int {
	up := []int{1}
	for up[len(up)-1] < maxUsers {
		next := up[len(up)-1] * 2
		if next <= up[len(up)-1] || next > maxUsers {
			next = maxUsers
		}
		up = append(up, next)
	}
	clients := make([]int, 0, len(up)*2-1)
	clients = append(clients, up...)
	for i := len(up) - 2; i >= 0; i-- {
		clients = append(clients, up[i])
	}
	return clients
}

func enableSaturate(base, id string, minCores, maxCores int) error {
	return enableScale(base, id, "saturate", minCores, maxCores)
}

// Policy for the service daemons (-policy, -tick-ms, -placement). The load generator stays on saturate.
var (
	servicePolicy    = "saturate"
	serviceTickMs    = 10
	servicePlacement = "least_loaded"
	servicePin       = true // -pin: gnn pins busy threads (false: cpuset sizing only)
)

// enableScale attaches policy (saturate or gnn) to a daemon with the -placement core hand-out.
// tick_ms is sent for gnn only.
func enableScale(base, id, policy string, minCores, maxCores int) error {
	req := map[string]any{
		"daemon_id": id,
		"policy":    policy,
		"min":       minCores,
		"max":       maxCores,
		"placement": servicePlacement,
	}
	if policy == "gnn" {
		req["tick_ms"] = serviceTickMs
		req["pin"] = servicePin
	}
	status, body, err := post(base, "/scale", req)
	if err != nil {
		return err
	}
	if status != http.StatusOK || !bytes.Contains(body, []byte(`"ok":true`)) {
		return fmt.Errorf("POST /scale %s: %d %s", id, status, bytes.TrimSpace(body))
	}
	return nil
}

func readCPUSet(id string) string {
	data, err := os.ReadFile(cgroupFile(id, "cpuset.cpus"))
	if err != nil {
		return "?"
	}
	text := strings.TrimSpace(string(data))
	if text == "" {
		return "?"
	}
	return text
}

func cpuSetList(lines []tuple, spec func(string) string) string {
	var b strings.Builder
	for i, line := range lines {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(filepath.Base(line.path))
		b.WriteByte('=')
		b.WriteString(spec(line.id))
	}
	return b.String()
}

// svcFixed (-svc-fixed "geo=1,mongod=2"): per-program fixed allocation (/scale min = max), keyed by the
// deployment line's program name (memcached port, mongod, service name). Others use -svc-min/-svc-max.
var svcFixed = map[string]int{}

func parseSvcFixed(s string) error {
	for _, kv := range strings.Split(s, ",") {
		if kv == "" {
			continue
		}
		k, v, ok := strings.Cut(kv, "=")
		n, err := strconv.Atoi(v)
		if !ok || err != nil || n < 1 {
			return fmt.Errorf("-svc-fixed: bad entry %q (want name=cores)", kv)
		}
		svcFixed[k] = n
	}
	return nil
}

func svcRange(line tuple, mn, mx int) (int, int) {
	if k, ok := svcFixed[filepath.Base(line.path)]; ok {
		return k, k
	}
	return mn, mx
}

// idPrefix names this instance's Mercury daemons (<prefix>-geo, <prefix>-loadgen,
// <prefix>-jaeger); -prefix sets it so two hotels can share one Mercury.
var (
	idPrefix  = "hotel"
	loadgenID = "hotel-loadgen"
)

// readyRate (req/s) probes a route under -arrivals poisson until the schedule sets its first level.
const readyRate = 20

// routeSeed gives each route its own reproducible Poisson arrival sequence.
func routeSeed(name string) int64 {
	h := int64(1469598103934665603)
	for _, c := range name {
		h = (h ^ int64(c)) * 1099511628211
	}
	return h
}
