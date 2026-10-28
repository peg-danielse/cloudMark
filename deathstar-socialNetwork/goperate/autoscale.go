package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// svcMinCores/svcMaxCores: per-service /scale min and max (-svc-min, -svc-max; default 1 and
// autoMaxCores). min = max gives a fixed allocation (gnn-slo-v1 allocation sweep). Thread caps
// (daemonEnv) stay at autoMaxCores so the services are the same in every sweep run.
var svcMinCores, svcMaxCores = 1, autoMaxCores

// root is the benchmarks checkout (DSB_ROOT, else the tree the binary is in); snRoot is its social tree.
var (
	root   = benchmarksRoot()
	snRoot = socialRoot()
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

const (
	autoMaxCores = 16
	autoWindows  = 4
	scaleMeasure = time.Second

	// Linux CPU 0 is Mercury. Homes begin at CPU 1, one per line of
	// mercury-social.txt. The in-process client keeps CPUs 60-63.
	homeFirstLinux = 1
	clientFirstCPU = 60
	clientLastCPU  = 63

	clientMercury = "mercury"
	clientInproc  = "inproc"
	loadgenID     = "social-loadgen"

	defaultGenCPU   = 60
	defaultGenMin   = 2
	defaultGenMax   = 8
	defaultGenProcs = 8
)

// benchmarksRoot is this repository's benchmarks directory: DSB_ROOT, else the
// directory above the built binary, else the source tree (go test / go run).
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

func socialRoot() string {
	return filepath.Join(benchmarksRoot(), "deathstar-socialNetwork")
}

func resolveExec(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(benchmarksRoot(), path)
}

// clientOpts chooses how autoscale drives load; see hotel operate for the
// same design. mercury: bin/loadgen as Mercury daemon social-loadgen.
// inproc: this process pinned to CPUs 60-63.
type clientOpts struct {
	mode     string
	bin      string
	out      string
	config   string
	file     string
	graph    string
	routes   string
	mix      string
	homeCPU  int
	genMin   int
	genMax   int
	genProcs int
	conns    int
}

func clientCPUs() []int {
	cpus := make([]int, 0, clientLastCPU-clientFirstCPU+1)
	for cpu := clientFirstCPU; cpu <= clientLastCPU; cpu++ {
		cpus = append(cpus, cpu)
	}
	return cpus
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

// autoscaleServices puts every daemon on its own home core, attaches
// saturate 1..16, and drives the social-network routes together from 1 user
// to maxUsers and back down.
func autoscaleServices(base string, cfg serviceConfig, lines []tuple, maxUsers int, opts clientOpts, track func(tuple)) error {
	maxUsers = demoLoad.workers(maxUsers)
	homes, err := daemonHomes(len(lines))
	if err != nil {
		return err
	}
	names, err := parseRoutes(opts.routes)
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
		pol := servicePolicy
		if saturateOnly[filepath.Base(line.path)] {
			pol = "saturate" // -saturate-only: e.g. too many threads for gnn's per-tick read
		}
		mn, mx := svcRange(line, svcMinCores, svcMaxCores)
		if err := enableScale(base, line.id, pol, mn, mx); err != nil {
			return err
		}
		fmt.Printf(" %s=%s", filepath.Base(line.path), spec)
	}
	fmt.Println()
	fmt.Printf("policy %s placement %s tick_ms %d min %d max %d users 1-%d routes %s mix %s graph %s\n",
		servicePolicy, servicePlacement, serviceTickMs, svcMinCores, svcMaxCores, maxUsers, opts.routes, orDefault(opts.mix, "equal"), opts.graph)

	fmt.Println(demoLoad.describe())
	closeWindows, err := startWindowLog(opts.out, socialOutputDir())
	if err != nil {
		return err
	}
	defer closeWindows()
	out, finishTraces, err := beginTraceAnalysis()
	if err != nil {
		return err
	}
	defer finishTraces()
	if opts.mode == clientInproc {
		w, err := loadGraph(opts.graph)
		if err != nil {
			return err
		}
		if w.mix, err = parseMix(opts.mix, names); err != nil {
			return err
		}
		traceMarks = tracing.enabled()
		gen := &genMeter{cpus: inprocCPUs, busy: selfBusy, max: len(clientCPUs())}
		return driveRoutes(cfg, names, maxUsers, w, out, lines, gen)
	}
	return runMercuryLoadgen(base, maxUsers, opts, track, out)
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

// driveRoutes ramps every route together and prints one line per route per
// window, in the hotel autoscaleN.out format: route, clients (this route's
// active users), arrival_rate, service_rate, avg_us, p95_us, p99_us, errors, cpus,
// then the gen_* columns.
// loadScale is the factor of an application-load antagonist (Mercury's src/test/interfere.py, kind load): the number
// in loadgen.scale next to the -hist file, read at every window, so a phase takes effect within one window. 1 when
// the file is missing or not a positive number; the hist and route lines report the scaled level.
func loadScale() float64 {
	hist := os.Getenv(envHist)
	if hist == "" {
		return 1
	}
	b, err := os.ReadFile(filepath.Join(filepath.Dir(hist), "loadgen.scale"))
	if err != nil {
		return 1
	}
	s, err := strconv.ParseFloat(strings.TrimSpace(string(b)), 64)
	if err != nil || s <= 0 {
		return 1
	}
	return s
}

func scaled(level int, s float64) int {
	if v := int(float64(level)*s + 0.5); v > 1 {
		return v
	}
	return 1
}

func driveRoutes(cfg serviceConfig, names []string, maxUsers int, wl *workload, w io.Writer, lines []tuple, gen *genMeter) error {
	routes, err := openRoutes(cfg, names, maxUsers, wl)
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
		level, base := lv.level, lv.clients
		clients, scale := base, 0.0
		levelStart := time.Now()
		for window := 0; lv.hold > 0 && time.Since(levelStart) < lv.hold || lv.hold == 0 && window < lv.windows; window++ {
			if s := loadScale(); s != scale { // an application-load antagonist scales the level (loadScale)
				scale, clients = s, scaled(base, s)
				for _, route := range routes {
					route.rec.setLevel(routeUsers(clients, route.weight))
				}
			}
			g0 := gen.begin()
			t0 := time.Now()
			readings, diffs, m0, m1 := measureRoutes(routes)
			t1 := time.Now()
			gw := gen.end(g0)
			cpus := cpuSetList(lines, readCPUSet)
			marks.emit(w, level, window, clients, t0, t1) // -jaeger only: window marker for the trace analyser
			writeHist(routes, diffs, level, window, func(r liveRoute) int { return routeUsers(clients, r.weight) }, m0, m1)
			for i, route := range routes {
				reading := readings[i]
				fmt.Fprintf(w, "route %s clients %d arrival_rate %d service_rate %d avg_us %.1f p95_us %.1f p99_us %.1f errors %d cpus %s %s%s\n",
					route.name, routeUsers(clients, route.weight), reading.arrivalRate, reading.serviceRate,
					reading.avgUs, reading.p95Us, reading.p99Us, reading.errors, cpus, gw, lv.suffix())
			}
		}
	}
	return nil
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
