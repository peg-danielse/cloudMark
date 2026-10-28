package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/boot"
	"golang.org/x/sys/unix"
)

// Environment read by bin/loadgen. Mercury execs a daemon with no arguments,
// so everything the generator needs arrives here.
const (
	envRole   = "OPERATE_ROLE"      // "loadgen" selects runLoadgen in main
	envUsers  = "LOADGEN_USERS"     // maximum users per route
	envOut    = "LOADGEN_OUT"       // output file, one fsynced line per write
	envID     = "LOADGEN_ID"        // this daemon's Mercury id, for its own cgroup
	envConfig = "HOTEL_CONFIG"      // config.json path
	envConns  = "LOADGEN_CONNS"     // gRPC connections per route (0 = one per worker)
	envWindow = "LOADGEN_WINDOW_MS" // measurement window per autoscale reading
	envFile   = "LOADGEN_FILE"      // deployment list, for the cpus column
	envMax    = "LOADGEN_MAX"       // saturate max, for gen_bound
	envRoutes = "LOADGEN_ROUTES"    // routes to drive (-routes), comma list; empty = all
	envHist   = "LOADGEN_HIST"      // per-window latency histogram file (-hist); empty = off
	envEntry  = "LOADGEN_ENTRY"     // frontend or services (-entry)
	roleGen   = "loadgen"
	doneLine  = "loadgen done"
	errorLine = "loadgen error"
)

// syncWriter makes every Write durable before it returns, so the orchestrator
// tailing the file (and a crash) never loses a finished line.
type syncWriter struct{ f *os.File }

func (w syncWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	if err != nil {
		return n, err
	}
	return n, w.f.Sync()
}

// runLoadgen is the body of bin/loadgen when Mercury starts it.
func runLoadgen() error {
	outPath := os.Getenv(envOut)
	if outPath == "" {
		return fmt.Errorf("%s is required", envOut)
	}
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := syncWriter{f}
	if err := loadgenMain(w); err != nil {
		fmt.Fprintf(w, "%s: %v\n", errorLine, err)
		return err
	}
	fmt.Fprintln(w, doneLine)
	return nil
}

func loadgenMain(w io.Writer) error {
	users, err := strconv.Atoi(os.Getenv(envUsers))
	if err != nil || users < 1 {
		return fmt.Errorf("%s must be a positive integer, got %q", envUsers, os.Getenv(envUsers))
	}
	if v := os.Getenv(envWindow); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 100 {
			return fmt.Errorf("bad %s %q", envWindow, v)
		}
		windowMs = n
	}
	if v := os.Getenv(envConns); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return fmt.Errorf("bad %s %q", envConns, v)
		}
		connsPerRoute = n
	}
	maxCores := defaultGenMax
	if v := os.Getenv(envMax); v != "" {
		if maxCores, err = strconv.Atoi(v); err != nil || maxCores < 1 {
			return fmt.Errorf("bad %s %q", envMax, v)
		}
	}
	configPath := os.Getenv(envConfig)
	if configPath == "" {
		configPath = root + "/deathstar-hotelReservations/config.json"
	}
	cfg, err := boot.LoadFile(configPath)
	if err != nil {
		return err
	}
	file := os.Getenv(envFile)
	if file == "" {
		file = root + "/mercury-hotel.txt"
	}
	id := os.Getenv(envID)
	if id == "" {
		id = loadgenID
	}
	idPrefix = strings.TrimSuffix(id, "-loadgen") // the operator's -prefix: daemon ids (cgroups) of this instance
	lines, err := parseFile(file)
	if err != nil {
		return err
	}
	if v := os.Getenv(envEntry); v != "" {
		loadEntry = v
	}
	if loadRoutes, err = parseRoutes(os.Getenv(envRoutes)); err != nil {
		return err
	}
	if p := os.Getenv(envHist); p != "" {
		hf, err := os.OpenFile(p, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		defer hf.Close()
		histOut = hf
	}
	traceMarks = os.Getenv(envTraceMarks) == "1"
	if v := os.Getenv(envLoad); v != "" {
		if err := demoLoad.decode(v); err != nil {
			return err
		}
	}
	gen := &genMeter{
		cpus: func() string { return readCPUSet(id) },
		busy: func() (time.Duration, bool) { return cgroupBusy(id) },
		max:  maxCores,
	}
	fmt.Fprintf(w, "loadgen start id=%s pid=%d users=%d conns=%d gomaxprocs=%s cpus=%s\n",
		id, os.Getpid(), users, connsPerRoute, os.Getenv("GOMAXPROCS"), readCPUSet(id))
	return driveRoutes(cfg, users, w, lines, gen)
}

// genMeter reports the load generator's own CPU use per measurement window.
type genMeter struct {
	cpus func() string
	busy func() (time.Duration, bool)
	max  int
}

type genSample struct {
	cpus string
	busy time.Duration
	ok   bool
	at   time.Time
}

func (g *genMeter) begin() genSample {
	busy, ok := g.busy()
	return genSample{cpus: g.cpus(), busy: busy, ok: ok, at: time.Now()}
}

// end formats the gen_* columns for the window that started at s.
// gen_resized: the cpuset changed during the window, so treat it as noisy.
// gen_bound: at max cores and at least 90% busy, so the generator, not the
// services, may be what limits this window.
func (g *genMeter) end(s genSample) string {
	busy, ok := g.busy()
	cpus := g.cpus()
	elapsed := time.Since(s.at)
	busyMs := int64(-1)
	bound := 0
	if ok && s.ok {
		d := busy - s.busy
		busyMs = d.Milliseconds()
		cores := countCPUs(cpus)
		if cores >= g.max && cores > 0 && elapsed > 0 &&
			float64(d)/(float64(elapsed)*float64(cores)) >= 0.90 {
			bound = 1
		}
	}
	resized := 0
	if cpus != s.cpus {
		resized = 1
	}
	return fmt.Sprintf("gen_cpus %s gen_busy_ms %d gen_resized %d gen_bound %d", cpus, busyMs, resized, bound)
}

// countCPUs counts CPUs in a cpuset list such as "60-61,63". Unparsable is 0.
func countCPUs(spec string) int {
	spec = strings.TrimSpace(spec)
	if spec == "" || spec == "?" {
		return 0
	}
	n := 0
	for _, part := range strings.Split(spec, ",") {
		lo, hi, isRange := strings.Cut(part, "-")
		a, err := strconv.Atoi(lo)
		if err != nil {
			return 0
		}
		b := a
		if isRange {
			if b, err = strconv.Atoi(hi); err != nil || b < a {
				return 0
			}
		}
		n += b - a + 1
	}
	return n
}

// cgroupBusy is usage_usec from the daemon's cgroup cpu.stat.
func cgroupBusy(id string) (time.Duration, bool) {
	data, err := os.ReadFile(cgroupFile(id, "cpu.stat"))
	if err != nil {
		return 0, false
	}
	return parseUsage(string(data))
}

func parseUsage(stat string) (time.Duration, bool) {
	for _, line := range strings.Split(stat, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "usage_usec" {
			us, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return 0, false
			}
			return time.Duration(us) * time.Microsecond, true
		}
	}
	return 0, false
}

func inprocCPUs() string {
	return fmt.Sprintf("%d-%d", clientFirstCPU, clientLastCPU)
}

// selfBusy is this process's user plus system time.
func selfBusy() (time.Duration, bool) {
	var ru unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &ru); err != nil {
		return 0, false
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano()), true
}

// runMercuryLoadgen starts bin/loadgen under Mercury, gives it a home CPU and
// the saturate policy, and copies its output to stdout until it finishes.
func runMercuryLoadgen(base string, lines []tuple, maxUsers int, opts clientOpts, track func(tuple), w io.Writer) error {
	if st, err := os.Stat(opts.bin); err != nil || st.Mode()&0o111 == 0 {
		return fmt.Errorf("loadgen binary %s is missing or not executable (go build -o bin/loadgen ./operate)", opts.bin)
	}
	out := opts.out
	if out == "" {
		out = defaultGenOut()
	}
	env := map[string]string{
		envRole:      roleGen,
		envUsers:     strconv.Itoa(maxUsers),
		envOut:       out,
		envID:        loadgenID,
		envConfig:    opts.config,
		envConns:     strconv.Itoa(opts.conns),
		envWindow:    strconv.Itoa(windowMs),
		envMax:       strconv.Itoa(opts.genMax),
		envRoutes:    strings.Join(loadRoutes, ","),
		envEntry:     loadEntry,
		envHist:      histPath,
		"GOMAXPROCS": strconv.Itoa(opts.genProcs),
	}
	if tracing.enabled() {
		env[envTraceMarks] = "1"
	}
	if demoLoad.needsEnv() {
		env[envLoad] = demoLoad.encode()
	}
	if v := os.Getenv(envCgroupRoot); v != "" {
		env[envCgroupRoot] = v
	}
	env["DSB_ROOT"] = root // Mercury passes only explicit env: the loadgen needs the same root
	gen := tuple{path: opts.bin, hostname: "localhost", id: loadgenID}
	pid, err := start(base, gen, opts.homeCPU, env)
	if err != nil {
		return fmt.Errorf("%s: %w", loadgenID, err)
	}
	track(gen)
	fmt.Printf("started loadgen daemon_id=%s pid %d %s\n", loadgenID, pid, formatEnv(env))
	if err := setCpus(base, loadgenID, strconv.Itoa(opts.homeCPU)); err != nil {
		return err
	}
	if err := enableSaturate(base, loadgenID, opts.genMin, opts.genMax); err != nil {
		return err
	}
	fmt.Printf("policy loadgen saturate min %d max %d home %d out %s\n", opts.genMin, opts.genMax, opts.homeCPU, out)
	return tailLoadgen(out, func() bool { return processAlive(pid) }, w, 100*time.Millisecond)
}

// histPath is the -hist file (<gen-out without .out>.hist.jsonl), empty when -hist is off.
var histPath string

func defaultGenOut() string {
	return filepath.Join(root, "deathstar-hotelReservations", "output",
		fmt.Sprintf("loadgen-%d.out", time.Now().Unix()))
}

func processAlive(pid int) bool {
	_, err := os.Stat("/proc/" + strconv.Itoa(pid))
	return err == nil
}

// tailLoadgen copies complete lines from path to w until it sees doneLine
// (nil) or errorLine (error), or until alive reports false and no more data
// arrives (error).
func tailLoadgen(path string, alive func() bool, w io.Writer, poll time.Duration) error {
	var f *os.File
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		if f, err = os.Open(path); err == nil {
			break
		}
		if !alive() || time.Now().After(deadline) {
			return fmt.Errorf("loadgen output %s never appeared: %v", path, err)
		}
		time.Sleep(poll)
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var partial strings.Builder
	dead := false
	for {
		chunk, err := r.ReadString('\n')
		partial.WriteString(chunk)
		if err == nil {
			line := partial.String()
			partial.Reset()
			if _, werr := io.WriteString(w, line); werr != nil {
				return werr
			}
			trimmed := strings.TrimSpace(line)
			if trimmed == doneLine {
				return nil
			}
			if strings.HasPrefix(trimmed, errorLine) {
				return errors.New(trimmed)
			}
			continue
		}
		if err != io.EOF {
			return err
		}
		if dead {
			return fmt.Errorf("loadgen exited before %q", doneLine)
		}
		if !alive() {
			dead = true // read once more to drain what it wrote before exiting
			continue
		}
		time.Sleep(poll)
	}
}
