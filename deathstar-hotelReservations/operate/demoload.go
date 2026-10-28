package main

// Demo load shapes and per-window logging. This file is identical in hotel
// operate and social goperate.
//
// -load ramp (default) keeps the original schedule: 1 user doubling to -users
// and back, autoWindows windows per level, unchanged output.
//
// -load sine drives every route with a sine of users between -users-min and
// -users-max:
//
//	users(t) = mid + amp*sin(2*pi*(t-epoch)/period + phase)
//
// where mid and amp follow from the bounds, t is the wall clock and epoch is
// -sine-epoch-ms (0: when the load starts). Phase 0 starts at the midpoint
// going up, 180 at the midpoint going down, so two operators sharing an epoch
// with -phase 0 and -phase 180 push load from one app to the other. The user
// count changes every -step; each step is round(step/window) measurement
// windows. -ramp-in blends linearly from 1 user to the sine over its length.
// The load stops after -duration (0: -ramp-in plus one -period). Route lines
// in sine mode end with "load sine step S target_users N t_s T".
//
// With -load sine, -load-csv or -status-file the route lines also pass
// through windowLog. It writes one CSV row per route per window, prints a
// "demo app ..." summary line per window and rewrites the status file that
// speculator --status shows live.

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	loadRamp      = "ramp"
	loadSine      = "sine"
	loadList      = "list" // -levels a,b,c: each held for -step
	arrClosed     = "closed"
	arrPoisson    = "poisson"
	envLoad       = "LOADGEN_LOAD"        // sine schedule for bin/loadgen (loadOpts.encode)
	envCgroupRoot = "MERCURY_CGROUP_ROOT" // Mercury's cgroup directory (fake_mercury.py uses a user-space one)
)

// cgroupRoot holds one directory per Mercury daemon id with cpuset.cpus and cpu.stat.
var cgroupRoot = envOr(envCgroupRoot, "/sys/fs/cgroup/mercury")

func cgroupFile(id, name string) string { return filepath.Join(cgroupRoot, id, name) }

type loadOpts struct {
	mode                string
	usersMin, usersMax  int
	usersMean, usersAmp int
	period, step        time.Duration
	duration, rampIn    time.Duration
	phase               float64
	epochMs             int64
	csv, status         string
	homes               string
	app                 string
	// Arrivals: closed (each of N workers sends its next request when the last returns; levels are users) or
	// poisson (open loop, exponential gaps; levels are requests per second per route). -levels for -load list.
	arrivals         string
	levelList        string
	levelVals        []int
	rateMin, rateMax int
	inflight         int
	reqTimeout       time.Duration
}

func (o *loadOpts) poisson() bool { return o.arrivals == arrPoisson }

// needsEnv: the schedule bin/loadgen must receive in LOADGEN_LOAD (anything but the default closed ramp).
func (o *loadOpts) needsEnv() bool { return o.sine() || o.mode == loadList || o.poisson() }

// timeout per request: -req-timeout, else 10 s closed (as before) and 2 s open loop.
func (o *loadOpts) timeout() time.Duration {
	if o.reqTimeout > 0 {
		return o.reqTimeout
	}
	if o.poisson() {
		return 2 * time.Second
	}
	return 10 * time.Second
}

// demoLoad is the active schedule: flags in the operator, LOADGEN_LOAD in bin/loadgen.
var demoLoad = &loadOpts{mode: loadRamp}

// appOut is where autoscale route lines go before the trace analyser: stdout, or a windowLog.
var appOut io.Writer = os.Stdout

func addLoadFlags(app string) *loadOpts {
	o := &loadOpts{app: app}
	flag.StringVar(&o.mode, "load", loadRamp, "autoscale load shape: ramp (1 to -users and back, doubling) or sine (-users-min..-users-max)")
	flag.IntVar(&o.usersMin, "users-min", 50, "-load sine: lowest users per route")
	flag.IntVar(&o.usersMax, "users-max", 150, "-load sine: highest users per route (also the worker pool size)")
	flag.IntVar(&o.usersMean, "users-mean", 0, "-load sine: mean users per route; with -users-amp replaces -users-min/-users-max (0 = use min/max)")
	flag.IntVar(&o.usersAmp, "users-amp", 0, "-load sine: amplitude around -users-mean (0 = half the mean, the 1:2 ratio of 50-150)")
	flag.DurationVar(&o.period, "period", 10*time.Minute, "-load sine: sine period")
	flag.Float64Var(&o.phase, "phase", 0, "-load sine: phase in degrees (0 = midpoint rising, 90 = max, 180 = midpoint falling, 270 = min)")
	flag.DurationVar(&o.step, "step", 5*time.Second, "-load sine: how often the user count changes (>= -window-ms)")
	flag.DurationVar(&o.duration, "duration", 0, "-load sine: total load time including -ramp-in (0 = ramp-in + one period)")
	flag.DurationVar(&o.rampIn, "ramp-in", 0, "-load sine: ramp linearly from 1 user to the sine over this long")
	flag.Int64Var(&o.epochMs, "sine-epoch-ms", 0, "-load sine: unix ms of phase 0; give two operators the same value so their phases line up (0 = when the load starts)")
	flag.StringVar(&o.csv, "load-csv", "", "per-route per-window CSV (default with -load sine: <gen-out stem>.windows.csv; - = off)")
	flag.StringVar(&o.status, "status-file", "", "rewrite this file with the latest window summary (speculator --status)")
	flag.StringVar(&o.arrivals, "arrivals", arrClosed, "closed (N users, each sends when its last request returns) or poisson (open loop: Poisson arrivals; levels are req/s per route)")
	flag.StringVar(&o.levelList, "levels", "", "-load list: comma-separated levels (users, or req/s with -arrivals poisson), each held for -step")
	flag.IntVar(&o.rateMin, "rate-min", 0, "-load sine -arrivals poisson: lowest req/s per route (instead of -users-min)")
	flag.IntVar(&o.rateMax, "rate-max", 0, "-load sine -arrivals poisson: highest req/s per route (instead of -users-max)")
	flag.IntVar(&o.inflight, "inflight", 4096, "-arrivals poisson: requests in flight per route; an arrival beyond it counts as an error")
	flag.DurationVar(&o.reqTimeout, "req-timeout", 0, "per-request timeout, counted as an error (0: 10s closed, 2s poisson)")
	flag.StringVar(&o.homes, "homes", "", "home CPUs for the service daemons, e.g. 1-15 (round-robin when shorter than the deployment; empty = from CPU 1)")
	demoLoad = o
	return o
}

func (o *loadOpts) sine() bool { return o.mode == loadSine }

func (o *loadOpts) validate(windowMs int) error {
	if o.mode != loadRamp && o.mode != loadSine && o.mode != loadList {
		return fmt.Errorf("-load must be %s, %s or %s", loadRamp, loadSine, loadList)
	}
	if o.arrivals == "" {
		o.arrivals = arrClosed
	}
	if o.arrivals != arrClosed && o.arrivals != arrPoisson {
		return fmt.Errorf("-arrivals must be %s or %s", arrClosed, arrPoisson)
	}
	if o.poisson() && o.mode == loadRamp {
		return errors.New("-arrivals poisson needs -load list or -load sine (the ramp's 1, 2, 4, ... are user counts)")
	}
	if o.poisson() && o.inflight < 1 {
		return errors.New("-inflight must be >= 1")
	}
	if o.mode == loadList {
		o.levelVals = nil
		for _, f := range strings.Split(o.levelList, ",") {
			v, err := strconv.Atoi(strings.TrimSpace(f))
			if err != nil || v < 1 {
				return fmt.Errorf("-levels: %q is not a positive integer", f)
			}
			o.levelVals = append(o.levelVals, v)
		}
		if o.step < time.Duration(windowMs)*time.Millisecond {
			return fmt.Errorf("-step %s is shorter than -window-ms %d", o.step, windowMs)
		}
		return nil
	}
	if o.poisson() && o.sine() && o.rateMax > 0 {
		o.usersMin, o.usersMax = o.rateMin, o.rateMax
	}
	if o.homes != "" {
		if _, err := parseCPUList(o.homes); err != nil {
			return fmt.Errorf("-homes: %w", err)
		}
	}
	if !o.sine() {
		return nil
	}
	if o.usersMean > 0 {
		amp := o.usersAmp
		if amp == 0 {
			amp = o.usersMean / 2
		}
		o.usersMin, o.usersMax = o.usersMean-amp, o.usersMean+amp
	} else if o.usersAmp != 0 {
		return errors.New("-users-amp needs -users-mean")
	}
	window := time.Duration(windowMs) * time.Millisecond
	switch {
	case o.usersMin < 1 || o.usersMax < o.usersMin:
		return errors.New("-load sine needs 1 <= -users-min <= -users-max (or -users-amp < -users-mean)")
	case o.step < window:
		return fmt.Errorf("-step %s is shorter than -window-ms %d", o.step, windowMs)
	case o.period < 2*o.step:
		return fmt.Errorf("-period %s needs at least two -step %s", o.period, o.step)
	case o.duration < 0 || o.rampIn < 0:
		return errors.New("-duration and -ramp-in must be >= 0")
	case o.duration > 0 && o.duration < o.step:
		return errors.New("-duration is shorter than one -step")
	}
	return nil
}

// total is how long the sine load runs.
func (o *loadOpts) total() time.Duration {
	if o.duration > 0 {
		return o.duration
	}
	return o.rampIn + o.period
}

// workers is the per-route worker pool: -users for ramp, -users-max for sine.
func (o *loadOpts) workers(users int) int {
	if o.sine() {
		return o.usersMax
	}
	return users
}

// usersAt is the target user count elapsed after the load started and sinceEpoch after phase 0.
func (o *loadOpts) usersAt(elapsed, sinceEpoch time.Duration) int {
	mid := float64(o.usersMin+o.usersMax) / 2
	amp := float64(o.usersMax-o.usersMin) / 2
	x := 2*math.Pi*sinceEpoch.Seconds()/o.period.Seconds() + o.phase*math.Pi/180
	u := mid + amp*math.Sin(x)
	if o.rampIn > 0 && elapsed < o.rampIn {
		u = 1 + (u-1)*elapsed.Seconds()/o.rampIn.Seconds()
	}
	n := int(math.Round(u))
	if n < 1 {
		n = 1
	}
	if n > o.usersMax {
		n = o.usersMax
	}
	return n
}

func (o *loadOpts) describe() string {
	if !o.sine() {
		return "load ramp"
	}
	return fmt.Sprintf("load sine users %d-%d period %s phase %g step %s duration %s ramp_in %s epoch_ms %d",
		o.usersMin, o.usersMax, o.period, o.phase, o.step, o.total(), o.rampIn, o.epochMs)
}

// encode/decode carry the sine schedule to bin/loadgen (LOADGEN_LOAD).
// encode: "v2|mode|min|max|period ms|step ms|duration ms|ramp-in ms|phase|epoch ms|arrivals|inflight|timeout ms|levels".
func (o *loadOpts) encode() string {
	lv := make([]string, len(o.levelVals))
	for i, v := range o.levelVals {
		lv[i] = strconv.Itoa(v)
	}
	return fmt.Sprintf("v2|%s|%d|%d|%d|%d|%d|%d|%g|%d|%s|%d|%d|%s", o.mode, o.usersMin, o.usersMax, o.period.Milliseconds(),
		o.step.Milliseconds(), o.duration.Milliseconds(), o.rampIn.Milliseconds(), o.phase, o.epochMs, o.arrivals,
		o.inflight, o.reqTimeout.Milliseconds(), strings.Join(lv, ","))
}

func (o *loadOpts) decode(s string) error {
	if strings.HasPrefix(s, "v2|") {
		f := strings.Split(s, "|")
		if len(f) != 14 {
			return fmt.Errorf("bad %s %q", envLoad, s)
		}
		var n [13]int64
		for i := 2; i <= 12; i++ {
			if i == 8 || i == 10 {
				continue
			}
			v, err := strconv.ParseInt(f[i], 10, 64)
			if err != nil {
				return fmt.Errorf("bad %s %q", envLoad, s)
			}
			n[i] = v
		}
		phase, err := strconv.ParseFloat(f[8], 64)
		if err != nil {
			return fmt.Errorf("bad %s %q", envLoad, s)
		}
		ms := time.Millisecond
		o.mode, o.usersMin, o.usersMax = f[1], int(n[2]), int(n[3])
		o.period, o.step, o.duration, o.rampIn = time.Duration(n[4])*ms, time.Duration(n[5])*ms, time.Duration(n[6])*ms, time.Duration(n[7])*ms
		o.phase, o.epochMs, o.arrivals, o.inflight, o.reqTimeout = phase, n[9], f[10], int(n[11]), time.Duration(n[12])*ms
		o.levelVals = nil
		if f[13] != "" {
			for _, x := range strings.Split(f[13], ",") {
				v, err := strconv.Atoi(x)
				if err != nil {
					return fmt.Errorf("bad %s %q", envLoad, s)
				}
				o.levelVals = append(o.levelVals, v)
			}
		}
		return nil
	}
	f := strings.Split(s, ",")
	if len(f) != 9 || f[0] != loadSine {
		return fmt.Errorf("bad %s %q", envLoad, s)
	}
	var n [8]int64
	for i := 1; i < 9; i++ {
		if i == 7 {
			continue
		}
		v, err := strconv.ParseInt(f[i], 10, 64)
		if err != nil {
			return fmt.Errorf("bad %s %q", envLoad, s)
		}
		n[i-1] = v
	}
	phase, err := strconv.ParseFloat(f[7], 64)
	if err != nil {
		return fmt.Errorf("bad %s %q", envLoad, s)
	}
	ms := time.Millisecond
	o.mode, o.usersMin, o.usersMax = loadSine, int(n[0]), int(n[1])
	o.period, o.step, o.duration, o.rampIn = time.Duration(n[2])*ms, time.Duration(n[3])*ms, time.Duration(n[4])*ms, time.Duration(n[5])*ms
	o.phase, o.epochMs = phase, n[7]
	return nil
}

type loadLevel struct {
	level, clients, windows int
	elapsed                 time.Duration
	sine                    bool
	// hold: when set, the level lasts this long by the clock instead of windows windows. -load list sets it to
	// -step: the bookkeeping between windows grows with the number of routes (about 60 ms each on dtn-01), so with
	// seven routes 1320 windows of 500 ms took 1205 s instead of 660 s.
	hold time.Duration
}

// suffix ends each route line of a sine level; ramp lines keep their old format.
func (l loadLevel) suffix() string {
	if !l.sine {
		return ""
	}
	return fmt.Sprintf(" load sine step %d target_users %d t_s %.1f", l.level, l.clients, l.elapsed.Seconds())
}

// levels returns the load plan one level at a time. Sine levels are computed
// from the clock when asked for, so a slow window stretches a step rather
// than shifting the phase.
func (o *loadOpts) levels(maxUsers, windowMs int, now func() time.Time) func() (loadLevel, bool) {
	if o.mode == loadList {
		per := int(math.Round(float64(o.step) / float64(time.Duration(windowMs)*time.Millisecond)))
		if per < 1 {
			per = 1
		}
		i := 0
		return func() (loadLevel, bool) {
			if i >= len(o.levelVals) {
				return loadLevel{}, false
			}
			l := loadLevel{level: i, clients: o.levelVals[i], windows: per, hold: o.step}
			i++
			return l, true
		}
	}
	if !o.sine() {
		ramp := rampClients(maxUsers)
		i := 0
		return func() (loadLevel, bool) {
			if i >= len(ramp) {
				return loadLevel{}, false
			}
			l := loadLevel{level: i, clients: ramp[i], windows: autoWindows}
			i++
			return l, true
		}
	}
	start := now()
	epoch := start
	if o.epochMs > 0 {
		epoch = time.Unix(0, o.epochMs*int64(time.Millisecond))
	}
	per := int(math.Round(float64(o.step) / float64(time.Duration(windowMs)*time.Millisecond)))
	if per < 1 {
		per = 1
	}
	total := o.total()
	k := 0
	return func() (loadLevel, bool) {
		t := now()
		elapsed := t.Sub(start)
		if elapsed >= total {
			return loadLevel{}, false
		}
		l := loadLevel{level: k, clients: o.usersAt(elapsed, t.Sub(epoch)), windows: per, elapsed: elapsed, sine: true}
		k++
		return l, true
	}
}

// parseCPUList parses "1-15,20,22-23".
func parseCPUList(spec string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(strings.TrimSpace(spec), ",") {
		if part == "" {
			continue
		}
		lo, hi, isRange := strings.Cut(part, "-")
		a, err := strconv.Atoi(lo)
		if err != nil || a < 0 {
			return nil, fmt.Errorf("bad cpu list %q", spec)
		}
		b := a
		if isRange {
			if b, err = strconv.Atoi(hi); err != nil || b < a {
				return nil, fmt.Errorf("bad cpu list %q", spec)
			}
		}
		for c := a; c <= b; c++ {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty cpu list %q", spec)
	}
	return out, nil
}

// listHomes hands out the -homes CPUs to n daemons, round-robin.
func listHomes(spec string, n int) ([]int, error) {
	cpus, err := parseCPUList(spec)
	if err != nil {
		return nil, err
	}
	homes := make([]int, n)
	for i := range homes {
		homes[i] = cpus[i%len(cpus)]
	}
	return homes, nil
}

// windowCSV is the default -load-csv next to the loadgen output.
func windowCSV(genOut, outDir string) string {
	if demoLoad.csv == "-" {
		return ""
	}
	if demoLoad.csv != "" {
		return demoLoad.csv
	}
	if !demoLoad.sine() {
		return ""
	}
	if genOut != "" {
		return strings.TrimSuffix(genOut, filepath.Ext(genOut)) + ".windows.csv"
	}
	return filepath.Join(outDir, fmt.Sprintf("windows-%d.csv", time.Now().Unix()))
}

// startWindowLog puts a windowLog in front of stdout when -load sine,
// -load-csv or -status-file asks for it; close flushes the last window.
func startWindowLog(genOut, outDir string) (func(), error) {
	csvPath := windowCSV(genOut, outDir)
	if csvPath == "" && demoLoad.status == "" {
		return func() {}, nil
	}
	wl, err := newWindowLog(demoLoad.app, os.Stdout, csvPath, demoLoad.status)
	if err != nil {
		return nil, err
	}
	appOut = wl
	fmt.Printf("windows csv %s status %s\n", orNone(csvPath), orNone(demoLoad.status))
	return func() { wl.Close(); appOut = os.Stdout }, nil
}

func orNone(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// routeWin is one route line.
type routeWin struct {
	name                  string
	clients, target, step int
	tS                    float64
	sine                  bool
	arrival, service, err int64
	avg, p95, p99         float64
	cores, coreSum, gen   int
}

// parseRouteLine reads "route NAME clients N ... cpus a=1 b=2-3 gen_cpus 60 ... [load sine step S target_users N t_s T]".
func parseRouteLine(line string) (routeWin, bool) {
	f := strings.Fields(line)
	if len(f) < 2 || f[0] != "route" {
		return routeWin{}, false
	}
	r := routeWin{name: f[1], target: -1}
	union := map[int]bool{}
	num := func(s string) float64 { v, _ := strconv.ParseFloat(s, 64); return v }
	for i := 2; i < len(f); i++ {
		key := f[i]
		if key == "cpus" {
			for i+1 < len(f) && strings.Contains(f[i+1], "=") {
				i++
				_, spec, _ := strings.Cut(f[i], "=")
				if cpus, err := parseCPUList(spec); err == nil {
					r.coreSum += len(cpus)
					for _, c := range cpus {
						union[c] = true
					}
				}
			}
			continue
		}
		if i+1 >= len(f) {
			break
		}
		val := f[i+1]
		switch key {
		case "clients":
			r.clients = int(num(val))
		case "arrival_rate":
			r.arrival = int64(num(val))
		case "service_rate":
			r.service = int64(num(val))
		case "avg_us":
			r.avg = num(val)
		case "p95_us":
			r.p95 = num(val)
		case "p99_us":
			r.p99 = num(val)
		case "errors":
			r.err = int64(num(val))
		case "gen_cpus":
			if cpus, err := parseCPUList(val); err == nil {
				r.gen = len(cpus)
			}
		case "load":
			r.sine = val == loadSine
		case "step":
			r.step = int(num(val))
		case "target_users":
			r.target = int(num(val))
		case "t_s":
			r.tS = num(val)
		default:
			continue
		}
		i++
	}
	r.cores = len(union)
	if r.target < 0 {
		r.target = r.clients
	}
	return r, true
}

// windowLog passes lines through to w and summarises each window of route
// lines (a window ends when a route repeats, or at "loadgen done"/Close).
type windowLog struct {
	mu      sync.Mutex
	w       io.Writer
	app     string
	csv     *os.File
	status  string
	partial []byte
	cur     []routeWin
	window  int
	cps     map[string]string
	now     func() time.Time
}

const windowCSVHeader = "unix_ms,app,window,step,t_s,target_users,route,clients,arrival_rate,service_rate,avg_us,p95_us,p99_us,errors,app_cores,core_sum,gen_cores\n"

func newWindowLog(app string, w io.Writer, csvPath, status string) (*windowLog, error) {
	wl := &windowLog{w: w, app: app, status: status, cps: map[string]string{}, now: time.Now}
	if csvPath != "" {
		if err := os.MkdirAll(filepath.Dir(csvPath), 0o755); err != nil {
			return nil, err
		}
		f, err := os.Create(csvPath)
		if err != nil {
			return nil, err
		}
		if _, err := f.WriteString(windowCSVHeader); err != nil {
			f.Close()
			return nil, err
		}
		wl.csv = f
	}
	return wl, nil
}

func (wl *windowLog) Write(p []byte) (int, error) {
	wl.mu.Lock()
	defer wl.mu.Unlock()
	wl.partial = append(wl.partial, p...)
	for {
		at := strings.IndexByte(string(wl.partial), '\n')
		if at < 0 {
			break
		}
		line := string(wl.partial[:at+1])
		wl.partial = wl.partial[at+1:]
		if _, err := io.WriteString(wl.w, line); err != nil {
			return len(p), err
		}
		wl.handle(strings.TrimSpace(line))
	}
	return len(p), nil
}

func (wl *windowLog) handle(line string) {
	switch {
	case strings.HasPrefix(line, "route "):
		r, ok := parseRouteLine(line)
		if !ok {
			return
		}
		for _, c := range wl.cur {
			if c.name == r.name {
				wl.flush()
				break
			}
		}
		wl.cur = append(wl.cur, r)
	case strings.HasPrefix(line, "cp "):
		// cp window W clients C route R samples N root_p95_ms X d=share ...
		f := strings.Fields(line)
		route, shares := "", []string{}
		for i := 1; i < len(f); i++ {
			if f[i] == "route" && i+1 < len(f) {
				route = f[i+1]
			}
			if strings.Contains(f[i], "=") && len(shares) < 4 {
				shares = append(shares, f[i])
			}
		}
		if route != "" {
			wl.cps[route] = strings.Join(shares, " ")
		}
	case line == doneLine:
		wl.flush()
	}
}

func (wl *windowLog) flush() {
	if len(wl.cur) == 0 {
		return
	}
	wl.window++
	now := wl.now()
	first := wl.cur[0]
	var rate, arrival, errs int64
	var p95max, wsum float64
	worst := first.name
	target := 0
	for _, r := range wl.cur {
		rate += r.service
		arrival += r.arrival
		errs += r.err
		wsum += r.p95 * float64(r.service)
		if r.p95 > p95max {
			p95max, worst = r.p95, r.name
		}
		if r.target > target {
			target = r.target
		}
		if wl.csv != nil {
			fmt.Fprintf(wl.csv, "%d,%s,%d,%d,%.1f,%d,%s,%d,%d,%d,%.1f,%.1f,%.1f,%d,%d,%d,%d\n",
				now.UnixMilli(), wl.app, wl.window, r.step, r.tS, r.target, r.name, r.clients, r.arrival, r.service,
				r.avg, r.p95, r.p99, r.err, r.cores, r.coreSum, r.gen)
		}
	}
	wmean := 0.0
	if rate > 0 {
		wmean = wsum / float64(rate)
	}
	fmt.Fprintf(wl.w, "demo app %s window %d t_s %.1f target_users %d rate %d p95_ms_max %.2f %s p95_ms_wmean %.2f errors %d cores %d core_sum %d gen_cores %d\n",
		wl.app, wl.window, first.tS, target, rate, p95max/1000, worst, wmean/1000, errs, first.cores, first.coreSum, first.gen)
	if wl.status != "" {
		var b strings.Builder
		fmt.Fprintf(&b, "app %s\nunix_ms %d\nwindow %d\nt_s %.1f\ntarget_users %d\nrate %d\narrival %d\np95_ms_max %.2f %s\np95_ms_wmean %.2f\nerrors %d\ncores %d\ncore_sum %d\ngen_cores %d\n",
			wl.app, now.UnixMilli(), wl.window, first.tS, target, rate, arrival, p95max/1000, worst, wmean/1000, errs, first.cores, first.coreSum, first.gen)
		routes := make([]string, 0, len(wl.cps))
		for r := range wl.cps {
			routes = append(routes, r)
		}
		sort.Strings(routes)
		for _, r := range routes {
			fmt.Fprintf(&b, "cp %s %s\n", r, wl.cps[r])
		}
		tmp := wl.status + ".tmp"
		if os.WriteFile(tmp, []byte(b.String()), 0o644) == nil {
			_ = os.Rename(tmp, wl.status)
		}
	}
	wl.cur = wl.cur[:0]
}

// Close flushes the last window and closes the CSV.
func (wl *windowLog) Close() error {
	wl.mu.Lock()
	defer wl.mu.Unlock()
	if len(wl.partial) > 0 {
		_, _ = wl.w.Write(wl.partial)
		wl.partial = nil
	}
	wl.flush()
	if wl.csv != nil {
		err := wl.csv.Close()
		wl.csv = nil
		return err
	}
	return nil
}
