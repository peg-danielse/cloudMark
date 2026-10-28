package main

// The media load generator. Each route in -routes (routes.go) gets its own arrival stream and its own histogram. It
// runs either in this process (-client inproc) or as a Mercury daemon, bin/loadgen with OPERATE_ROLE=loadgen (-client
// mercury, the same arrangement as the social benchmark).
//
// Arrivals (-arrivals):
//   closed   each route has N users, each sending its next request when the last returns; a level is N users at
//            full weight.
//   poisson  open loop: exponential gaps at a mean rate; a level is requests per second at full weight. Latency counts
//            from the scheduled arrival, so a client backlog shows as latency. -inflight caps requests in flight per
//            route; an arrival beyond it is an error.
//
// -mix sets each route's share of a level (weight 1 is the full level; the default is 1 for every route). Each level is
// held for -step and measured in -window windows. Every window writes one "route" line per route to the output and one
// JSON line per route to the histogram file (-hist), in the format the analysis reads.

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const (
	arrClosed  = "closed"
	arrPoisson = "poisson"
	envRole    = "OPERATE_ROLE" // "loadgen" selects runLoadgen in main
	roleGen    = "loadgen"
	doneLine   = "loadgen done"
	errorLine  = "loadgen error"
)

// loadOpts is everything one load run needs. A Mercury daemon gets it through the environment (env).
type loadOpts struct {
	arrivals string
	routes   []string
	mix      map[string]float64 // weight per route, 1 = the full level
	levels   []int
	step     time.Duration
	window   time.Duration
	timeout  time.Duration
	inflight int
	seed     int64
	users    int    // seeded usernames username_1..users, which compose-review writes as
	config   string // service-config.json
	movies   string // datasets/tmdb/movies.json
	id       string // Mercury id of this daemon, for the output
}

const (
	envArrivals = "LOADGEN_ARRIVALS"
	envRoutes   = "LOADGEN_ROUTES"
	envMix      = "LOADGEN_MIX"
	envLevels   = "LOADGEN_LEVELS"
	envStepMs   = "LOADGEN_STEP_MS"
	envWindowMs = "LOADGEN_WINDOW_MS"
	envTimeout  = "LOADGEN_TIMEOUT_MS"
	envInflight = "LOADGEN_INFLIGHT"
	envSeed     = "LOADGEN_SEED"
	envUsers    = "MEDIA_USERS"
	envConfig   = "MEDIA_CONFIG"
	envMovies   = "MEDIA_MOVIES"
	envOut      = "LOADGEN_OUT"  // route lines, one fsynced line per write
	envHist     = "LOADGEN_HIST" // histogram JSON lines
	envID       = "LOADGEN_ID"
)

// env encodes o for a Mercury daemon, which execs with no arguments.
func (o loadOpts) env() map[string]string {
	levels := make([]string, len(o.levels))
	for i, v := range o.levels {
		levels[i] = strconv.Itoa(v)
	}
	mix := make([]string, len(o.routes))
	for i, name := range o.routes {
		mix[i] = fmt.Sprintf("%s=%g", name, o.mix[name])
	}
	return map[string]string{
		envRole:     roleGen,
		envArrivals: o.arrivals,
		envRoutes:   strings.Join(o.routes, ","),
		envMix:      strings.Join(mix, ","),
		envLevels:   strings.Join(levels, ","),
		envStepMs:   strconv.FormatInt(o.step.Milliseconds(), 10),
		envWindowMs: strconv.FormatInt(o.window.Milliseconds(), 10),
		envTimeout:  strconv.FormatInt(o.timeout.Milliseconds(), 10),
		envInflight: strconv.Itoa(o.inflight),
		envSeed:     strconv.FormatInt(o.seed, 10),
		envUsers:    strconv.Itoa(o.users),
		envConfig:   o.config,
		envMovies:   o.movies,
		envID:       o.id,
	}
}

// optsFromEnv is the inverse of env, used by the daemon.
func optsFromEnv() (loadOpts, error) {
	var o loadOpts
	o.arrivals = os.Getenv(envArrivals)
	var err error
	if o.routes, err = parseRoutes(os.Getenv(envRoutes)); err != nil {
		return o, err
	}
	if o.mix, err = parseMix(os.Getenv(envMix), o.routes); err != nil {
		return o, err
	}
	for _, part := range strings.Split(os.Getenv(envLevels), ",") {
		v, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			return o, fmt.Errorf("bad %s %q", envLevels, os.Getenv(envLevels))
		}
		o.levels = append(o.levels, v)
	}
	ms := func(name string) (time.Duration, error) {
		v, err := strconv.ParseInt(os.Getenv(name), 10, 64)
		if err != nil || v <= 0 {
			return 0, fmt.Errorf("%s must be a positive number of milliseconds, got %q", name, os.Getenv(name))
		}
		return time.Duration(v) * time.Millisecond, nil
	}
	if o.step, err = ms(envStepMs); err != nil {
		return o, err
	}
	if o.window, err = ms(envWindowMs); err != nil {
		return o, err
	}
	if o.timeout, err = ms(envTimeout); err != nil {
		return o, err
	}
	if o.inflight, err = strconv.Atoi(os.Getenv(envInflight)); err != nil {
		return o, fmt.Errorf("bad %s", envInflight)
	}
	if o.seed, err = strconv.ParseInt(os.Getenv(envSeed), 10, 64); err != nil {
		return o, fmt.Errorf("bad %s", envSeed)
	}
	if o.users, err = strconv.Atoi(os.Getenv(envUsers)); err != nil {
		return o, fmt.Errorf("bad %s", envUsers)
	}
	o.config = os.Getenv(envConfig)
	o.movies = os.Getenv(envMovies)
	o.id = os.Getenv(envID)
	return o, o.check()
}

func (o loadOpts) check() error {
	if o.arrivals != arrClosed && o.arrivals != arrPoisson {
		return fmt.Errorf("arrivals must be %s or %s, got %q", arrClosed, arrPoisson, o.arrivals)
	}
	if len(o.routes) == 0 {
		return fmt.Errorf("no routes")
	}
	if len(o.levels) == 0 {
		return fmt.Errorf("no levels")
	}
	for _, v := range o.levels {
		if v < 1 {
			return fmt.Errorf("level %d is below 1", v)
		}
	}
	if o.window <= 0 || o.step < o.window {
		return fmt.Errorf("need 0 < window <= step, got window %v step %v", o.window, o.step)
	}
	if o.inflight < 1 {
		return fmt.Errorf("inflight must be at least 1")
	}
	if o.users < 1 {
		return fmt.Errorf("users must be at least 1")
	}
	return nil
}

// parseRoutes splits a comma-separated route list and checks every name.
func parseRoutes(spec string) ([]string, error) {
	var names []string
	seen := map[string]bool{}
	for _, part := range strings.Split(spec, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		if _, ok := routeDefs[name]; !ok {
			return nil, fmt.Errorf("unknown route %q (routes: %s)", name, strings.Join(routeNames, ","))
		}
		if seen[name] {
			return nil, fmt.Errorf("route %q listed twice", name)
		}
		seen[name] = true
		names = append(names, name)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no routes in %q", spec)
	}
	return names, nil
}

// parseMix reads route=weight pairs. Empty means weight 1 for every route; a listed route must be one of routes and
// have a weight above 0.
func parseMix(spec string, routes []string) (map[string]float64, error) {
	mix := map[string]float64{}
	for _, name := range routes {
		mix[name] = 1
	}
	if strings.TrimSpace(spec) == "" {
		return mix, nil
	}
	listed := map[string]bool{}
	for _, part := range strings.Split(spec, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, fmt.Errorf("bad -mix entry %q: want route=weight", part)
		}
		w, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil || w <= 0 {
			return nil, fmt.Errorf("bad weight in -mix entry %q", part)
		}
		name := strings.TrimSpace(k)
		if _, ok := mix[name]; !ok {
			return nil, fmt.Errorf("-mix names %q, which is not in -routes", name)
		}
		mix[name] = w
		listed[name] = true
	}
	for _, name := range routes {
		if !listed[name] {
			mix[name] = 0 // a route left out of a given mix gets no load
		}
	}
	return mix, nil
}

// syncWriter makes every Write durable before it returns, so the orchestrator tailing the file (and a crash) never
// loses a finished line.
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
	var hist io.Writer
	if p := os.Getenv(envHist); p != "" {
		h, err := os.OpenFile(p, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		defer h.Close()
		hist = syncWriter{h}
	}
	o, err := optsFromEnv()
	if err == nil {
		err = driveLoad(w, hist, o)
	}
	if err != nil {
		fmt.Fprintf(w, "%s: %v\n", errorLine, err)
		return err
	}
	fmt.Fprintln(w, doneLine)
	return nil
}

// running is one route's recorder during a run.
type running struct {
	name   string
	weight float64
	rec    *recorder
}

// driveLoad runs every level in o.levels for every route, writing route lines to w and histogram lines to hist (may be
// nil). All routes share each window, so their numbers line up in time.
func driveLoad(w io.Writer, hist io.Writer, o loadOpts) error {
	cfg, err := loadConfig(o.config)
	if err != nil {
		return err
	}
	movies, err := loadMovies(o.movies)
	if err != nil {
		return err
	}
	addr := map[string]string{}
	for _, name := range o.routes {
		for _, svc := range routeDefs[name].services {
			if _, ok := addr[svc]; ok {
				continue
			}
			if addr[svc], err = cfg.address(svc); err != nil {
				return err
			}
		}
	}
	maxLevel := 0
	for _, v := range o.levels {
		maxLevel = max(maxLevel, v)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var runs []running
	for i, name := range o.routes {
		weight := o.mix[name]
		if weight <= 0 {
			continue
		}
		slots := o.inflight
		if o.arrivals == arrClosed {
			slots = max(1, int(math.Ceil(float64(maxLevel)*weight)))
		}
		rt := newRouteRun(name, movies, o.users, slots, o.seed+int64(i)*1_000_003, addr, o.timeout)
		def := routeDefs[name]
		call := func(ctx context.Context, slot int) error { return def.call(ctx, rt, slot) }
		var rec *recorder
		if o.arrivals == arrPoisson {
			rec = startPoisson(ctx, call, slots, o.timeout, o.seed+int64(i))
		} else {
			rec = startLoad(ctx, call, slots, max(1, int(math.Round(float64(o.levels[0])*weight))))
		}
		runs = append(runs, running{name: name, weight: weight, rec: rec})
	}
	if len(runs) == 0 {
		return fmt.Errorf("every route has weight 0")
	}
	fmt.Fprintf(w, "loadgen start id=%s pid=%d arrivals=%s routes=%s levels=%v step=%v window=%v timeout=%v inflight=%d users=%d movies=%d\n",
		o.id, os.Getpid(), o.arrivals, o.routes, o.levels, o.step, o.window, o.timeout, o.inflight, o.users, len(movies))

	perLevel := int(o.step / o.window)
	for li, level := range o.levels {
		for _, r := range runs {
			setTarget(r.rec, o.arrivals, float64(level)*r.weight)
		}
		for win := 0; win < perLevel; win++ {
			befores := make([]snap, len(runs))
			for i, r := range runs {
				befores[i] = r.rec.snapshot()
			}
			m0 := monoNs()
			began := time.Now()
			time.Sleep(o.window)
			elapsed := time.Since(began)
			m1 := monoNs()
			for i, r := range runs {
				diff := r.rec.snapshot().sub(befores[i])
				target := float64(level) * r.weight
				writeWindow(w, r.name, li, win, target, o.arrivals, elapsed, diff, lastError(r.rec))
				writeHist(hist, r.name, li, win, target, m0, m1, diff)
			}
		}
	}
	return nil
}

// setTarget applies one level to a route: arrivals per second (poisson) or active users (closed).
func setTarget(rec *recorder, arrivals string, target float64) {
	if arrivals == arrPoisson {
		rec.rate.Store(math.Float64bits(target))
		return
	}
	rec.setActive(int32(max(1, math.Round(target))))
}

// writeWindow prints one summary line for a route's window: rates per second, mean and p95 latency of completed
// requests, and errors.
func writeWindow(w io.Writer, route string, level, window int, target float64, arrivals string, elapsed time.Duration, d snap, lastErr string) {
	avgUs := 0.0
	if d.done > 0 {
		avgUs = float64(d.sumNs) / float64(d.done) / 1e3
	}
	fmt.Fprintf(w, "route %s level %d window %d target %.2f arrivals %s arrival_rate %d service_rate %d avg_us %.1f p95_us %.1f errors %d\n",
		route, level, window, target, arrivals, perSecond(d.sent, elapsed), perSecond(d.done, elapsed), avgUs,
		float64(d.percentile(0.95))/1e3, d.errs)
	if d.errs > 0 {
		fmt.Fprintf(w, "last_error %q\n", lastErr)
	}
}

// lastError is the most recent failure the route saw, for the window lines.
func lastError(rec *recorder) string {
	text, _ := rec.lastErr.Load().(string)
	return text
}

// writeHist writes one JSON line for a route's window: its monotonic bounds, counts and every non-empty latency bucket
// as [upper_ns, count].
func writeHist(w io.Writer, route string, level, window int, target float64, m0, m1 int64, d snap) {
	if w == nil {
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, `{"route":%q,"level":%d,"window":%d,"clients":%d,"t0_ns":%d,"t1_ns":%d,"sent":%d,"done":%d,"errors":%d,"hist":[`,
		route, level, window, int(math.Round(target)), m0, m1, d.sent, d.done, d.errs)
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
	io.WriteString(w, b.String())
}

func perSecond(count uint64, elapsed time.Duration) uint64 {
	if elapsed <= 0 {
		return 0
	}
	return uint64(float64(count) / elapsed.Seconds())
}

func monoNs() int64 {
	var ts unix.Timespec
	if unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts) != nil {
		return 0
	}
	return ts.Nano()
}
