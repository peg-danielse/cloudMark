package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"golang.org/x/sys/unix"
	sn "socialnetwork/goperate/social_network"
)

// serviceConfig is config/service-config.json: one {addr, port} per key.
type serviceConfig map[string]json.RawMessage

func loadConfig(path string) (serviceConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg serviceConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

func (c serviceConfig) address(key string) (string, error) {
	raw, ok := c[key]
	if !ok {
		return "", fmt.Errorf("config has no %s", key)
	}
	var ep struct {
		Addr string `json:"addr"`
		Port int    `json:"port"`
	}
	if err := json.Unmarshal(raw, &ep); err != nil || ep.Port <= 0 {
		return "", fmt.Errorf("config %s has no port", key)
	}
	if ep.Addr == "" {
		ep.Addr = "127.0.0.1"
	}
	return fmt.Sprintf("%s:%d", ep.Addr, ep.Port), nil
}

// Route names follow DeathStarBench's wrk2 scripts.
const (
	routeCompose  = "compose-post"
	routeHome     = "read-home-timeline"
	routeUser     = "read-user-timeline"
	routeFollow   = "follow"
	routeLogin    = "login"
	defaultRoutes = routeCompose + "," + routeHome + "," + routeUser + "," + routeFollow
)

var routeService = map[string]string{
	routeCompose: "compose-post-service",
	routeHome:    "home-timeline-service",
	routeUser:    "user-timeline-service",
	routeFollow:  "social-graph-service",
	routeLogin:   "user-service",
}

// workload is what every route needs to build realistic requests: the user
// ids of the loaded social graph and the edges that must not be unfollowed.
type workload struct {
	users []int64
	edges map[[2]int64]bool
	mix   map[string]float64 // weight per route, max weight = 1
}

// loadGraph reads users and edges the same way the C++ init does: ids
// 0..N-1 from the .nodes count plus every id that appears in an edge.
func loadGraph(edgesPath string) (*workload, error) {
	f, err := os.Open(edgesPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	w := &workload{edges: map[[2]int64]bool{}}
	ids := map[int64]bool{}
	if n, err := nodeCount(edgesPath); err == nil {
		for id := int64(0); id < n; id++ {
			ids[id] = true
		}
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		from, to, ok := parseEdge(sc.Text())
		if !ok {
			continue
		}
		w.edges[[2]int64{from, to}] = true
		ids[from], ids[to] = true, true
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(ids) < 2 {
		return nil, fmt.Errorf("%s: need at least 2 users", edgesPath)
	}
	for id := range ids {
		w.users = append(w.users, id)
	}
	sort.Slice(w.users, func(i, j int) bool { return w.users[i] < w.users[j] })
	return w, nil
}

func nodeCount(edgesPath string) (int64, error) {
	if !strings.HasSuffix(edgesPath, ".edges") {
		return 0, errors.New("not .edges")
	}
	data, err := os.ReadFile(strings.TrimSuffix(edgesPath, ".edges") + ".nodes")
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(strings.Fields(string(data) + " 0")[0]), 10, 64)
}

func parseEdge(line string) (int64, int64, bool) {
	line = strings.TrimSpace(line)
	if line == "" || line[0] == '%' || line[0] == '#' {
		return 0, 0, false
	}
	f := strings.Fields(line)
	if len(f) != 2 {
		return 0, 0, false
	}
	a, err1 := strconv.ParseInt(f[0], 10, 64)
	b, err2 := strconv.ParseInt(f[1], 10, 64)
	if err1 != nil || err2 != nil || a == b {
		return 0, 0, false
	}
	return a, b, true
}

// parseMix reads "compose-post=0.1,read-home-timeline=0.6". Missing routes
// get weight 1 when the mix is empty; weights are scaled so the largest is 1.
func parseMix(spec string, routes []string) (map[string]float64, error) {
	mix := map[string]float64{}
	if strings.TrimSpace(spec) == "" {
		for _, r := range routes {
			mix[r] = 1
		}
		return mix, nil
	}
	for _, part := range strings.Split(spec, ",") {
		name, val, ok := strings.Cut(strings.TrimSpace(part), "=")
		w, err := strconv.ParseFloat(val, 64)
		if !ok || err != nil || w <= 0 {
			return nil, fmt.Errorf("bad mix entry %q", part)
		}
		mix[name] = w
	}
	var top float64
	for _, r := range routes {
		if mix[r] <= 0 {
			return nil, fmt.Errorf("mix has no weight for route %s", r)
		}
		top = math.Max(top, mix[r])
	}
	for r := range mix {
		mix[r] /= top
	}
	return mix, nil
}

// routeUsers is how many users a route runs when the ramp is at clients.
func routeUsers(clients int, weight float64) int {
	n := int(math.Ceil(float64(clients) * weight))
	if n < 1 {
		n = 1
	}
	return n
}

func parseRoutes(spec string) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	for _, r := range strings.Split(spec, ",") {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		if _, ok := routeService[r]; !ok {
			return nil, fmt.Errorf("unknown route %q (have %s,%s)", r, defaultRoutes, routeLogin)
		}
		if !seen[r] {
			seen[r] = true
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no routes")
	}
	return out, nil
}

// Thrift connections carry one call at a time, so a connection cannot be
// shared concurrently the way a gRPC one can. connsPerRoute > 0 makes a pool
// of that many connections that workers check out per call, which also caps
// that route's in-flight calls at connsPerRoute. 0 (default) gives every
// worker its own connection, as nginx-thrift does per request in DSB.
var connsPerRoute = 0

type tconn struct {
	trans  thrift.TTransport
	client thrift.TClient
}

func dialThrift(addr string) (*tconn, error) {
	cfg := &thrift.TConfiguration{
		ConnectTimeout: 3 * time.Second,
		SocketTimeout:  10 * time.Second,
	}
	sock := thrift.NewTSocketConf(addr, cfg)
	trans := thrift.NewTFramedTransportConf(sock, cfg)
	if err := trans.Open(); err != nil {
		return nil, err
	}
	proto := thrift.NewTBinaryProtocolConf(trans, cfg)
	return &tconn{trans: trans, client: thrift.NewTStandardClient(proto, proto)}, nil
}

// connSet hands a worker a connection and takes it back. A connection that
// saw a transport error is closed and redialed on next use.
type connSet struct {
	addr    string
	pool    chan *tconn // shared mode
	own     []*tconn    // per-worker mode
	perWork bool
}

func newConnSet(addr string, workers int) (*connSet, error) {
	cs := &connSet{addr: addr}
	if demoLoad.poisson() {
		// Open loop: request numbers keep growing, so no per-worker slots; a pool of -inflight connections, dialled
		// on first use (one now, to fail fast), so only the in-flight cap limits concurrency.
		c, err := dialThrift(addr)
		if err != nil {
			return nil, err
		}
		cs.pool = make(chan *tconn, demoLoad.inflight)
		cs.pool <- c
		for i := 1; i < demoLoad.inflight; i++ {
			cs.pool <- nil
		}
		return cs, nil
	}
	if connsPerRoute <= 0 || connsPerRoute >= workers {
		cs.perWork = true
		cs.own = make([]*tconn, workers)
		c, err := dialThrift(addr) // fail fast if the service is down
		if err != nil {
			return nil, err
		}
		cs.own[0] = c
		return cs, nil
	}
	cs.pool = make(chan *tconn, connsPerRoute)
	for i := 0; i < connsPerRoute; i++ {
		c, err := dialThrift(addr)
		if err != nil {
			cs.close()
			return nil, err
		}
		cs.pool <- c
	}
	return cs, nil
}

func (cs *connSet) get(ctx context.Context, worker int) (*tconn, error) {
	if cs.perWork {
		if c := cs.own[worker]; c != nil {
			return c, nil
		}
		c, err := dialThrift(cs.addr)
		if err != nil {
			return nil, err
		}
		cs.own[worker] = c
		return c, nil
	}
	select {
	case c := <-cs.pool:
		if c == nil {
			nc, err := dialThrift(cs.addr)
			if err != nil {
				cs.pool <- nil
				return nil, err
			}
			return nc, nil
		}
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// put returns c; broken connections are closed and replaced by a redial.
func (cs *connSet) put(worker int, c *tconn, broken bool) {
	if broken && c != nil {
		c.trans.Close()
		c = nil
	}
	if cs.perWork {
		cs.own[worker] = c
		return
	}
	cs.pool <- c
}

func (cs *connSet) close() {
	if cs.perWork {
		for _, c := range cs.own {
			if c != nil {
				c.trans.Close()
			}
		}
		return
	}
	for {
		select {
		case c := <-cs.pool:
			if c != nil {
				c.trans.Close()
			}
		default:
			return
		}
	}
}

// transportBroken reports whether err leaves the connection unusable. A
// ServiceException is an application answer on a healthy connection.
func transportBroken(err error) bool {
	var se *sn.ServiceException
	return err != nil && !errors.As(err, &se)
}

type load struct {
	call  func(context.Context, int) error
	close func()
}

var lettersDigits = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randString(r *rand.Rand, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = lettersDigits[r.Intn(len(lettersDigits))]
	}
	return string(b)
}

func username(id int64) string { return "user" + strconv.FormatInt(id, 10) }

// composeText follows DSB's compose-post.lua: 256 random characters, 0-5
// @mentions of real users and 0-5 http URLs.
func composeText(r *rand.Rand, users []int64) string {
	var b strings.Builder
	b.WriteString(randString(r, 256))
	for i := r.Intn(6); i > 0; i-- {
		b.WriteString(" @")
		b.WriteString(username(users[r.Intn(len(users))]))
	}
	for i := r.Intn(6); i > 0; i-- {
		b.WriteString(" http://")
		b.WriteString(randString(r, 64))
	}
	return b.String()
}

// timelineWindow follows DSB's read-*-timeline.lua: start in [0,100), 10 posts.
func timelineWindow(r *rand.Rand) (int32, int32) {
	start := int32(r.Intn(100))
	return start, start + 10
}

// followPair picks two distinct users that are not an edge of the dataset, so
// toggling follow/unfollow on them never removes a loaded edge.
func followPair(r *rand.Rand, w *workload) (int64, int64) {
	for {
		a := w.users[r.Intn(len(w.users))]
		b := w.users[r.Intn(len(w.users))]
		if a != b && !w.edges[[2]int64{a, b}] {
			return a, b
		}
	}
}

// openLoad builds one route's call function. Every worker has its own
// random source and, for follow, its own pair it follows and unfollows in
// turn.
func openLoad(cfg serviceConfig, name string, workers int, w *workload) (load, error) {
	addr, err := cfg.address(routeService[name])
	if err != nil {
		return load{}, err
	}
	cs, err := newConnSet(addr, workers)
	if err != nil {
		return load{}, fmt.Errorf("%s dial %s: %w", name, addr, err)
	}
	rngs := make([]*rand.Rand, workers)
	pairs := make([][2]int64, workers)
	following := make([]bool, workers)
	for i := range rngs {
		rngs[i] = rand.New(rand.NewSource(int64(i)*7919 + int64(len(name))))
		a, b := followPair(rngs[i], w)
		pairs[i] = [2]int64{a, b}
	}
	carrier := map[string]string{}
	do := func(ctx context.Context, worker int, fn func(thrift.TClient) error) error {
		c, err := cs.get(ctx, worker)
		if err != nil {
			return err
		}
		err = fn(c.client)
		cs.put(worker, c, transportBroken(err))
		return err
	}
	var call func(context.Context, int) error
	switch name {
	case routeCompose:
		call = func(ctx context.Context, worker int) error {
			r := rngs[worker]
			id := w.users[r.Intn(len(w.users))]
			text := composeText(r, w.users)
			n := r.Intn(5)
			mediaIDs := make([]int64, n)
			mediaTypes := make([]string, n)
			for i := range mediaIDs {
				mediaIDs[i] = r.Int63()
				mediaTypes[i] = "png"
			}
			return do(ctx, worker, func(c thrift.TClient) error {
				return sn.NewComposePostServiceClient(c).ComposePost(ctx, r.Int63(), username(id), id, text,
					mediaIDs, mediaTypes, sn.PostType_POST, carrier)
			})
		}
	case routeHome:
		call = func(ctx context.Context, worker int) error {
			r := rngs[worker]
			id := w.users[r.Intn(len(w.users))]
			start, stop := timelineWindow(r)
			return do(ctx, worker, func(c thrift.TClient) error {
				_, err := sn.NewHomeTimelineServiceClient(c).ReadHomeTimeline(ctx, r.Int63(), id, start, stop, carrier)
				return err
			})
		}
	case routeUser:
		call = func(ctx context.Context, worker int) error {
			r := rngs[worker]
			id := w.users[r.Intn(len(w.users))]
			start, stop := timelineWindow(r)
			return do(ctx, worker, func(c thrift.TClient) error {
				_, err := sn.NewUserTimelineServiceClient(c).ReadUserTimeline(ctx, r.Int63(), id, start, stop, carrier)
				return err
			})
		}
	case routeFollow:
		call = func(ctx context.Context, worker int) error {
			r := rngs[worker]
			p := pairs[worker]
			unfollow := following[worker]
			err := do(ctx, worker, func(c thrift.TClient) error {
				g := sn.NewSocialGraphServiceClient(c)
				if unfollow {
					return g.Unfollow(ctx, r.Int63(), p[0], p[1], carrier)
				}
				return g.Follow(ctx, r.Int63(), p[0], p[1], carrier)
			})
			if err == nil {
				following[worker] = !unfollow
			}
			return err
		}
	case routeLogin:
		call = func(ctx context.Context, worker int) error {
			r := rngs[worker]
			id := w.users[r.Intn(len(w.users))]
			return do(ctx, worker, func(c thrift.TClient) error {
				tok, err := sn.NewUserServiceClient(c).Login(ctx, r.Int63(), username(id), "password", carrier)
				if err == nil && tok == "" {
					return errors.New("empty login token")
				}
				return err
			})
		}
	default:
		cs.close()
		return load{}, fmt.Errorf("no load for %s", name)
	}
	return load{call: call, close: cs.close}, nil
}

type liveRoute struct {
	name   string
	weight float64
	work   load
	rec    *recorder
	cancel context.CancelFunc
}

func openRoutes(cfg serviceConfig, names []string, maxUsers int, w *workload) ([]liveRoute, error) {
	routes := make([]liveRoute, 0, len(names))
	for _, name := range names {
		weight := w.mix[name]
		workers := routeUsers(maxUsers, weight)
		if demoLoad.poisson() {
			workers = demoLoad.inflight // per-worker state for each in-flight slot (startPoisson's worker numbers)
		}
		work, err := openLoad(cfg, name, workers, w)
		if err != nil {
			return routes, err
		}
		ctx, cancel := context.WithCancel(context.Background())
		var rec *recorder
		if demoLoad.poisson() {
			rec = startPoisson(ctx, work.call, demoLoad.inflight, demoLoad.timeout(), routeSeed(name))
			rec.setLevel(readyRate) // until the schedule starts
		} else {
			rec = startLoad(ctx, work.call, workers, 1)
		}
		routes = append(routes, liveRoute{name: name, weight: weight, work: work, rec: rec, cancel: cancel})
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

// histOut receives one JSON line per route per window when -hist is on (LOADGEN_HIST in the load generator):
// the window's monotonic bounds, counts, and every non-empty latency bucket as [upper_ns, count].
var histOut io.Writer

func writeHist(routes []liveRoute, diffs []snap, level, window int, clients func(liveRoute) int, m0, m1 int64) {
	if histOut == nil {
		return
	}
	for i, route := range routes {
		d := diffs[i]
		var b strings.Builder
		fmt.Fprintf(&b, `{"route":%q,"level":%d,"window":%d,"clients":%d,"t0_ns":%d,"t1_ns":%d,"sent":%d,"done":%d,"errors":%d,"hist":[`,
			route.name, level, window, clients(route), m0, m1, d.sent, d.done, d.errs)
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

func waitReady(rec *recorder, name string) error {
	deadline := time.Now().Add(10 * time.Second)
	hard := time.Now().Add(120 * time.Second) // a backend answering LOADING (Redis) extends the wait up to this
	for time.Now().Before(deadline) {
		if rec.snapshot().done > 0 {
			return nil
		}
		if text, ok := rec.lastErr.Load().(string); ok && strings.Contains(text, "LOADING") && time.Now().Add(time.Second).After(deadline) && deadline.Before(hard) {
			deadline = deadline.Add(time.Second)
		}
		time.Sleep(20 * time.Millisecond)
	}
	detail := "no response"
	if text, ok := rec.lastErr.Load().(string); ok && text != "" {
		detail = text
	}
	return fmt.Errorf("%s: %s", name, detail)
}
