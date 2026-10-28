package main

// jaegercp.go: Jaeger tracing for the Mercury operators. This file is shared
// verbatim by deathstar-hotelReservations/operate and
// deathstar-socialNetwork/goperate (stdlib only, no app types except tuple,
// start, setCpus and kill, which both operators define with the same
// signatures). The app-specific parts (sampling hook, span -> daemon model)
// live in jaeger_app.go in each repo.
//
// What it does when -jaeger is on:
//
//  1. startJaeger runs the Jaeger v2 all-in-one (run-jaeger.sh from
//     ~/Benchmarks/runtime/jaeger; ~/Documents/Benchmarks/runtime/jaeger on the PC) as a bare helper process pinned with taskset
//     (-jaeger-mode bare), as a Mercury daemon moved onto -jaeger-cpus with
//     POST /cpus (-jaeger-mode mercury), or uses one that is already running
//     (-jaeger-mode external).
//  2. driveRoutes prints one marker line per measurement window
//     ("window <n> level <l> wi <i> clients <c> start_us <a> end_us <b>").
//     Route parsers only match "^route", so the marker is invisible to them.
//  3. lineSniffer sees those markers on the operator's stdout copy and hands
//     each window to cpTracer, which waits -trace-lag-ms (client flush plus
//     collector batch), fetches the window's traces from the Jaeger v3 query
//     API, and computes a CRISP-style critical path for every sampled request.
//  4. Each window writes traces.csv rows (route x daemon) and, with
//     -trace-cp-lines, one "cp ..." summary line per route to stdout.
//
// Critical path (CRISP, Zhang et al., USENIX ATC'22): walk back from the end
// of a span; the child that finished last before the cursor is on the path,
// the gap after it is the parent's own time, then continue from that child's
// start. Children are clipped to the parent's interval (async work that
// outlives the request, e.g. RabbitMQ consumers, is off the path). A client
// span's own time (serialisation, network, queueing in front of the callee)
// is charged to the callee (and counted in rpc_share); a client span that
// waits on a store (memcached, MongoDB, Redis) is charged to that store's
// daemon. All time of the root interval is attributed, so per request the
// shares of all daemons sum to 1.

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ---------------------------------------------------------------- options

const (
	jaegerModeBare     = "bare"
	jaegerModeMercury  = "mercury"
	jaegerModeExternal = "external"

	// Default Jaeger ports; -jaeger-port-offset is added to every one.
	jaegerPortCompact   = 6831  // jaeger.thrift compact, UDP (agent protocol)
	jaegerPortBinary    = 6832  // jaeger.thrift binary, UDP
	jaegerPortThriftHTT = 14268 // jaeger.thrift over HTTP (/api/traces)
	jaegerPortQueryHTTP = 16686 // query API (/api/v3/...)
	jaegerPortHealth    = 13133
)

// jaegerOpts are the -jaeger* and -trace-* flags.
type jaegerOpts struct {
	on        bool
	mode      string
	script    string
	cpus      string
	host      string
	offset    int
	addr      string // query API host:port; "" = host:16686+offset
	sample    float64
	transport string // http | udp (apps that support it)
	storage   string
	maxTraces int
	lag       time.Duration
	depth     int
	out       string
	cpLines   bool
	avoid     string // ports Jaeger must not take, e.g. "8080,18180-18199"
}

// tracing is set by main from the flags (nil or !on = tracing off, old behaviour).
var tracing *jaegerOpts

// addJaegerFlags registers the tracing flags on the default flag set.
func addJaegerFlags(benchRoot string) *jaegerOpts {
	o := &jaegerOpts{}
	flag.BoolVar(&o.on, "jaeger", false, "trace: start Jaeger, enable app tracing at -trace-sample, write per-window critical-path shares (traces.csv)")
	flag.StringVar(&o.mode, "jaeger-mode", jaegerModeBare, "how Jaeger runs: bare (helper process, taskset -jaeger-cpus, no sudo), mercury (Mercury daemon moved to -jaeger-cpus), external (already running)")
	script := benchRoot + "/runtime/jaeger/run-jaeger.sh" // moved from <root>/jaeger; keep the old place as a fallback
	if _, err := os.Stat(script); err != nil {
		if _, old := os.Stat(benchRoot + "/jaeger/run-jaeger.sh"); old == nil {
			script = benchRoot + "/jaeger/run-jaeger.sh"
		}
	}
	flag.StringVar(&o.script, "jaeger-bin", script, "Jaeger launcher (run-jaeger.sh, env-configured)")
	flag.StringVar(&o.cpus, "jaeger-cpus", "62", "helper CPU list for Jaeger (keep it off the service pool)")
	flag.StringVar(&o.host, "jaeger-host", "127.0.0.1", "address Jaeger listens on and the apps report to")
	flag.IntVar(&o.offset, "jaeger-port-offset", 0, "added to every Jaeger port (6831/6832 udp, 14268, 14250, 4317, 4318, 16686, 16685, 13133)")
	flag.StringVar(&o.addr, "jaeger-addr", "", "Jaeger query API host:port (default -jaeger-host:16686+offset)")
	flag.Float64Var(&o.sample, "trace-sample", 0.02, "probabilistic sampling rate the apps are started with (0-1)")
	flag.StringVar(&o.transport, "trace-transport", "http", "app -> Jaeger transport: http (jaeger.thrift to :14268) or udp (compact to :6831; C++ social only, Go hotel always uses http)")
	flag.StringVar(&o.storage, "jaeger-storage", "memory", "Jaeger storage: memory or badger")
	flag.IntVar(&o.maxTraces, "jaeger-max-traces", 200000, "memory storage bound (traces)")
	flag.DurationVar(&o.lag, "trace-lag", 3*time.Second, "wait after a window ends before querying it (client flush 1 s + collector batch)")
	flag.IntVar(&o.depth, "trace-depth", 5000, "max traces fetched per window (query.searchDepth)")
	flag.StringVar(&o.out, "trace-out", "", "traces CSV (default traces.csv next to -gen-out, else output/traces-<unix>.csv)")
	flag.BoolVar(&o.cpLines, "trace-cp-lines", true, "also print one 'cp window ... route ...' line per route and window")
	flag.StringVar(&o.avoid, "jaeger-avoid-ports", "8080,18180-18199", "ports/ranges no Jaeger port may land on (other users' services)")
	return o
}

func (o *jaegerOpts) validate() error {
	if o == nil || !o.on {
		return nil
	}
	switch o.mode {
	case jaegerModeBare, jaegerModeMercury, jaegerModeExternal:
	default:
		return fmt.Errorf("-jaeger-mode must be bare, mercury or external")
	}
	if o.sample < 0 || o.sample > 1 {
		return fmt.Errorf("-trace-sample must be in [0,1]")
	}
	if o.transport != "http" && o.transport != "udp" {
		return fmt.Errorf("-trace-transport must be http or udp")
	}
	if o.storage != "memory" && o.storage != "badger" {
		return fmt.Errorf("-jaeger-storage must be memory or badger")
	}
	if o.offset < 0 || jaegerPortQueryHTTP+o.offset > 65535 {
		return fmt.Errorf("bad -jaeger-port-offset %d", o.offset)
	}
	if err := o.checkAvoid(); err != nil {
		return err
	}
	if o.depth < 1 || (o.storage == "memory" && o.depth >= o.maxTraces) {
		return fmt.Errorf("need 1 <= -trace-depth < -jaeger-max-traces")
	}
	if o.mode != jaegerModeExternal && strings.TrimSpace(o.cpus) == "" {
		return fmt.Errorf("-jaeger-cpus is required for -jaeger-mode %s", o.mode)
	}
	return nil
}

func (o *jaegerOpts) enabled() bool { return o != nil && o.on }

var jaegerAllPorts = []int{jaegerPortCompact, jaegerPortBinary, jaegerPortThriftHTT, 14250, 4317, 4318, jaegerPortQueryHTTP, 16685, jaegerPortHealth}

// checkAvoid rejects an offset that puts any Jaeger port on -jaeger-avoid-ports.
func (o *jaegerOpts) checkAvoid() error {
	for _, part := range strings.Split(o.avoid, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lo, hi, isRange := strings.Cut(part, "-")
		a, err := strconv.Atoi(lo)
		b := a
		if err == nil && isRange {
			b, err = strconv.Atoi(hi)
		}
		if err != nil || b < a {
			return fmt.Errorf("bad -jaeger-avoid-ports entry %q", part)
		}
		for _, p := range jaegerAllPorts {
			if q := o.port(p); q >= a && q <= b {
				return fmt.Errorf("-jaeger-port-offset %d puts Jaeger port %d (default %d) in avoided %s", o.offset, q, p, part)
			}
		}
	}
	return nil
}

func (o *jaegerOpts) port(def int) int { return def + o.offset }

func (o *jaegerOpts) hostPort(def int) string {
	return net.JoinHostPort(o.host, strconv.Itoa(o.port(def)))
}

// queryBase is the Jaeger query API base URL.
func (o *jaegerOpts) queryBase() string {
	if o.addr != "" {
		if strings.Contains(o.addr, "://") {
			return strings.TrimRight(o.addr, "/")
		}
		return "http://" + o.addr
	}
	return "http://" + o.hostPort(jaegerPortQueryHTTP)
}

// collectorHTTP is the jaeger.thrift HTTP endpoint the clients post to.
func (o *jaegerOpts) collectorHTTP() string {
	return "http://" + o.hostPort(jaegerPortThriftHTT) + "/api/traces"
}

// agentUDP is the jaeger.thrift compact UDP address.
func (o *jaegerOpts) agentUDP() string { return o.hostPort(jaegerPortCompact) }

func (o *jaegerOpts) sampleString() string { return strconv.FormatFloat(o.sample, 'g', -1, 64) }

// launchEnv configures run-jaeger.sh (Mercury passes only explicit env).
func (o *jaegerOpts) launchEnv() map[string]string {
	return map[string]string{
		"JAEGER_PORT_OFFSET": strconv.Itoa(o.offset),
		"JAEGER_STORAGE":     o.storage,
		"JAEGER_MAX_TRACES":  strconv.Itoa(o.maxTraces),
		"JAEGER_HOST":        o.host,
	}
}

// csvPath is -trace-out, else traces.csv beside the loadgen output, else a
// timestamped file in appOutputDir.
func (o *jaegerOpts) csvPath(genOut, appOutputDir string) string {
	if o.out != "" {
		return o.out
	}
	if genOut != "" {
		return filepath.Join(filepath.Dir(genOut), "traces.csv")
	}
	return filepath.Join(appOutputDir, fmt.Sprintf("traces-%d.csv", time.Now().Unix()))
}

// ---------------------------------------------------------------- Jaeger process

type jaegerProc struct {
	o    *jaegerOpts
	cmd  *exec.Cmd
	dead chan struct{} // closed when a bare Jaeger exits
	base string        // Mercury base URL (mercury mode)
	id   string        // Mercury daemon id (mercury mode)
}

// startJaeger brings Jaeger up according to o.mode and waits for its query API.
// cpu is the Mercury daemon CPU (-cpu) used for /start in mercury mode; idPrefix
// is "hotel" or "social"; logPath is the bare-mode log; track registers the
// Mercury daemon for the operator's normal kill-on-exit.
func startJaeger(o *jaegerOpts, base string, cpu int, idPrefix, logPath string, track func(tuple)) (*jaegerProc, error) {
	p := &jaegerProc{o: o}
	if o.mode == jaegerModeExternal {
		if err := waitJaeger(o.queryBase(), 10*time.Second, nil); err != nil {
			return nil, fmt.Errorf("-jaeger-mode external: %w", err)
		}
		fmt.Printf("jaeger external query %s collector %s\n", o.queryBase(), o.collectorHTTP())
		return p, nil
	}
	if err := waitJaeger(o.queryBase(), 0, nil); err == nil {
		return nil, fmt.Errorf("a Jaeger already answers on %s: use -jaeger-mode external or another -jaeger-port-offset", o.queryBase())
	}
	if err := jaegerPortsFree(o); err != nil {
		return nil, err
	}
	if st, err := os.Stat(o.script); err != nil || st.Mode()&0o111 == 0 {
		return nil, fmt.Errorf("jaeger launcher %s is missing or not executable", o.script)
	}
	env := o.launchEnv()
	if logPath != "" {
		if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
			return nil, err
		}
		env["JAEGER_LOG"] = logPath
	}
	switch o.mode {
	case jaegerModeBare:
		env["JAEGER_CPUS"] = o.cpus
		cmd := exec.Command(o.script)
		cmd.Env = os.Environ()
		for k, v := range env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
		// Own process group (stop() signals the group); Pdeathsig so a killed
		// operator does not leave Jaeger behind.
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGTERM}
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		p.cmd, p.dead = cmd, make(chan struct{})
		go func() { cmd.Wait(); close(p.dead) }()
		fmt.Printf("jaeger bare pid %d cpus %s %s log %s\n", cmd.Process.Pid, o.cpus, formatEnv(env), logPath)
	case jaegerModeMercury:
		p.base, p.id = base, idPrefix+"-jaeger"
		line := tuple{path: o.script, hostname: "localhost", id: p.id}
		pid, err := start(base, line, cpu, env)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p.id, err)
		}
		track(line)
		fmt.Printf("jaeger mercury daemon_id=%s pid %d %s\n", p.id, pid, formatEnv(env))
		if err := setCpus(base, p.id, o.cpus); err != nil {
			return p, err
		}
		fmt.Printf("jaeger cpus %s (fixed, no /scale policy)\n", o.cpus)
	}
	if err := waitJaeger(o.queryBase(), 60*time.Second, p.dead); err != nil {
		p.stop()
		if logPath != "" {
			err = fmt.Errorf("%w (log %s)", err, logPath)
		}
		return nil, err
	}
	fmt.Printf("jaeger up query %s collector %s agent udp %s sample %s\n", o.queryBase(), o.collectorHTTP(), o.agentUDP(), o.sampleString())
	return p, nil
}

// stop ends a bare Jaeger. Mercury mode is killed with the other tracked
// daemons; external is left alone.
func (p *jaegerProc) stop() {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	pgid := p.cmd.Process.Pid
	_ = syscall.Kill(-pgid, syscall.SIGTERM)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if syscall.Kill(-pgid, 0) != nil {
			fmt.Printf("jaeger stopped\n")
			p.cmd = nil
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
	fmt.Printf("jaeger killed\n")
	p.cmd = nil
}

// waitJaeger polls GET /api/v3/services until it answers 200. timeout 0 = one
// try; a closed dead channel (the process exited) ends the wait early.
func waitJaeger(queryBase string, timeout time.Duration, dead <-chan struct{}) error {
	deadline := time.Now().Add(timeout)
	c := &http.Client{Timeout: 2 * time.Second}
	var last error
	for {
		resp, err := c.Get(queryBase + "/api/v3/services")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			err = fmt.Errorf("status %s", resp.Status)
		}
		last = err
		if !time.Now().Before(deadline) {
			return fmt.Errorf("jaeger query %s not answering: %v", queryBase, last)
		}
		select {
		case <-dead:
			return fmt.Errorf("jaeger exited before its query API answered on %s", queryBase)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func jaegerPortsFree(o *jaegerOpts) error {
	for _, p := range []int{jaegerPortThriftHTT, jaegerPortQueryHTTP, jaegerPortHealth, 4317, 4318, 14250, 16685} {
		l, err := net.Listen("tcp", o.hostPort(p))
		if err != nil {
			return fmt.Errorf("jaeger port %s busy: %v (pick another -jaeger-port-offset)", o.hostPort(p), err)
		}
		l.Close()
	}
	for _, p := range []int{jaegerPortCompact, jaegerPortBinary} {
		c, err := net.ListenPacket("udp", o.hostPort(p))
		if err != nil {
			return fmt.Errorf("jaeger udp port %s busy: %v (pick another -jaeger-port-offset)", o.hostPort(p), err)
		}
		c.Close()
	}
	return nil
}

// ---------------------------------------------------------------- window markers

// traceMarks makes driveRoutes print window markers (set in the operator for
// inproc, from LOADGEN_TRACE_MARKS=1 in bin/loadgen).
var traceMarks bool

const envTraceMarks = "LOADGEN_TRACE_MARKS"

type traceWindow struct {
	idx, level, wi, clients int
	start, end              time.Time
}

func formatTraceMark(w traceWindow) string {
	return fmt.Sprintf("window %d level %d wi %d clients %d start_us %d end_us %d\n",
		w.idx, w.level, w.wi, w.clients, w.start.UnixMicro(), w.end.UnixMicro())
}

func parseTraceMark(line string) (traceWindow, bool) {
	f := strings.Fields(line)
	keys := []string{"window", "level", "wi", "clients", "start_us", "end_us"}
	if len(f) != 2*len(keys) {
		return traceWindow{}, false
	}
	var v [6]int64
	for i, key := range keys {
		if f[2*i] != key {
			return traceWindow{}, false
		}
		n, err := strconv.ParseInt(f[2*i+1], 10, 64)
		if err != nil {
			return traceWindow{}, false
		}
		v[i] = n
	}
	return traceWindow{idx: int(v[0]), level: int(v[1]), wi: int(v[2]), clients: int(v[3]),
		start: time.UnixMicro(v[4]), end: time.UnixMicro(v[5])}, true
}

// markCounter numbers the windows driveRoutes marks.
type markCounter struct{ n int }

func (m *markCounter) emit(w io.Writer, level, wi, clients int, start, end time.Time) {
	if !traceMarks {
		return
	}
	fmt.Fprint(w, formatTraceMark(traceWindow{idx: m.n, level: level, wi: wi, clients: clients, start: start, end: end}))
	m.n++
}

// lockedWriter serialises writers sharing one stream (route lines, cp lines).
type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// lineSniffer copies everything to w and calls on for every complete line.
type lineSniffer struct {
	w   io.Writer
	buf []byte
	on  func(string)
}

func (s *lineSniffer) Write(p []byte) (int, error) {
	n, err := s.w.Write(p)
	s.buf = append(s.buf, p...)
	for {
		i := bytes.IndexByte(s.buf, '\n')
		if i < 0 {
			break
		}
		line := string(s.buf[:i])
		s.buf = s.buf[i+1:]
		if s.on != nil {
			s.on(line)
		}
	}
	return n, err
}

// ---------------------------------------------------------------- traces

type traceSpan struct {
	traceID, spanID, parentID string
	service, name             string
	kind                      int // OTLP: 1 internal, 2 server, 3 client, 4 producer, 5 consumer
	start, end                int64
}

func (s *traceSpan) isClient() bool {
	if s.kind == 3 || s.kind == 4 {
		return true
	}
	n := strings.ToLower(s.name)
	return strings.HasSuffix(n, "_client") || strings.HasSuffix(n, "client")
}

// traceModel is the app's view of its spans.
type traceModel struct {
	// route of a trace from its root span ("" = not a route we drive).
	route func(root *traceSpan) string
	// service maps a tracer service name to its Mercury program (basename).
	service func(service string) string
	// backend: the store daemon a span waits on (memcached port, mongod,
	// redis-server, ...), "" when the span is not a store call.
	backend func(s *traceSpan) string
}

func (m traceModel) owner(s *traceSpan, kids []*traceSpan) (daemon string, rpc bool) {
	if b := m.backend(s); b != "" {
		return b, false
	}
	if s.isClient() {
		for _, k := range kids {
			if k.service != s.service {
				return m.service(k.service), true
			}
		}
	}
	return m.service(s.service), false
}

type v3Doc struct {
	Result *struct {
		ResourceSpans []struct {
			Resource struct {
				Attributes []struct {
					Key   string `json:"key"`
					Value struct {
						StringValue string `json:"stringValue"`
					} `json:"value"`
				} `json:"attributes"`
			} `json:"resource"`
			ScopeSpans []struct {
				Spans []struct {
					TraceID      string          `json:"traceId"`
					SpanID       string          `json:"spanId"`
					ParentSpanID string          `json:"parentSpanId"`
					Name         string          `json:"name"`
					Kind         json.RawMessage `json:"kind"`
					Start        json.RawMessage `json:"startTimeUnixNano"`
					End          json.RawMessage `json:"endTimeUnixNano"`
				} `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"resourceSpans"`
	} `json:"result"`
	Error *struct {
		HTTPCode int    `json:"httpCode"`
		Message  string `json:"message"`
	} `json:"error"`
}

var otlpKinds = map[string]int{"SPAN_KIND_INTERNAL": 1, "SPAN_KIND_SERVER": 2, "SPAN_KIND_CLIENT": 3, "SPAN_KIND_PRODUCER": 4, "SPAN_KIND_CONSUMER": 5}

func rawInt(r json.RawMessage) int64 {
	s := strings.Trim(strings.TrimSpace(string(r)), `"`)
	if k, ok := otlpKinds[s]; ok {
		return int64(k)
	}
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// parseV3 reads a Jaeger /api/v3/traces response (one or more JSON documents)
// into spans grouped by trace id. A "No traces found" error document is empty.
func parseV3(r io.Reader) (map[string][]*traceSpan, error) {
	out := map[string][]*traceSpan{}
	dec := json.NewDecoder(r)
	for {
		var doc v3Doc
		if err := dec.Decode(&doc); err == io.EOF {
			return out, nil
		} else if err != nil {
			return out, err
		}
		if doc.Error != nil {
			if doc.Error.HTTPCode == http.StatusNotFound {
				continue
			}
			return out, fmt.Errorf("jaeger: %d %s", doc.Error.HTTPCode, doc.Error.Message)
		}
		if doc.Result == nil {
			continue
		}
		for _, rs := range doc.Result.ResourceSpans {
			svc := ""
			for _, a := range rs.Resource.Attributes {
				if a.Key == "service.name" {
					svc = a.Value.StringValue
				}
			}
			for _, ss := range rs.ScopeSpans {
				for _, sp := range ss.Spans {
					s := &traceSpan{traceID: sp.TraceID, spanID: sp.SpanID, parentID: sp.ParentSpanID,
						service: svc, name: sp.Name, kind: int(rawInt(sp.Kind)), start: rawInt(sp.Start), end: rawInt(sp.End)}
					if s.end < s.start {
						s.end = s.start
					}
					out[s.traceID] = append(out[s.traceID], s)
				}
			}
		}
	}
}

// traceResult is one sampled request.
type traceResult struct {
	route string
	start int64 // root start, unix ns
	dur   int64 // root duration, ns
	crit  map[string]int64
	rpc   map[string]int64
	spans map[string]int
}

// analyzeTrace computes the critical path of one trace. ok is false when the
// root is not a route of this app.
func analyzeTrace(spans []*traceSpan, m traceModel) (traceResult, bool) {
	byID := make(map[string]*traceSpan, len(spans))
	var uniq []*traceSpan
	for _, s := range spans {
		if _, dup := byID[s.spanID]; dup {
			continue
		}
		byID[s.spanID] = s
		uniq = append(uniq, s)
	}
	kids := map[string][]*traceSpan{}
	var root *traceSpan
	for _, s := range uniq {
		if p, ok := byID[s.parentID]; ok && s.parentID != "" && p != s {
			kids[s.parentID] = append(kids[s.parentID], s)
			continue
		}
		if root == nil || s.start < root.start || (s.start == root.start && s.end-s.start > root.end-root.start) {
			root = s
		}
	}
	if root == nil {
		return traceResult{}, false
	}
	res := traceResult{route: m.route(root), start: root.start, dur: root.end - root.start,
		crit: map[string]int64{}, rpc: map[string]int64{}, spans: map[string]int{}}
	if res.route == "" || res.dur <= 0 {
		return res, false
	}
	for _, s := range uniq {
		d, _ := m.owner(s, kids[s.spanID])
		res.spans[d]++
	}
	visited := map[*traceSpan]bool{}
	var walk func(s *traceSpan, lo, hi int64, depth int)
	walk = func(s *traceSpan, lo, hi int64, depth int) {
		a, b := max64(s.start, lo), min64(s.end, hi)
		if b <= a || visited[s] {
			return
		}
		visited[s] = true
		ks := kids[s.spanID]
		owner, rpc := m.owner(s, ks)
		add := func(d int64) {
			if d <= 0 {
				return
			}
			res.crit[owner] += d
			if rpc {
				res.rpc[owner] += d
			}
		}
		used := make([]bool, len(ks))
		t := b
		for depth < 512 {
			best, bestEnd := -1, int64(0)
			for i, k := range ks {
				if used[i] || k.start >= t || k.end <= a {
					continue
				}
				if e := min64(k.end, t); best < 0 || e > bestEnd {
					best, bestEnd = i, e
				}
			}
			if best < 0 {
				break
			}
			used[best] = true
			add(t - bestEnd)
			k := ks[best]
			walk(k, max64(k.start, a), bestEnd, depth+1)
			t = max64(k.start, a)
			if t <= a {
				break
			}
		}
		add(t - a)
	}
	walk(root, root.start, root.end, 0)
	return res, true
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// ---------------------------------------------------------------- aggregation

var traceCSVHeader = []string{"window", "level", "wi", "clients", "start_unix_us", "end_unix_us",
	"route", "daemon", "crit_share", "crit_share_tail", "rpc_share", "excl_ms_p50", "excl_ms_p95",
	"excl_ms_mean", "span_count", "sample_count", "root_ms_p50", "root_ms_p95", "fetched", "truncated"}

type cpRow struct {
	route, daemon    string
	share, tail, rpc float64
	p50, p95, mean   float64
	spans, samples   int
	rootP50, rootP95 float64
}

// windowRows turns the window's sampled requests into one row per route and
// daemon. crit_share = daemon's critical-path time / total request time over
// the samples; crit_share_tail is the same over requests >= the route's p90;
// rpc_share is the part of crit_share spent in client spans in front of the
// daemon; excl_ms_* are per-request critical-path ms (0 when absent).
func windowRows(results []traceResult) []cpRow {
	byRoute := map[string][]traceResult{}
	for _, r := range results {
		byRoute[r.route] = append(byRoute[r.route], r)
	}
	routes := make([]string, 0, len(byRoute))
	for r := range byRoute {
		routes = append(routes, r)
	}
	sort.Strings(routes)
	var rows []cpRow
	for _, route := range routes {
		rs := byRoute[route]
		durs := make([]float64, len(rs))
		var total int64
		for i, r := range rs {
			durs[i] = float64(r.dur) / 1e6
			total += r.dur
		}
		p90 := quantile(durs, 0.90)
		rootP50, rootP95 := quantile(durs, 0.50), quantile(durs, 0.95)
		var tailTotal int64
		for _, r := range rs {
			if float64(r.dur)/1e6 >= p90 {
				tailTotal += r.dur
			}
		}
		daemons := map[string]bool{}
		for _, r := range rs {
			for d := range r.crit {
				daemons[d] = true
			}
			for d := range r.spans {
				daemons[d] = true
			}
		}
		var rrows []cpRow
		for d := range daemons {
			row := cpRow{route: route, daemon: d, samples: len(rs), rootP50: rootP50, rootP95: rootP95}
			excl := make([]float64, len(rs))
			var crit, rpc, tail int64
			for i, r := range rs {
				crit += r.crit[d]
				rpc += r.rpc[d]
				if float64(r.dur)/1e6 >= p90 {
					tail += r.crit[d]
				}
				excl[i] = float64(r.crit[d]) / 1e6
				row.spans += r.spans[d]
			}
			if total > 0 {
				row.share = float64(crit) / float64(total)
				row.rpc = float64(rpc) / float64(total)
			}
			if tailTotal > 0 {
				row.tail = float64(tail) / float64(tailTotal)
			}
			row.p50, row.p95 = quantile(excl, 0.50), quantile(excl, 0.95)
			row.mean = float64(crit) / 1e6 / float64(len(rs))
			rrows = append(rrows, row)
		}
		sort.Slice(rrows, func(i, j int) bool {
			if rrows[i].share != rrows[j].share {
				return rrows[i].share > rrows[j].share
			}
			return rrows[i].daemon < rrows[j].daemon
		})
		rows = append(rows, rrows...)
	}
	return rows
}

// quantile: nearest-rank on a copy.
func quantile(v []float64, q float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	i := int(math.Ceil(q*float64(len(s)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(s) {
		i = len(s) - 1
	}
	return s[i]
}

func f4(v float64) string { return strconv.FormatFloat(v, 'f', 4, 64) }
func f3(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }

// ---------------------------------------------------------------- tracer

// cpTracer analyses windows asynchronously, in order, on one goroutine.
type cpTracer struct {
	query   string
	model   traceModel
	lag     time.Duration
	depth   int
	f       *os.File
	csv     *csv.Writer
	out     io.Writer // cp lines; nil = off
	q       chan traceWindow
	done    chan struct{}
	client  *http.Client
	mu      sync.Mutex
	dropped int
	windows int
	failed  int
	samples int
	trunc   int
	now     func() time.Time
	sleep   func(time.Duration)
}

func newCPTracer(query string, m traceModel, csvPath string, out io.Writer, lag time.Duration, depth int) (*cpTracer, error) {
	if err := os.MkdirAll(filepath.Dir(csvPath), 0o755); err != nil {
		return nil, err
	}
	f, err := os.Create(csvPath)
	if err != nil {
		return nil, err
	}
	t := &cpTracer{query: strings.TrimRight(query, "/"), model: m, lag: lag, depth: depth, f: f, csv: csv.NewWriter(f),
		out: out, q: make(chan traceWindow, 4096), done: make(chan struct{}),
		client: &http.Client{Timeout: 30 * time.Second}, now: time.Now, sleep: time.Sleep}
	if err := t.csv.Write(traceCSVHeader); err != nil {
		f.Close()
		return nil, err
	}
	t.csv.Flush()
	go t.run()
	return t, nil
}

// mark queues a window; it never blocks the load generator's output path.
func (t *cpTracer) mark(w traceWindow) {
	select {
	case t.q <- w:
	default:
		t.mu.Lock()
		t.dropped++
		t.mu.Unlock()
	}
}

// onLine is the lineSniffer callback.
func (t *cpTracer) onLine(line string) {
	if w, ok := parseTraceMark(line); ok {
		t.mark(w)
	}
}

// close processes what is queued (waiting out the lag of the last window) and
// closes the CSV.
func (t *cpTracer) close() string {
	close(t.q)
	<-t.done
	t.csv.Flush()
	t.f.Close()
	t.mu.Lock()
	defer t.mu.Unlock()
	return fmt.Sprintf("traces windows %d samples %d failed %d truncated %d dropped %d csv %s",
		t.windows, t.samples, t.failed, t.trunc, t.dropped, t.f.Name())
}

func (t *cpTracer) run() {
	defer close(t.done)
	for w := range t.q {
		if d := w.end.Add(t.lag).Sub(t.now()); d > 0 {
			t.sleep(d)
		}
		if err := t.process(w); err != nil {
			t.mu.Lock()
			t.failed++
			t.mu.Unlock()
			fmt.Fprintf(os.Stderr, "traces window %d: %v\n", w.idx, err)
		}
	}
}

func (t *cpTracer) fetch(start, end time.Time) (map[string][]*traceSpan, error) {
	q := url.Values{}
	q.Set("query.startTimeMin", start.UTC().Format(time.RFC3339Nano))
	q.Set("query.startTimeMax", end.UTC().Format(time.RFC3339Nano))
	q.Set("query.searchDepth", strconv.Itoa(t.depth))
	resp, err := t.client.Get(t.query + "/api/v3/traces?" + q.Encode())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return map[string][]*traceSpan{}, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("GET /api/v3/traces: %s %s", resp.Status, bytes.TrimSpace(body))
	}
	return parseV3(resp.Body)
}

func (t *cpTracer) process(w traceWindow) error {
	traces, err := t.fetch(w.start, w.end)
	if err != nil {
		return err
	}
	lo, hi := w.start.UnixNano(), w.end.UnixNano()
	var results []traceResult
	for _, spans := range traces {
		r, ok := analyzeTrace(spans, t.model)
		if ok && r.start >= lo && r.start < hi {
			results = append(results, r)
		}
	}
	truncated := 0
	if len(traces) >= t.depth {
		truncated = 1
	}
	rows := windowRows(results)
	for _, r := range rows {
		rec := []string{strconv.Itoa(w.idx), strconv.Itoa(w.level), strconv.Itoa(w.wi), strconv.Itoa(w.clients),
			strconv.FormatInt(w.start.UnixMicro(), 10), strconv.FormatInt(w.end.UnixMicro(), 10),
			r.route, r.daemon, f4(r.share), f4(r.tail), f4(r.rpc), f3(r.p50), f3(r.p95), f3(r.mean),
			strconv.Itoa(r.spans), strconv.Itoa(r.samples), f3(r.rootP50), f3(r.rootP95),
			strconv.Itoa(len(traces)), strconv.Itoa(truncated)}
		if err := t.csv.Write(rec); err != nil {
			return err
		}
	}
	t.csv.Flush()
	if t.out != nil {
		var b strings.Builder
		for i := 0; i < len(rows); {
			j := i
			fmt.Fprintf(&b, "cp window %d clients %d route %s samples %d root_p95_ms %s", w.idx, w.clients, rows[i].route, rows[i].samples, f3(rows[i].rootP95))
			for ; j < len(rows) && rows[j].route == rows[i].route; j++ {
				if rows[j].share >= 0.005 {
					fmt.Fprintf(&b, " %s=%s", rows[j].daemon, f3(rows[j].share))
				}
			}
			b.WriteByte('\n')
			i = j
		}
		if b.Len() > 0 {
			io.WriteString(t.out, b.String())
		}
	}
	t.mu.Lock()
	t.windows++
	t.samples += len(results)
	t.trunc += truncated
	t.mu.Unlock()
	return t.csv.Error()
}
