// Command mediaoperate drives the DeathStarBench media microservices under Mercury, with the same flags and output as
// the hotel and social operators, so the comparison harness (src/test/dsb_ab.sh, compare_systems.sh) drives it the same
// way.
//
// Mercury is started separately (or by dsb_ab.sh). The stores (MongoDB on 27018, memcached on 11300-11306, redis on
// 6379) and the 13 C++ Thrift services are the daemons in benchmarks/mercury-media.txt, named media-<program>. The
// services run from deathstar-mediaMicroservices/run/, which holds the standalone config (run/config, written by
// scripts/standalone_config.py). The stores must already hold the seeded data.
//
//	mediaoperate -mercury 127.0.0.1:8080 -cpu 2 start          start the stack, leave it running
//	mediaoperate -mercury 127.0.0.1:8080 stop                  kill every daemon in the list
//	mediaoperate -mercury 127.0.0.1:8080 -cpu 2 autoscale      start, attach the policy to every daemon, drive the
//	                                                           routes with the levels, stop (run is the same)
//
// Routes (-routes, default all): read-page, compose-review, read-movie-reviews (routes.go). -mix sets each route's share
// of a level. -arrivals poisson (default) is open loop at -levels req/s; -arrivals closed holds -levels users. The
// output (-gen-out, default out/loadgen.out) gets the route lines and "loadgen done"; its stem gets .hist.jsonl, the
// latency histograms per route per window.
//
// -policy saturate|gnn with -placement, -tick-ms, -pin and -svc-min/-svc-max (or -svc-fixed) sets each daemon's Mercury
// policy. -client mercury (default) runs the generator as Mercury daemon media-loadgen (bin/loadgen, OPERATE_ROLE=loadgen)
// homed on -gen-cpu with saturate -gen-min..-gen-max; -client inproc runs it in this process, unpinned.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	loadgenID     = "media-loadgen"
	clientMercury = "mercury"
	clientInproc  = "inproc"
	mongoAddr     = "127.0.0.1:27018"
	redisAddr     = "127.0.0.1:6379"
	defaultGenCPU = 60
	defaultGenMin = 4
	defaultGenMax = 8
	defaultProcs  = 16
)

// benchmarksRoot is the benchmarks directory: the parent of deathstar-mediaMicroservices, where mercury-media.txt and
// the runtime stores live.
func benchmarksRoot() string { return filepath.Dir(mediaRoot()) }

// clientOpts is everything the run command needs besides the load itself.
type clientOpts struct {
	client   string
	bin      string
	out      string // -gen-out: the loadgen output file; its stem gets .hist.jsonl
	genCPU   int
	genMin   int
	genMax   int
	genProcs int
	load     loadOpts
}

func main() {
	if os.Getenv(envRole) == roleGen {
		if err := runLoadgen(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	mercury := flag.String("mercury", "127.0.0.1:8080", "Mercury host:port")
	cpu := flag.Int("cpu", 0, "daemon CPU number Mercury was given (not the CPU Mercury pinned itself to)")
	file := flag.String("file", benchmarksRoot()+"/mercury-media.txt", "deployment list")
	config := flag.String("config", mediaRoot()+"/run/config/service-config.json", "service-config.json the services read")
	movies := flag.String("movies", mediaRoot()+"/datasets/tmdb/movies.json", "movies the seed registered; read-page picks one at random")
	routes := flag.String("routes", strings.Join(routeNames, ","), "routes to drive: "+strings.Join(routeNames, ", "))
	mix := flag.String("mix", "", "route weights, e.g. compose-review=0.2,read-page=0.8 (1 = the full level); empty = every route at the full level")
	users := flag.Int("users", 1000, "seeded usernames, username_1..N, that compose-review writes as")
	entry := flag.String("entry", "", "accepted for the harness's hotel and social flags; the media benchmark has no front end, so it must stay empty")
	flag.Bool("hist", true, "accepted for the harness; the histograms are always written")
	arrivals := flag.String("arrivals", arrPoisson, "poisson (open loop, levels are req/s) or closed (levels are users)")
	load := flag.String("load", "list", "load shape: list (-levels, each held for -step); the only shape the media benchmark has")
	levels := flag.String("levels", "50", "comma-separated levels, each held for -step")
	step := flag.Duration("step", 10*time.Second, "time per level")
	windowMs := flag.Int("window-ms", 1000, "measurement window in ms")
	timeout := flag.Duration("req-timeout", 0, "per-request timeout, counted as an error (0: 2s poisson, 10s closed)")
	inflight := flag.Int("inflight", 4096, "poisson: requests in flight per route; an arrival beyond it counts as an error")
	seed := flag.Int64("seed", 1, "seed for arrival times and movie choice")
	client := flag.String("client", clientMercury, "load generator: mercury (bin/loadgen as a Mercury daemon) or inproc (this process)")
	genBin := flag.String("gen-bin", mediaRoot()+"/goperate/bin/loadgen", "loadgen binary for -client mercury")
	genOut := flag.String("gen-out", "", "loadgen output file (default out/media-<unix>/loadgen.out)")
	genCPU := flag.Int("gen-cpu", defaultGenCPU, "loadgen home Linux CPU")
	genMin := flag.Int("gen-min", defaultGenMin, "loadgen saturate min cores")
	genMax := flag.Int("gen-max", defaultGenMax, "loadgen saturate max cores")
	genProcs := flag.Int("gen-procs", defaultProcs, "loadgen GOMAXPROCS")
	policy := flag.String("policy", "saturate", "policy for the service daemons: saturate or gnn (the load generator stays on saturate)")
	placement := flag.String("placement", "least_loaded", "core hand-out for /scale: least_loaded or nearest_first")
	tickMs := flag.Int("tick-ms", 10, "gnn decision tick in ms (sent with -policy gnn)")
	pin := flag.Bool("pin", true, "with -policy gnn: pin busy threads to CPUs (false = only size cpusets)")
	svcMin := flag.Int("svc-min", 1, "autoscale: per-daemon min cores")
	svcMax := flag.Int("svc-max", 16, "autoscale: per-daemon max cores")
	svcFixedFlag := flag.String("svc-fixed", "", "autoscale: per-program fixed cores, e.g. PageService=2,mongod=2 (min = max for those daemons)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: mediaoperate [flags] start|stop|autoscale|run\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	cmd := flag.Arg(0)
	if cmd != "start" && cmd != "stop" && cmd != "autoscale" && cmd != "run" {
		flag.Usage()
		os.Exit(2)
	}
	if *client != clientMercury && *client != clientInproc {
		fmt.Fprintf(os.Stderr, "-client must be %s or %s\n", clientMercury, clientInproc)
		os.Exit(2)
	}
	if *entry != "" {
		fmt.Fprintln(os.Stderr, "-entry must be empty: the media benchmark has no front end, the load generator calls the services")
		os.Exit(2)
	}
	if *load != "list" {
		fmt.Fprintf(os.Stderr, "-load must be list, got %q\n", *load)
		os.Exit(2)
	}
	if *policy != "saturate" && *policy != "gnn" || *placement != "least_loaded" && *placement != "nearest_first" || *tickMs < 10 {
		fmt.Fprintln(os.Stderr, "need -policy saturate|gnn, -placement least_loaded|nearest_first, -tick-ms >= 10")
		os.Exit(2)
	}
	if *svcMin < 1 || *svcMax < *svcMin || *windowMs < 1 {
		fmt.Fprintln(os.Stderr, "need 1 <= -svc-min <= -svc-max and -window-ms >= 1")
		os.Exit(2)
	}
	if err := parseSvcFixed(*svcFixedFlag); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	svcMinCores, autoMaxCores = *svcMin, *svcMax
	servicePolicy, serviceTickMs, servicePlacement, servicePin = *policy, *tickMs, *placement, *pin

	lv, err := parseLevels(*levels)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	names, err := parseRoutes(*routes)
	var mixWeights map[string]float64
	if err == nil {
		mixWeights, err = parseMix(*mix, names)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if *timeout == 0 {
		*timeout = 10 * time.Second
		if *arrivals == arrPoisson {
			*timeout = 2 * time.Second
		}
	}
	loadOpt := loadOpts{
		arrivals: *arrivals, routes: names, mix: mixWeights, levels: lv, step: *step,
		window: time.Duration(*windowMs) * time.Millisecond, timeout: *timeout,
		inflight: *inflight, seed: *seed, users: *users, config: *config, movies: *movies,
	}
	if cmd == "autoscale" || cmd == "run" {
		if err := loadOpt.check(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
	opts := clientOpts{
		client: *client, bin: *genBin, out: *genOut, genCPU: *genCPU, genMin: *genMin, genMax: *genMax,
		genProcs: *genProcs, load: loadOpt,
	}
	if err := run(*mercury, *cpu, *file, cmd, opts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseLevels(spec string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(spec, ",") {
		v, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || v < 1 {
			return nil, fmt.Errorf("bad level %q in -levels %q", part, spec)
		}
		out = append(out, v)
	}
	return out, nil
}

// listenAddr is where a daemon listens. Numeric names are memcached instances (run/<port>), the others are the
// stores and the Thrift services, which take their port from the config.
func listenAddr(line tuple, cfg serviceConfig) (string, error) {
	base := filepath.Base(line.path)
	if _, err := strconv.Atoi(base); err == nil {
		return "127.0.0.1:" + base, nil
	}
	switch base {
	case "mongod":
		return mongoAddr, nil
	case "redis-server":
		return redisAddr, nil
	}
	return cfg.address(serviceKey(base))
}

func run(mercury string, cpu int, file, cmd string, opts clientOpts) error {
	lines, err := parseFile(file)
	if err != nil {
		return err
	}
	cfg, err := loadConfig(opts.load.config)
	if err != nil {
		return err
	}
	base := mercuryURL(mercury)
	if err := ping(base); err != nil {
		return err
	}
	if cmd == "stop" {
		return stopAll(base, lines)
	}
	if cpu < 1 {
		return fmt.Errorf("-cpu is required: pass a daemon CPU from the mercury command line")
	}

	var started []tuple
	stopStarted := func() {
		for i := len(started) - 1; i >= 0; i-- {
			if err := kill(base, started[i].id); err != nil {
				fmt.Fprintf(os.Stderr, "kill %s: %v\n", started[i].id, err)
			}
		}
		started = nil
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
		pid, err := start(base, line, cpu, map[string]string{})
		if err != nil {
			stopStarted()
			return fmt.Errorf("%s: %w", line.id, err)
		}
		fmt.Printf("started %s daemon_id=%s pid %d\n", filepath.Base(line.path), line.id, pid)
		started = append(started, line)
		if err := waitPort(addr, 90*time.Second); err != nil {
			stopStarted()
			return fmt.Errorf("%s did not listen on %s: %w", line.id, addr, err)
		}
	}
	if cmd == "start" {
		fmt.Printf("left %d daemons running\n", len(lines))
		return nil
	}
	if cmd == "autoscale" || cmd == "run" {
		for _, line := range lines {
			mn, mx := svcRange(line, svcMinCores, autoMaxCores)
			if err := enableScale(base, line.id, servicePolicy, mn, mx); err != nil {
				stopStarted()
				return err
			}
		}
		fmt.Printf("policy %s placement %s tick_ms %d min %d max %d\n", servicePolicy, servicePlacement, serviceTickMs,
			svcMinCores, autoMaxCores)
	}
	err = runLoad(base, opts, &started)
	stopStarted()
	return err
}

// runLoad drives one run. started collects the daemons it starts so the caller can stop them.
func runLoad(base string, opts clientOpts, started *[]tuple) error {
	outPath := opts.out
	if outPath == "" {
		outPath = filepath.Join(mediaRoot(), "output", fmt.Sprintf("media-%d", time.Now().Unix()), "loadgen.out")
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}
	stem := strings.TrimSuffix(outPath, filepath.Ext(outPath))
	histPath := stem + ".hist.jsonl"
	o := opts.load
	o.id = loadgenID
	if opts.client == clientInproc {
		f, err := os.Create(outPath)
		if err != nil {
			return err
		}
		defer f.Close()
		h, err := os.Create(histPath)
		if err != nil {
			return err
		}
		defer h.Close()
		w := io.MultiWriter(f, os.Stdout)
		return driveLoad(w, h, o)
	}
	return runMercuryLoadgen(base, opts, o, outPath, histPath, started)
}

// runMercuryLoadgen starts bin/loadgen under Mercury, homes it on -gen-cpu with the saturate policy, and copies its
// output to stdout until it reports done or fails.
func runMercuryLoadgen(base string, opts clientOpts, o loadOpts, outPath, histPath string, started *[]tuple) error {
	if st, err := os.Stat(opts.bin); err != nil || st.Mode()&0o111 == 0 {
		return fmt.Errorf("loadgen binary %s is missing or not executable (cd goperate && go build -o bin/loadgen .)", opts.bin)
	}
	os.Remove(outPath)
	os.Remove(histPath)
	env := o.env()
	env[envOut] = outPath
	env[envHist] = histPath
	gen := tuple{path: opts.bin, hostname: "localhost", id: loadgenID}
	pid, err := start(base, gen, opts.genCPU, env)
	if err != nil {
		return fmt.Errorf("%s: %w", loadgenID, err)
	}
	*started = append(*started, gen)
	fmt.Printf("started loadgen daemon_id=%s pid %d out %s\n", loadgenID, pid, outPath)
	if err := setCpus(base, loadgenID, strconv.Itoa(opts.genCPU)); err != nil {
		return err
	}
	if err := enableSaturate(base, loadgenID, opts.genMin, opts.genMax); err != nil {
		return err
	}
	fmt.Printf("policy loadgen saturate min %d max %d home %d\n", opts.genMin, opts.genMax, opts.genCPU)
	return tailLoadgen(outPath, func() bool { return processAlive(pid) }, os.Stdout, 100*time.Millisecond)
}

// tailLoadgen copies complete lines from path to w until it sees doneLine (nil) or errorLine (error), or until alive
// reports false and no more data arrives (error).
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
	var partial strings.Builder
	buf := make([]byte, 64*1024)
	dead := false
	for {
		n, err := f.Read(buf)
		if n > 0 {
			partial.Write(buf[:n])
			for {
				s := partial.String()
				i := strings.IndexByte(s, '\n')
				if i < 0 {
					break
				}
				line := s[:i+1]
				partial.Reset()
				partial.WriteString(s[i+1:])
				if _, werr := io.WriteString(w, line); werr != nil {
					return werr
				}
				trimmed := strings.TrimSpace(line)
				if trimmed == doneLine {
					return nil
				}
				if strings.HasPrefix(trimmed, errorLine) {
					return fmt.Errorf("%s", trimmed)
				}
			}
			continue
		}
		if err != nil && err != io.EOF {
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
