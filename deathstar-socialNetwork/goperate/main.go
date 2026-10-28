// Command goperate drives the DeathStarBench social network under Mercury,
// mirroring deathstar-hotelReservations/operate.
//
// Mercury is started separately (sudo ~/Mercury/build/mercury 8080). The
// services are the C++ Thrift binaries in ../bin, launched through ../run/
// so they find config/service-config.json. There is no nginx front end; the
// load generator speaks Thrift (framed, binary) to the services directly.
//
//	goperate -mercury 127.0.0.1:8080 -cpu 2 start      start all of mercury-social.txt, leave running
//	goperate -mercury 127.0.0.1:8080 stop              kill every daemon in the list
//	goperate -mercury 127.0.0.1:8080 -cpu 2 init       start, load -graph users+edges into MongoDB, stop
//	goperate -mercury 127.0.0.1:8080 -cpu 2 autoscale  start, ramp load with saturate, stop
//
// autoscale gives every daemon a home core (CPU 1 upward), attaches saturate
// 1..16, and drives -routes together from 1 user to -users and back down,
// four 1 s windows per step. -client mercury (default) runs the generator as
// Mercury daemon social-loadgen (bin/loadgen, same binary with
// OPERATE_ROLE=loadgen) homed on -gen-cpu with saturate -gen-min..-gen-max;
// -client inproc pins this process to CPUs 60-63 instead.
//
// init must have run once against the same MongoDB before autoscale, so the
// users user<id>/password and their follow edges exist.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

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
	users := flag.Int("users", 1024, "maximum concurrent users per route (scaled by -mix)")
	file := flag.String("file", benchmarksRoot()+"/mercury-social.txt", "deployment list")
	configPath := flag.String("config", socialRoot()+"/config/service-config.json", "service-config.json")
	graph := flag.String("graph", defaultGraph(), "social graph .edges file (init loads it; autoscale draws user ids from it)")
	routes := flag.String("routes", defaultRoutes, "comma-separated routes: compose-post,read-home-timeline,read-user-timeline,follow,login")
	mix := flag.String("mix", "", "route weights, e.g. compose-post=0.1,read-home-timeline=0.6,read-user-timeline=0.3 (DSB mixed workload); empty = every route runs -users")
	initWorkers := flag.Int("init-workers", 8, "parallel connections for init and seed")
	seedPerUser := flag.Int("seed-posts", 10, "seed: posts composed per user after the graph is registered")
	client := flag.String("client", clientMercury, "autoscale load generator: mercury (bin/loadgen as a Mercury daemon) or inproc (this process pinned to CPUs 60-63)")
	histFlag := flag.Bool("hist", false, "autoscale: write every window's latency histogram per route to <gen-out stem>.hist.jsonl (deadline miss rates offline)")
	conns := flag.Int("conns", 0, "Thrift connections per route shared by its workers; 0 = one per worker. A Thrift connection carries one call at a time, so N>0 also caps in-flight calls per route at N")
	genBin := flag.String("gen-bin", socialRoot()+"/goperate/bin/loadgen", "loadgen binary for -client mercury")
	genOut := flag.String("gen-out", "", "loadgen output file (default output/loadgen-<unix>.out)")
	genCPU := flag.Int("gen-cpu", defaultGenCPU, "loadgen home Linux CPU")
	genMin := flag.Int("gen-min", defaultGenMin, "loadgen saturate min cores")
	genMax := flag.Int("gen-max", defaultGenMax, "loadgen saturate max cores")
	genProcs := flag.Int("gen-procs", defaultGenProcs, "loadgen GOMAXPROCS (keep >= gen-max so busy threads can exceed cores)")
	satOnly := flag.String("saturate-only", "", "comma-separated program names kept on saturate when -policy gnn (e.g. rabbitmq-server)")
	pin := flag.Bool("pin", true, "with -policy gnn: pin busy threads to CPUs (false = only size cpusets)")
	window := flag.Int("window-ms", 1000, "autoscale measurement window in ms (4 windows per user step)")
	policy := flag.String("policy", "saturate", "autoscale policy for the service daemons: saturate or gnn (the load generator stays on saturate)")
	tickMs := flag.Int("tick-ms", 10, "gnn decision tick in ms (sent with -policy gnn)")
	svcMin := flag.Int("svc-min", 1, "autoscale: per-service min cores (min = max: fixed allocation)")
	svcMax := flag.Int("svc-max", autoMaxCores, "autoscale: per-service max cores")
	svcFixedFlag := flag.String("svc-fixed", "", "autoscale: per-program fixed cores, e.g. geo=1,mongod=2 (min = max for those daemons)")
	placement := flag.String("placement", "least_loaded", "core hand-out for /scale: least_loaded or nearest_first")
	addLoadFlags("social") // -load sine, -users-min/-max, -period, -phase, -step, -duration, -ramp-in, -homes: demoload.go
	runDir := flag.String("run-dir", "", "working directory for the C++ services (config/ written there; default <gen-out dir>/social-cwd when -jaeger is on or -config is not the repo's)")
	tracing = addJaegerFlags(root) // -jaeger*, -trace-*: see jaegercp.go and jaeger_app.go
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: goperate [flags] start|stop|init|autoscale\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if err := tracing.validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if flag.NArg() != 1 {
		flag.Usage()
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
	for _, n := range strings.Split(*satOnly, ",") {
		if n != "" {
			saturateOnly[n] = true
		}
	}
	servicePolicy, serviceTickMs, servicePlacement = *policy, *tickMs, *placement
	cmd := flag.Arg(0)
	switch cmd {
	case "start", "stop", "init", "seed", "autoscale":
	default:
		flag.Usage()
		os.Exit(2)
	}
	if *client != clientMercury && *client != clientInproc {
		fmt.Fprintf(os.Stderr, "-client must be %s or %s\n", clientMercury, clientInproc)
		os.Exit(2)
	}
	if *conns < 0 || *genMin < 1 || *genMax < *genMin || *genProcs < 1 || *users < 1 || *initWorkers < 1 {
		fmt.Fprintln(os.Stderr, "need -users >= 1, -conns >= 0, 1 <= -gen-min <= -gen-max, -gen-procs >= 1, -init-workers >= 1")
		os.Exit(2)
	}
	names, err := parseRoutes(*routes)
	if err == nil {
		_, err = parseMix(*mix, names)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if *svcMin < 1 || *svcMax < *svcMin {
		fmt.Fprintln(os.Stderr, "need 1 <= -svc-min <= -svc-max")
		os.Exit(2)
	}
	svcMinCores, svcMaxCores = *svcMin, *svcMax
	if err := parseSvcFixed(*svcFixedFlag); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	connsPerRoute = *conns
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
		tracing.out = tracing.csvPath(*genOut, socialOutputDir())
	}
	if *runDir != "" {
		socialCwd = *runDir
	} else if *configPath != snRoot+"/config/service-config.json" && cmd != "stop" {
		// The services read config/service-config.json from their cwd: a moved -config (demo: MongoDB on 27018) needs a run dir.
		if *client == clientMercury && *genOut == "" {
			*genOut = defaultGenOut()
		}
		base := *genOut
		if base == "" {
			base = filepath.Join(socialOutputDir(), "x")
		}
		socialCwd = filepath.Join(filepath.Dir(base), "social-cwd")
	}
	opts := clientOpts{
		mode: *client, bin: *genBin, out: *genOut, config: *configPath, file: *file, graph: *graph,
		routes: *routes, mix: *mix, homeCPU: *genCPU, genMin: *genMin, genMax: *genMax,
		genProcs: *genProcs, conns: *conns,
	}
	seedPerUserN = *seedPerUser
	if err := run(*mercury, *cpu, *users, *initWorkers, cmd, opts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// seedPerUserN is -seed-posts (posts per user for the seed command).
var seedPerUserN = 10

func run(mercury string, cpu, users, initWorkers int, cmd string, opts clientOpts) error {
	lines, err := parseFile(opts.file)
	if err != nil {
		return err
	}
	cfg, err := loadConfig(opts.config)
	if err != nil {
		return err
	}
	if cmd == "init" || cmd == "seed" || cmd == "autoscale" {
		if _, err := loadGraph(opts.graph); err != nil {
			return fmt.Errorf("graph: %w", err)
		}
	}
	base := mercuryURL(mercury)
	if err := ping(base); err != nil {
		return err
	}
	if cmd == "stop" {
		if tracing.enabled() && tracing.mode == jaegerModeMercury {
			_ = kill(base, "social-jaeger")
		}
		return stopAll(base, lines)
	}
	if cpu < 1 {
		return fmt.Errorf("-cpu is required: pass a daemon CPU from the mercury command line")
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

	if !tracing.enabled() && socialCwd != "" {
		if err := prepareSocialRunDir(socialRunDir(), opts.config); err != nil {
			return err
		}
		fmt.Printf("social cwd %s (config/service-config.json -> %s)\n", socialRunDir(), opts.config)
	}
	if tracing.enabled() {
		if cmd == "start" && tracing.mode == jaegerModeBare {
			fmt.Fprintln(os.Stderr, "warning: -jaeger-mode bare Jaeger stops when goperate exits; use mercury or external with start")
		}
		if err := prepareSocialRunDir(socialRunDir(), opts.config); err != nil {
			return err
		}
		fmt.Printf("social cwd %s (config/jaeger-config.yml: probabilistic %s)\n", socialRunDir(), tracing.sampleString())
		if jaeger, err = startJaeger(tracing, base, cpu, "social", jaegerLogPath(), track); err != nil {
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
		if addr != "" {
			if err := portFree(addr); err != nil {
				stopStarted()
				return fmt.Errorf("%s: %w", line.id, err)
			}
		}
		env := addTraceEnv(line, daemonEnv(line))
		pid, err := start(base, line, cpu, env)
		if err != nil {
			stopStarted()
			return fmt.Errorf("%s: %w", line.id, err)
		}
		fmt.Printf("started %s daemon_id=%s pid %d %s\n", filepath.Base(line.path), line.id, pid, formatEnv(env))
		track(line)
		if addr == "" {
			// WriteHomeTimelineService only consumes RabbitMQ; require it to stay up.
			time.Sleep(time.Second)
			if !processAlive(pid) {
				stopStarted()
				return fmt.Errorf("%s exited right after start", line.id)
			}
			continue
		}
		if err := waitPort(addr, 90*time.Second); err != nil {
			stopStarted()
			return fmt.Errorf("%s did not listen on %s: %w", line.id, addr, err)
		}
		if filepath.Base(line.path) == "redis-server" {
			// Redis listens while it still loads its dataset and answers -LOADING until done.
			if err := waitRedis(addr, 120*time.Second); err != nil {
				stopStarted()
				return fmt.Errorf("%s not ready on %s: %w", line.id, addr, err)
			}
		}
	}

	if cmd == "start" {
		fmt.Printf("left %d daemons running\n", len(lines))
		return nil
	}
	snapshot := func() []tuple {
		startedMu.Lock()
		defer startedMu.Unlock()
		return append([]tuple(nil), started...)
	}
	switch cmd {
	case "init":
		err = initGraph(cfg, opts.graph, initWorkers)
	case "seed": // init, then posts: the state collect/test runs restore (Mercury src/test/dsb_seed.sh)
		if err = initGraph(cfg, opts.graph, initWorkers); err == nil {
			err = seedPosts(cfg, opts.graph, seedPerUserN, initWorkers)
		}
		if err == nil { // redis runs with --save "": persist it before the daemons stop, after the fan-out settles
			err = saveRedis(cfg)
		}
	case "autoscale":
		err = autoscaleServices(base, cfg, snapshot(), users, opts, track)
	}
	stopStarted()
	return err
}
