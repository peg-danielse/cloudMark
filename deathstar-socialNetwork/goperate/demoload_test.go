package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func sineOpts() *loadOpts {
	return &loadOpts{mode: loadSine, usersMin: 300, usersMax: 600, period: 10 * time.Minute, step: 5 * time.Second}
}

func TestSineUsersAt(t *testing.T) {
	o := sineOpts()
	cases := []struct {
		at   time.Duration
		want int
	}{
		{0, 450}, {150 * time.Second, 600}, {300 * time.Second, 450}, {450 * time.Second, 300}, {600 * time.Second, 450},
		{50 * time.Second, 525}, // 450 + 150*sin(30deg)
	}
	for _, c := range cases {
		if got := o.usersAt(c.at, c.at); got != c.want {
			t.Errorf("usersAt(%s) = %d, want %d", c.at, got, c.want)
		}
	}
	o.phase = 180
	if got := o.usersAt(150*time.Second, 150*time.Second); got != 300 {
		t.Errorf("phase 180 at quarter period = %d, want 300", got)
	}
	o.phase = 270
	if got := o.usersAt(0, 0); got != 300 {
		t.Errorf("phase 270 at 0 = %d, want 300", got)
	}
}

func TestSineBoundsAndRampIn(t *testing.T) {
	o := sineOpts()
	o.rampIn = 60 * time.Second
	if got := o.usersAt(0, 0); got != 1 {
		t.Errorf("ramp-in start = %d, want 1", got)
	}
	if got := o.usersAt(30*time.Second, 0); got != 226 { // 1 + (450-1)/2 = 225.5
		t.Errorf("ramp-in half = %d, want 226", got)
	}
	if got := o.usersAt(60*time.Second, 0); got != 450 {
		t.Errorf("ramp-in end = %d, want 450", got)
	}
	o.rampIn = 0
	for s := 0; s < 1200; s += 7 {
		d := time.Duration(s) * time.Second
		if u := o.usersAt(d, d); u < 300 || u > 600 {
			t.Fatalf("usersAt(%s) = %d out of [300,600]", d, u)
		}
	}
	if o.total() != 10*time.Minute {
		t.Errorf("default total %s", o.total())
	}
	o.rampIn, o.duration = time.Minute, 0
	if o.total() != 11*time.Minute {
		t.Errorf("total with ramp-in %s", o.total())
	}
}

// fakeClock advances by the time each window would take.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func TestSineLevelsFollowClock(t *testing.T) {
	o := sineOpts()
	o.period, o.step, o.duration = 60*time.Second, 5*time.Second, 60*time.Second
	c := &fakeClock{t: time.Unix(1_800_000_000, 0)}
	next := o.levels(o.workers(1024), 1000, c.now)
	var got []int
	for {
		l, ok := next()
		if !ok {
			break
		}
		if l.windows != 5 || !l.sine || l.level != len(got) {
			t.Fatalf("level %+v", l)
		}
		got = append(got, l.clients)
		c.t = c.t.Add(time.Duration(l.windows) * 1010 * time.Millisecond) // windows run a little long
	}
	// 60 s / 5.05 s per step = 12 steps; step k starts at 5.05k s.
	if len(got) != 12 {
		t.Fatalf("got %d steps %v", len(got), got)
	}
	if got[0] != 450 || got[3] != 600 || got[9] != 300 {
		t.Errorf("schedule %v", got)
	}
	if o.workers(1024) != 600 {
		t.Errorf("workers %d", o.workers(1024))
	}
}

func TestSineEpochAlignsTwoApps(t *testing.T) {
	a, b := sineOpts(), sineOpts()
	b.phase = 180
	epoch := time.Unix(1_800_000_000, 0)
	a.epochMs, b.epochMs = epoch.UnixMilli(), epoch.UnixMilli()
	// a starts 150 s after the epoch, b 20 s later: both read the same wall clock.
	ca := &fakeClock{t: epoch.Add(150 * time.Second)}
	cb := &fakeClock{t: epoch.Add(170 * time.Second)}
	la, _ := a.levels(600, 1000, ca.now)()
	lb, _ := b.levels(600, 1000, cb.now)()
	if la.clients != 600 || lb.clients != 303 { // b: 450 - 150*sin(2pi*170/600) = 303.3
		t.Errorf("aligned levels a=%d b=%d", la.clients, lb.clients)
	}
	if la.clients+b.usersAt(0, 150*time.Second) != 900 {
		t.Errorf("anti-phase sum %d", la.clients+b.usersAt(0, 150*time.Second))
	}
}

func TestRampLevelsUnchanged(t *testing.T) {
	o := &loadOpts{mode: loadRamp}
	next := o.levels(8, 1000, time.Now)
	var got []int
	for {
		l, ok := next()
		if !ok {
			break
		}
		if l.windows != autoWindows || l.sine || l.suffix() != "" {
			t.Fatalf("ramp level %+v", l)
		}
		got = append(got, l.clients)
	}
	want := rampClients(8)
	if len(got) != len(want) {
		t.Fatalf("ramp %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ramp %v want %v", got, want)
		}
	}
}

func TestLoadEncodeDecodeAndValidate(t *testing.T) {
	o := sineOpts()
	o.phase, o.rampIn, o.duration, o.epochMs = 180, 30*time.Second, 20*time.Minute, 1_800_000_000_123
	var d loadOpts
	if err := d.decode(o.encode()); err != nil {
		t.Fatal(err)
	}
	if d.mode != loadSine || d.usersMin != 300 || d.usersMax != 600 || d.period != o.period || d.step != o.step ||
		d.duration != o.duration || d.rampIn != o.rampIn || d.phase != 180 || d.epochMs != o.epochMs {
		t.Errorf("round trip %+v", d)
	}
	if err := d.decode("sine,1,2"); err == nil {
		t.Error("short LOADGEN_LOAD accepted")
	}
	if err := sineOpts().validate(1000); err != nil {
		t.Error(err)
	}
	bad := []func(*loadOpts){
		func(o *loadOpts) { o.mode = "square" },
		func(o *loadOpts) { o.usersMin = 0 },
		func(o *loadOpts) { o.usersMax = 100 },
		func(o *loadOpts) { o.step = 500 * time.Millisecond },
		func(o *loadOpts) { o.period = 8 * time.Second },
		func(o *loadOpts) { o.rampIn = -time.Second },
		func(o *loadOpts) { o.homes = "3-1" },
	}
	for i, f := range bad {
		o := sineOpts()
		f(o)
		if o.validate(1000) == nil {
			t.Errorf("bad option %d accepted", i)
		}
	}
}

func TestUsersMeanAmp(t *testing.T) {
	o := &loadOpts{mode: loadSine, usersMean: 100, period: time.Minute, step: 5 * time.Second}
	if err := o.validate(1000); err != nil || o.usersMin != 50 || o.usersMax != 150 {
		t.Fatalf("mean 100: %v %d-%d", err, o.usersMin, o.usersMax)
	}
	if u := o.usersAt(15*time.Second, 15*time.Second); u != 150 {
		t.Errorf("peak %d", u)
	}
	o = &loadOpts{mode: loadSine, usersMean: 100, usersAmp: 20, period: time.Minute, step: 5 * time.Second}
	if err := o.validate(1000); err != nil || o.usersMin != 80 || o.usersMax != 120 {
		t.Fatalf("mean 100 amp 20: %v %d-%d", err, o.usersMin, o.usersMax)
	}
	for _, bad := range []*loadOpts{
		{mode: loadSine, usersMean: 100, usersAmp: 100, period: time.Minute, step: 5 * time.Second},
		{mode: loadSine, usersMin: 50, usersMax: 150, usersAmp: 10, period: time.Minute, step: 5 * time.Second},
	} {
		if bad.validate(1000) == nil {
			t.Errorf("accepted %+v", bad)
		}
	}
}

func TestListHomes(t *testing.T) {
	h, err := listHomes("16-18,40", 6)
	if err != nil {
		t.Fatal(err)
	}
	want := []int{16, 17, 18, 40, 16, 17}
	for i := range want {
		if h[i] != want[i] {
			t.Fatalf("homes %v want %v", h, want)
		}
	}
}

func TestWindowLog(t *testing.T) {
	dir := t.TempDir()
	csvPath, status := filepath.Join(dir, "w.csv"), filepath.Join(dir, "status")
	var out bytes.Buffer
	wl, err := newWindowLog("hotel", &out, csvPath, status)
	if err != nil {
		t.Fatal(err)
	}
	wl.now = func() time.Time { return time.Unix(1_800_000_000, 0) }
	line := func(route string, rate int, p95 float64, step, users int) string {
		return "route " + route + " clients " + itoa(users) + " arrival_rate " + itoa(rate) + " service_rate " + itoa(rate) +
			" avg_us 900.0 p95_us " + ftoa(p95) + " p99_us 9000.0 errors 0 cpus 11211=1 mongod=2-4 geo=3-5 gen_cpus 28,30 gen_busy_ms 10 gen_resized 0 gen_bound 0" +
			" load sine step " + itoa(step) + " target_users " + itoa(users) + " t_s " + itoa(step*5) + ".0\n"
	}
	// split writes across line boundaries
	text := line("search", 1000, 2000, 0, 450) + line("geo", 3000, 1000, 0, 450) +
		"cp window 1 clients 450 route search samples 20 root_p95_ms 2.1 mongod=0.385 rate=0.304 geo=0.185 11212=0.117 x=0.009\n" +
		line("search", 1100, 4000, 1, 500)
	for i := 0; i < len(text); i += 37 {
		j := i + 37
		if j > len(text) {
			j = len(text)
		}
		wl.Write([]byte(text[i:j]))
	}
	st, err := os.ReadFile(status)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"app hotel\n", "window 1\n", "target_users 450\n", "rate 4000\n", "p95_ms_max 2.00 search\n",
		"p95_ms_wmean 1.25\n", "cores 5\n", "core_sum 7\n", "gen_cores 2\n"} {
		if !strings.Contains(string(st), want) {
			t.Errorf("status lacks %q:\n%s", want, st)
		}
	}
	wl.Write([]byte(doneLine + "\n"))
	st, _ = os.ReadFile(status)
	if !strings.Contains(string(st), "window 2\n") || !strings.Contains(string(st), "cp search mongod=0.385 rate=0.304 geo=0.185 11212=0.117\n") {
		t.Errorf("status after done:\n%s", st)
	}
	if err := wl.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), "route search") || strings.Count(out.String(), "demo app hotel window ") != 2 ||
		!strings.Contains(out.String(), "demo app hotel window 1 t_s 0.0 target_users 450 rate 4000 p95_ms_max 2.00 search p95_ms_wmean 1.25 errors 0 cores 5 core_sum 7 gen_cores 2\n") {
		t.Errorf("output:\n%s", out.String())
	}
	csv, _ := os.ReadFile(csvPath)
	rows := strings.Split(strings.TrimSpace(string(csv)), "\n")
	if len(rows) != 4 || rows[0]+"\n" != windowCSVHeader ||
		rows[1] != "1800000000000,hotel,1,0,0.0,450,search,450,1000,1000,900.0,2000.0,9000.0,0,5,7,2" ||
		!strings.HasPrefix(rows[3], "1800000000000,hotel,2,1,5.0,500,search,500,") {
		t.Errorf("csv:\n%s", csv)
	}
}

func TestParseRampRouteLine(t *testing.T) {
	r, ok := parseRouteLine("route geo clients 8 arrival_rate 10 service_rate 9 avg_us 1.0 p95_us 2.0 p99_us 3.0 errors 1 cpus geo=? rate=7 gen_cpus ? gen_busy_ms -1 gen_resized 0 gen_bound 0")
	if !ok || r.sine || r.target != 8 || r.cores != 1 || r.gen != 0 || r.err != 1 || r.service != 9 {
		t.Errorf("ramp line %+v", r)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

func ftoa(f float64) string { return itoa(int(f)) + ".0" }

func TestLoadV2RoundTrip(t *testing.T) {
	o := &loadOpts{mode: loadList, arrivals: arrPoisson, levelList: "250, 500,1000", step: 2 * time.Second, inflight: 64}
	if err := o.validate(500); err != nil {
		t.Fatal(err)
	}
	var d loadOpts
	if err := d.decode(o.encode()); err != nil {
		t.Fatal(err)
	}
	if d.mode != loadList || !d.poisson() || d.inflight != 64 || len(d.levelVals) != 3 || d.levelVals[2] != 1000 || d.step != 2*time.Second {
		t.Fatalf("round trip: %+v", d)
	}
	next := d.levels(1, 500, time.Now)
	var got []int
	for l, ok := next(); ok; l, ok = next() {
		if l.windows != 4 {
			t.Fatalf("2 s step at 500 ms windows: %d windows", l.windows)
		}
		got = append(got, l.clients)
	}
	if len(got) != 3 || got[0] != 250 {
		t.Fatalf("levels %v", got)
	}
	cycled := &loadOpts{mode: loadList, arrivals: arrPoisson, levelList: "100,120,150", step: 100 * time.Second, duration: 350 * time.Second, inflight: 1}
	if err := cycled.validate(500); err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{t: time.Unix(1_800_000_000, 0)}
	cycle := cycled.levels(1, 500, clock.now)
	var seq []int
	var holds []time.Duration
	for {
		l, ok := cycle()
		if !ok {
			break
		}
		seq = append(seq, l.clients)
		holds = append(holds, l.hold)
		clock.t = clock.t.Add(l.hold)
	}
	if len(seq) != 4 || seq[0] != 100 || seq[1] != 120 || seq[2] != 150 || seq[3] != 100 {
		t.Fatalf("cycle %v", seq)
	}
	if holds[3] != 50*time.Second {
		t.Fatalf("last hold %s", holds[3])
	}
	short := &loadOpts{mode: loadList, arrivals: arrPoisson, levelList: "100,120", step: 100 * time.Second, duration: 50 * time.Second, inflight: 1}
	if short.validate(500) == nil {
		t.Fatal("duration shorter than a step accepted")
	}
	bad := &loadOpts{mode: loadRamp, arrivals: arrPoisson, inflight: 1}
	if bad.validate(500) == nil {
		t.Fatal("poisson ramp accepted")
	}
}
