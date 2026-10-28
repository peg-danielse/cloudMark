package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/boot"
	attractions "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/attractions/proto"
	geo "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/geo/proto"
	profile "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/profile/proto"
	rate "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/rate/proto"
	recommendation "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/recommendation/proto"
	reservation "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/reservation/proto"
	review "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/review/proto"
	search "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/search/proto"
	user "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/user/proto"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
)

const (
	scaleWorkers = 128
	scaleWarmup  = 200 * time.Millisecond
	scaleMeasure = time.Second

	// Linux CPU 0 is Mercury. The service under test grows from CPU 1.
	// Every other daemon keeps one core from homeFirstCPU upward.
	// The loader stays on clientFirstCPU through clientLastCPU.
	homeFirstCPU   = 33
	clientFirstCPU = 60
	clientLastCPU  = 63

	scaleLat  = 37.7867
	scaleLon  = -122.4112
	scaleIn   = "2015-04-09"
	scaleOut  = "2015-04-10"
	scaleUser = "Cornell_31"
	scalePass = "1111111111"
)

// connsPerRoute is how many gRPC connections one route shares among its
// workers (round-robin by worker index). 0 restores one connection per worker.
var connsPerRoute = 16

// loadRoutes are the routes autoscale drives (-routes, LOADGEN_ROUTES for bin/loadgen). Every
// program in the deployment still starts; the services of the other routes only sit idle.
var loadRoutes = frontendRoutes

// loadEntry: frontend (default: every route is a frontend HTTP endpoint, as a real client sees the app) or services
// (each route calls one service directly over gRPC, the earlier sweeps).
var loadEntry = entryFrontend

const (
	entryFrontend = "frontend"
	entryServices = "services"
)

// frontendRoutes are the hotel frontend's endpoints (services/frontend/server.go); each calls the services behind it.
var frontendRoutes = []string{"hotels", "recommendations", "user", "review", "restaurants", "museums", "cinema", "reservation"}

func allRoutes() []string {
	if loadEntry == entryServices {
		return hotelServiceNames
	}
	return frontendRoutes
}

// parseRoutes reads a comma list of route names ("" or "all": every route of the entry), in that order.
func parseRoutes(list string) ([]string, error) {
	names := allRoutes()
	if list == "" || list == "all" {
		return names, nil
	}
	want := map[string]bool{}
	for _, name := range strings.Split(list, ",") {
		name = strings.TrimSpace(name)
		known := false
		for _, n := range names {
			known = known || n == name
		}
		if !known {
			return nil, fmt.Errorf("unknown route %q for -entry %s (routes: %s)", name, loadEntry, strings.Join(names, ","))
		}
		want[name] = true
	}
	var out []string
	for _, n := range names {
		if want[n] {
			out = append(out, n)
		}
	}
	return out, nil
}

var scaleWidths = []int{1, 2, 4, 8, 16, 32}

var hotelServiceNames = []string{
	"geo", "profile", "rate", "recommendation", "user",
	"reservation", "search", "review", "attractions", "frontend",
}

func scaleServices(base string, cfg boot.Config, lines []tuple) error {
	if err := pinTasks(clientCPUs()); err != nil {
		return err
	}
	runtime.GOMAXPROCS(len(clientCPUs()))
	homes := make(map[string]string, len(lines))
	for i, line := range lines {
		cpu := homeFirstCPU + i
		if cpu >= clientFirstCPU {
			return fmt.Errorf("no home cpu left for %s", line.id)
		}
		spec := strconv.Itoa(cpu)
		if err := setCpus(base, line.id, spec); err != nil {
			return err
		}
		homes[line.id] = spec
	}
	fmt.Printf("client cpus %d-%d\n", clientFirstCPU, clientLastCPU)
	fmt.Print("homes")
	for i, line := range lines {
		fmt.Printf(" %s=%d", filepath.Base(line.path), homeFirstCPU+i)
	}
	fmt.Println()

	for _, name := range hotelServiceNames {
		line, ok := findLine(lines, name)
		if !ok {
			return fmt.Errorf("deployment has no %s", name)
		}
		if err := sweepService(base, cfg, line, homes[line.id]); err != nil {
			return err
		}
	}
	return nil
}

func clientCPUs() []int {
	cpus := make([]int, 0, clientLastCPU-clientFirstCPU+1)
	for cpu := clientFirstCPU; cpu <= clientLastCPU; cpu++ {
		cpus = append(cpus, cpu)
	}
	return cpus
}

func findLine(lines []tuple, name string) (tuple, bool) {
	for _, line := range lines {
		if filepath.Base(line.path) == name {
			return line, true
		}
	}
	return tuple{}, false
}

func cpuSpec(cores int) string {
	if cores <= 1 {
		return "1"
	}
	return "1-" + strconv.Itoa(cores)
}

func setCpus(base, id, spec string) error {
	status, body, err := post(base, "/cpus", map[string]string{
		"daemon_id": id,
		"cpus":      spec,
	})
	if err != nil {
		return err
	}
	if status != http.StatusOK || !bytes.Contains(body, []byte(`"ok":true`)) {
		return fmt.Errorf("POST /cpus %s %s: %d %s", id, spec, status, bytes.TrimSpace(body))
	}
	return nil
}

func pinTasks(cpus []int) error {
	var set unix.CPUSet
	for _, cpu := range cpus {
		set.Set(cpu)
	}
	entries, err := os.ReadDir("/proc/self/task")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		tid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		if err := unix.SchedSetaffinity(tid, &set); err != nil {
			return fmt.Errorf("pin tid %d: %w", tid, err)
		}
	}
	return nil
}

type load struct {
	call  func(context.Context, int) error
	close func()
}

func sweepService(base string, cfg boot.Config, line tuple, home string) error {
	name := filepath.Base(line.path)
	work, err := openLoad(cfg, name, scaleWorkers)
	if err != nil {
		return err
	}
	defer work.close()

	ctx, cancel := context.WithCancel(context.Background())
	rec := startLoad(ctx, work.call, scaleWorkers, scaleWorkers)
	defer func() {
		cancel()
		rec.wg.Wait()
	}()
	if err := waitReady(rec, name); err != nil {
		return err
	}

	for _, cores := range scaleWidths {
		spec := cpuSpec(cores)
		if err := setCpus(base, line.id, spec); err != nil {
			return err
		}
		time.Sleep(scaleWarmup)
		reading := measure(rec)
		fmt.Printf("service %s cores %d cpus %s arrival_rate %d service_rate %d avg_us %.1f p95_us %.1f p99_us %.1f errors %d\n",
			name, cores, spec, reading.arrivalRate, reading.serviceRate, reading.avgUs, reading.p95Us, reading.p99Us, reading.errors)
	}
	return setCpus(base, line.id, home)
}

func openLoad(cfg boot.Config, name string, workers int) (load, error) {
	if loadEntry == entryFrontend {
		return openFrontendRoute(cfg, name, workers)
	}
	if name == "frontend" {
		return openFrontend(cfg, workers)
	}
	addr, err := cfg.Address("srv-" + name)
	if err != nil {
		return load{}, err
	}
	n := workers
	if connsPerRoute > 0 && connsPerRoute < n {
		n = connsPerRoute
	}
	conns := make([]*grpc.ClientConn, n)
	for i := range conns {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
		cancel()
		if err != nil {
			for _, open := range conns[:i] {
				open.Close()
			}
			return load{}, fmt.Errorf("%s dial: %w", name, err)
		}
		conns[i] = conn
	}
	call, err := grpcCall(name, conns)
	if err != nil {
		for _, conn := range conns {
			conn.Close()
		}
		return load{}, err
	}
	return load{
		call: call,
		close: func() {
			for _, conn := range conns {
				conn.Close()
			}
		},
	}, nil
}

func grpcCall(name string, conns []*grpc.ClientConn) (func(context.Context, int) error, error) {
	switch name {
	case "geo":
		clients := make([]geo.GeoClient, len(conns))
		for i, conn := range conns {
			clients[i] = geo.NewGeoClient(conn)
		}
		return func(ctx context.Context, worker int) error {
			res, err := clients[worker%len(clients)].Nearby(ctx, &geo.Request{Lat: scaleLat, Lon: scaleLon})
			if err != nil {
				return err
			}
			if !sameIDs(res.HotelIds, []string{"1", "2", "3", "5", "6"}) {
				return fmt.Errorf("hotel ids %v", res.HotelIds)
			}
			return nil
		}, nil
	case "profile":
		clients := make([]profile.ProfileClient, len(conns))
		for i, conn := range conns {
			clients[i] = profile.NewProfileClient(conn)
		}
		return func(ctx context.Context, worker int) error {
			res, err := clients[worker%len(clients)].GetProfiles(ctx, &profile.Request{HotelIds: []string{"1"}, Locale: "en"})
			if err != nil {
				return err
			}
			if len(res.Hotels) != 1 || res.Hotels[0].GetName() != "Clift Hotel" {
				return fmt.Errorf("profiles %v", res.Hotels)
			}
			return nil
		}, nil
	case "rate":
		clients := make([]rate.RateClient, len(conns))
		for i, conn := range conns {
			clients[i] = rate.NewRateClient(conn)
		}
		return func(ctx context.Context, worker int) error {
			res, err := clients[worker%len(clients)].GetRates(ctx, &rate.Request{
				HotelIds: []string{"1"}, InDate: scaleIn, OutDate: scaleOut,
			})
			if err != nil {
				return err
			}
			if len(res.RatePlans) != 1 || res.RatePlans[0].GetHotelId() != "1" {
				return fmt.Errorf("rates %v", res.RatePlans)
			}
			return nil
		}, nil
	case "recommendation":
		clients := make([]recommendation.RecommendationClient, len(conns))
		for i, conn := range conns {
			clients[i] = recommendation.NewRecommendationClient(conn)
		}
		return func(ctx context.Context, worker int) error {
			res, err := clients[worker%len(clients)].GetRecommendations(ctx, &recommendation.Request{
				Require: "rate", Lat: scaleLat, Lon: scaleLon,
			})
			if err != nil {
				return err
			}
			if !sameIDs(res.HotelIds, []string{"9", "24", "39", "54", "69"}) {
				return fmt.Errorf("hotel ids %v", res.HotelIds)
			}
			return nil
		}, nil
	case "user":
		clients := make([]user.UserClient, len(conns))
		for i, conn := range conns {
			clients[i] = user.NewUserClient(conn)
		}
		return func(ctx context.Context, worker int) error {
			res, err := clients[worker%len(clients)].CheckUser(ctx, &user.Request{Username: scaleUser, Password: scalePass})
			if err != nil {
				return err
			}
			if !res.Correct {
				return fmt.Errorf("correct=false")
			}
			return nil
		}, nil
	case "reservation":
		clients := make([]reservation.ReservationClient, len(conns))
		for i, conn := range conns {
			clients[i] = reservation.NewReservationClient(conn)
		}
		return func(ctx context.Context, worker int) error {
			res, err := clients[worker%len(clients)].CheckAvailability(ctx, &reservation.Request{
				HotelId: []string{"1", "2"}, InDate: scaleIn, OutDate: scaleOut, RoomNumber: 1,
			})
			if err != nil {
				return err
			}
			if !sameIDs(res.HotelId, []string{"1", "2"}) {
				return fmt.Errorf("hotel ids %v", res.HotelId)
			}
			return nil
		}, nil
	case "search":
		clients := make([]search.SearchClient, len(conns))
		for i, conn := range conns {
			clients[i] = search.NewSearchClient(conn)
		}
		return func(ctx context.Context, worker int) error {
			res, err := clients[worker%len(clients)].Nearby(ctx, &search.NearbyRequest{
				Lat: scaleLat, Lon: scaleLon, InDate: scaleIn, OutDate: scaleOut,
			})
			if err != nil {
				return err
			}
			if !sameIDs(res.HotelIds, []string{"1", "2", "3"}) {
				return fmt.Errorf("hotel ids %v", res.HotelIds)
			}
			return nil
		}, nil
	case "review":
		clients := make([]review.ReviewClient, len(conns))
		for i, conn := range conns {
			clients[i] = review.NewReviewClient(conn)
		}
		return func(ctx context.Context, worker int) error {
			res, err := clients[worker%len(clients)].GetReviews(ctx, &review.Request{HotelId: "1"})
			if err != nil {
				return err
			}
			if len(res.Reviews) != 4 {
				return fmt.Errorf("reviews %d", len(res.Reviews))
			}
			return nil
		}, nil
	case "attractions":
		clients := make([]attractions.AttractionsClient, len(conns))
		for i, conn := range conns {
			clients[i] = attractions.NewAttractionsClient(conn)
		}
		return func(ctx context.Context, worker int) error {
			res, err := clients[worker%len(clients)].NearbyRest(ctx, &attractions.Request{HotelId: "1"})
			if err != nil {
				return err
			}
			if !sameIDs(res.AttractionIds, []string{"1", "2", "3", "4", "5"}) {
				return fmt.Errorf("attraction ids %v", res.AttractionIds)
			}
			return nil
		}, nil
	default:
		return nil, fmt.Errorf("no load for %s", name)
	}
}

func openFrontend(cfg boot.Config, workers int) (load, error) {
	addr, err := cfg.Address("frontend")
	if err != nil {
		return load{}, err
	}
	url := "http://" + addr + "/hotels?inDate=" + scaleIn + "&outDate=" + scaleOut + "&lat=37.7867&lon=-122.4112"
	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns:        workers,
		MaxIdleConnsPerHost: workers,
		MaxConnsPerHost:     workers,
		DisableCompression:  true,
	}}
	return load{
		call: func(ctx context.Context, _ int) error {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				return err
			}
			resp, err := client.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			if err != nil {
				return err
			}
			if resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("Clift Hotel")) {
				return fmt.Errorf("status %d body %s", resp.StatusCode, clipBody(body))
			}
			return nil
		},
		close: func() { client.CloseIdleConnections() },
	}, nil
}

func clipBody(body []byte) string {
	s := string(bytes.TrimSpace(body))
	if len(s) > 200 {
		return s[:200]
	}
	return s
}

func waitReady(rec *recorder, name string) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if rec.snapshot().done > 0 {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	detail := "no response"
	if text, ok := rec.lastErr.Load().(string); ok && text != "" {
		detail = text
	}
	return fmt.Errorf("%s: %s", name, detail)
}

type reading struct {
	arrivalRate uint64
	serviceRate uint64
	avgUs       float64
	p95Us       float64
	p99Us       float64
	errors      uint64
}

func measure(rec *recorder) reading {
	before := rec.snapshot()
	began := time.Now()
	time.Sleep(scaleMeasure)
	after := rec.snapshot()
	return readWindow(before, after, time.Since(began))
}

// readWindow turns two recorder snapshots into one window's rates and times.
// avg_us is exact (sum/count); p95_us comes from the log histogram.
func readWindow(before, after snap, elapsed time.Duration) reading {
	d := after.sub(before)
	var avg float64
	if d.done > 0 {
		avg = float64(d.sumNs) / float64(d.done)
	}
	return reading{
		arrivalRate: perSecond(d.sent, elapsed),
		serviceRate: perSecond(d.done, elapsed),
		avgUs:       avg / float64(time.Microsecond),
		p95Us:       float64(d.percentile(0.95)) / float64(time.Microsecond),
		p99Us:       float64(d.percentile(0.99)) / float64(time.Microsecond),
		errors:      d.errs,
	}
}

func perSecond(count uint64, elapsed time.Duration) uint64 {
	if elapsed <= 0 || count == 0 {
		return 0
	}
	return count * uint64(time.Second) / uint64(elapsed)
}

func percentile(samples []uint64, p float64) uint64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]uint64(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := int(math.Ceil(p*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func sameIDs(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	left := append([]string(nil), got...)
	right := append([]string(nil), want...)
	sort.Strings(left)
	sort.Strings(right)
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// openFrontendRoute calls one frontend endpoint over HTTP with per-request parameters drawn the way DSB's mixed
// hotel workload draws them: check-in 9-23 April 2015 for 1-4 nights, a position around San Francisco, one of the
// 501 users (Cornell_<hex of the decimal id>, password = the id 10 times, cmd/user/db.go) and hotels 1-80.
// The frontend answers 200 with "Failed. ..." in the body on a bad login, so that counts as an error; "No Reviews",
// "Already reserved" and other business answers count as served requests.
func openFrontendRoute(cfg boot.Config, route string, workers int) (load, error) {
	addr, err := cfg.Address("frontend")
	if err != nil {
		return load{}, err
	}
	n := workers
	if n < 64 {
		n = 64
	}
	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns: n, MaxIdleConnsPerHost: n, MaxConnsPerHost: 0, DisableCompression: true,
	}}
	var seq atomic.Int64
	base := "http://" + addr + "/" + route + "?"
	query := func(r *rand.Rand) (string, error) {
		day := 9 + r.Intn(15)
		in := fmt.Sprintf("2015-04-%02d", day)
		out := fmt.Sprintf("2015-04-%02d", day+1+r.Intn(4))
		lat := 38.0235 + float64(r.Intn(481)-240)/1000
		lon := -122.095 + float64(r.Intn(325)-157)/1000
		id := strconv.Itoa(r.Intn(501))
		user := fmt.Sprintf("username=Cornell_%x&password=%s", id, strings.Repeat(id, 10))
		hotel := strconv.Itoa(1 + r.Intn(80))
		switch route {
		case "hotels":
			return fmt.Sprintf("inDate=%s&outDate=%s&lat=%.4f&lon=%.4f", in, out, lat, lon), nil
		case "recommendations":
			req := [...]string{"dis", "rate", "price"}[r.Intn(3)]
			return fmt.Sprintf("require=%s&lat=%.4f&lon=%.4f", req, lat, lon), nil
		case "user":
			return user, nil
		case "review", "restaurants", "museums", "cinema":
			return user + "&hotelId=" + hotel, nil
		case "reservation":
			return fmt.Sprintf("inDate=%s&outDate=%s&lat=%.4f&lon=%.4f&hotelId=%s&customerName=Cornell_%x&%s&number=1",
				in, out, lat, lon, hotel, id, user), nil
		}
		return "", fmt.Errorf("unknown frontend route %q", route)
	}
	if _, err := query(rand.New(rand.NewSource(1))); err != nil {
		return load{}, err
	}
	return load{
		call: func(ctx context.Context, _ int) error {
			r := rand.New(rand.NewSource(seq.Add(1)))
			q, _ := query(r)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+q, nil)
			if err != nil {
				return err
			}
			resp, err := client.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			if err != nil {
				return err
			}
			if resp.StatusCode != http.StatusOK || bytes.Contains(body, []byte("check your username and password")) {
				return fmt.Errorf("%s status %d body %s", route, resp.StatusCode, clipBody(body))
			}
			return nil
		},
		close: func() { client.CloseIdleConnections() },
	}, nil
}
