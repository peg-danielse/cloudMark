package main

import (
	"context"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	sn "socialnetwork/goperate/social_network"
)

func TestServiceKey(t *testing.T) {
	for bin, want := range map[string]string{
		"UserTimelineService":      "user-timeline-service",
		"UniqueIdService":          "unique-id-service",
		"ComposePostService":       "compose-post-service",
		"WriteHomeTimelineService": "write-home-timeline-service",
		"UrlShortenService":        "url-shorten-service",
	} {
		if got := serviceKey(bin); got != want {
			t.Fatalf("serviceKey(%s) = %s want %s", bin, got, want)
		}
	}
}

func TestParseFileAndListen(t *testing.T) {
	dir := t.TempDir()
	list := filepath.Join(dir, "m.txt")
	writeFile(list, "# c\n/x/memcached/11311 h\n/x/mongodb/mongod h # tail\n\n/x/run/UserService h\n/x/run/WriteHomeTimelineService h\n")
	lines, err := parseFile(list)
	if err != nil || len(lines) != 4 {
		t.Fatalf("parseFile = %v %v", lines, err)
	}
	if lines[0].id != "social-11311" || lines[2].id != "social-UserService" {
		t.Fatalf("ids %v", lines)
	}
	cfgPath := filepath.Join(dir, "c.json")
	writeFile(cfgPath, `{"secret":"s","user-service":{"addr":"127.0.0.1","port":10008,"connections":64}}`)
	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"127.0.0.1:11311", "127.0.0.1:27017", "127.0.0.1:10008", ""}
	for i, line := range lines {
		got, err := listenAddr(line, cfg)
		if err != nil || got != want[i] {
			t.Fatalf("listenAddr(%s) = %q %v want %q", line.id, got, err, want[i])
		}
	}
	if env := daemonEnv(lines[0]); env["MEMCACHED_THREADS"] != "16" {
		t.Fatalf("memcached env %v", env)
	}
	if env := daemonEnv(lines[2]); env != nil {
		t.Fatalf("C++ service env %v", env)
	}
	writeFile(list, writeDup)
	if _, err := parseFile(list); err == nil {
		t.Fatal("duplicate ids accepted")
	}
}

const writeDup = "/a/run/UserService h\n/b/run/UserService h\n"

func TestRealDeploymentList(t *testing.T) {
	lines, err := parseFile(benchmarksRoot() + "/mercury-social.txt")
	if err != nil {
		t.Skip("no mercury-social.txt here")
	}
	cfg, err := loadConfig(socialRoot() + "/config/service-config.json")
	if err != nil {
		t.Skip("no service-config.json here")
	}
	if _, err := daemonHomes(len(lines)); err != nil {
		t.Fatal(err)
	}
	for _, line := range lines {
		if _, err := listenAddr(line, cfg); err != nil {
			t.Fatalf("%s: %v", line.id, err)
		}
	}
	for r, key := range routeService {
		if _, err := cfg.address(key); err != nil {
			t.Fatalf("route %s: %v", r, err)
		}
	}
}

func TestMixAndRoutes(t *testing.T) {
	names, err := parseRoutes(defaultRoutes)
	if err != nil || len(names) != 4 {
		t.Fatalf("routes %v %v", names, err)
	}
	if _, err := parseRoutes("compose-post,nginx"); err == nil {
		t.Fatal("unknown route accepted")
	}
	mix, err := parseMix("", names)
	if err != nil || mix[routeFollow] != 1 {
		t.Fatalf("equal mix %v %v", mix, err)
	}
	names = names[:3]
	mix, err = parseMix("compose-post=0.1,read-home-timeline=0.6,read-user-timeline=0.3", names)
	if err != nil || mix[routeHome] != 1 || mix[routeCompose] < 0.166 || mix[routeCompose] > 0.167 {
		t.Fatalf("dsb mix %v %v", mix, err)
	}
	if _, err := parseMix("compose-post=1", names); err == nil {
		t.Fatal("mix missing a route accepted")
	}
	if routeUsers(1, 0.1) != 1 || routeUsers(1024, mix[routeCompose]) != 171 || routeUsers(1024, 1) != 1024 {
		t.Fatalf("routeUsers %d %d", routeUsers(1, 0.1), routeUsers(1024, mix[routeCompose]))
	}
}

func TestLoadGraphAndRequests(t *testing.T) {
	dir := t.TempDir()
	edges := filepath.Join(dir, "g.edges")
	writeFile(edges, "% comment\n1 0\n2 0\n2 2\n0 1\nbad line\n1 0\n")
	writeFile(filepath.Join(dir, "g.nodes"), "5\n")
	w, err := loadGraph(edges)
	if err != nil {
		t.Fatal(err)
	}
	if len(w.users) != 5 || len(w.edges) != 3 {
		t.Fatalf("users %v edges %v", w.users, w.edges)
	}
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 500; i++ {
		a, b := followPair(r, w)
		if a == b || w.edges[[2]int64{a, b}] {
			t.Fatalf("followPair gave %d->%d", a, b)
		}
		s, e := timelineWindow(r)
		if s < 0 || s >= 100 || e != s+10 {
			t.Fatalf("window %d %d", s, e)
		}
		text := composeText(r, w.users)
		if len(text) < 256 {
			t.Fatalf("text %q", text)
		}
		for _, word := range strings.Fields(text) {
			if strings.HasPrefix(word, "@") && !strings.HasPrefix(word, "@user") {
				t.Fatalf("mention %q", word)
			}
		}
	}
}

// fakeHome answers ReadHomeTimeline with one post.
type fakeHome struct{ calls atomic.Int64 }

func (f *fakeHome) ReadHomeTimeline(ctx context.Context, req int64, user int64, start, stop int32, c map[string]string) ([]*sn.Post, error) {
	f.calls.Add(1)
	if user < 0 {
		return nil, &sn.ServiceException{Message: "bad user"}
	}
	return []*sn.Post{{PostID: 1}}, nil
}

func (f *fakeHome) WriteHomeTimeline(ctx context.Context, req, post, user, ts int64, mentions []int64, c map[string]string) error {
	return nil
}

func serveHome(t *testing.T) (string, *fakeHome, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	sock, err := thrift.NewTServerSocket(addr)
	if err != nil {
		t.Fatal(err)
	}
	h := &fakeHome{}
	cfg := &thrift.TConfiguration{}
	srv := thrift.NewTSimpleServer4(sn.NewHomeTimelineServiceProcessor(h), sock,
		thrift.NewTFramedTransportFactoryConf(thrift.NewTTransportFactory(), cfg),
		thrift.NewTBinaryProtocolFactoryConf(cfg))
	go srv.Serve()
	if err := waitPort(addr, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	return addr, h, func() { srv.Stop() }
}

func TestReadHomeTimelineRoute(t *testing.T) {
	addr, h, stop := serveHome(t)
	defer stop()
	host, port, _ := net.SplitHostPort(addr)
	cfg := serviceConfig{"home-timeline-service": []byte(`{"addr":"` + host + `","port":` + port + `}`)}
	w := &workload{users: []int64{0, 1, 2}, edges: map[[2]int64]bool{}, mix: map[string]float64{routeHome: 1}}
	for _, conns := range []int{0, 2} {
		connsPerRoute = conns
		routes, err := openRoutes(cfg, []string{routeHome}, 8, w)
		if err != nil {
			t.Fatalf("conns=%d: %v", conns, err)
		}
		routes[0].rec.setActive(8)
		time.Sleep(100 * time.Millisecond)
		s := routes[0].rec.snapshot()
		stopRoutes(routes)
		if s.done < 8 || s.errs != 0 {
			t.Fatalf("conns=%d done %d errs %d", conns, s.done, s.errs)
		}
	}
	connsPerRoute = 0
	if h.calls.Load() == 0 {
		t.Fatal("server saw no calls")
	}
	if !transportBroken(os.ErrClosed) || transportBroken(&sn.ServiceException{Message: "x"}) {
		t.Fatal("transportBroken")
	}
	if alreadyStored(&sn.ServiceException{Message: "x"}) || !alreadyStored(&sn.ServiceException{Message: "User already existed"}) {
		t.Fatal("alreadyStored")
	}
}
