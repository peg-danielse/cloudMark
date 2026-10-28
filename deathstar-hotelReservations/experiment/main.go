// Command experiment starts the hotel-reservation processes and checks every
// frontend route and every gRPC method against the seed data those processes insert.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/boot"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/probe"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func benchmarksRoot() string {
	if r := os.Getenv("DSB_ROOT"); r != "" {
		return r
	}
	if exe, err := os.Executable(); err == nil {
		if exe, err = filepath.EvalSymlinks(exe); err == nil {
			if r := findBenchmarks(filepath.Dir(exe)); r != "" {
				return r
			}
		}
	}
	if _, file, _, ok := goruntime.Caller(0); ok {
		if r := findBenchmarks(filepath.Dir(file)); r != "" {
			return r
		}
	}
	fmt.Fprintln(os.Stderr, "cannot find the benchmarks directory; set DSB_ROOT")
	os.Exit(1)
	return ""
}

func findBenchmarks(start string) string {
	dir := start
	for {
		hotel := filepath.Join(dir, "deathstar-hotelReservations")
		runtimeDir := filepath.Join(dir, "runtime")
		if info, err := os.Stat(hotel); err == nil && info.IsDir() {
			if info, err = os.Stat(runtimeDir); err == nil && info.IsDir() {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func configPath() string {
	return benchmarksRoot() + "/deathstar-hotelReservations/config.json"
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type child struct {
	name string
	cmd  *exec.Cmd
	log  string
	done atomic.Bool
	code atomic.Int32
}

var hotel boot.Config

func run() error {
	work, err := os.MkdirTemp("", "hotel-experiment-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(work)

	hotel, err = boot.LoadFile(configPath())
	if err != nil {
		return err
	}
	config, err := os.ReadFile(configPath())
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(work, "config.json"), config, 0644); err != nil {
		return err
	}

	ports := []string{hotel.Mongo}
	for _, addr := range hotel.Memcached {
		ports = append(ports, addr)
	}
	for _, addr := range hotel.Services {
		ports = append(ports, addr)
	}
	for _, addr := range ports {
		if err := portFree(addr); err != nil {
			return err
		}
	}

	var children []*child
	stop := func() {
		for i := len(children) - 1; i >= 0; i-- {
			stopChild(children[i])
		}
	}
	defer stop()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		stop()
		os.Exit(1)
	}()

	runtime := []struct{ name, path string }{
		{"memcached-11211", benchmarksRoot() + "/runtime/memcached/11211"},
		{"memcached-11212", benchmarksRoot() + "/runtime/memcached/11212"},
		{"memcached-11213", benchmarksRoot() + "/runtime/memcached/11213"},
		{"memcached-11214", benchmarksRoot() + "/runtime/memcached/11214"},
		{"mongod", benchmarksRoot() + "/runtime/mongodb/mongod"},
	}
	for _, spec := range runtime {
		ch, err := startChild(spec.name, spec.path, work)
		if err != nil {
			return err
		}
		children = append(children, ch)
	}
	deps := []string{hotel.Mongo}
	for _, addr := range hotel.Memcached {
		deps = append(deps, addr)
	}
	for _, addr := range deps {
		if err := waitPort(addr, 60*time.Second); err != nil {
			return fmt.Errorf("%w\n%s", err, tailLogs(children))
		}
	}
	if err := dropHotelDatabases(); err != nil {
		return err
	}

	bin := benchmarksRoot() + "/deathstar-hotelReservations/bin"
	apps := []string{
		"geo", "profile", "rate", "recommendation", "user",
		"reservation", "review", "attractions", "search", "frontend",
	}
	appService := map[string]string{
		"geo": "srv-geo", "profile": "srv-profile", "rate": "srv-rate",
		"recommendation": "srv-recommendation", "user": "srv-user",
		"reservation": "srv-reservation", "review": "srv-review",
		"attractions": "srv-attractions", "search": "srv-search", "frontend": "frontend",
	}
	// Search and the frontend dial other services while they start. Each
	// process has to be listening before the next one is launched.
	for _, name := range apps {
		ch, err := startChild(name, filepath.Join(bin, name), work)
		if err != nil {
			return err
		}
		children = append(children, ch)
		if err := waitPort(hotel.Services[appService[name]], 60*time.Second); err != nil {
			return fmt.Errorf("%s did not listen: %w\n%s", name, err, tailLogs(children))
		}
		if err := childAlive(children, name); err != nil {
			return fmt.Errorf("%w\n%s", err, tailLogs(children))
		}
	}

	for _, ch := range children {
		if ch.done.Load() {
			return fmt.Errorf("%s exited %d\n%s", ch.name, ch.code.Load(), tailFile(ch.log))
		}
	}
	return probe.Run(hotel)
}

func startChild(name, path, dir string) (*child, error) {
	logPath := filepath.Join(dir, name+".log")
	f, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(path)
	cmd.Dir = dir
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		f.Close()
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	ch := &child{name: name, cmd: cmd, log: logPath}
	go func() {
		_ = cmd.Wait()
		if cmd.ProcessState != nil {
			ch.code.Store(int32(cmd.ProcessState.ExitCode()))
		}
		ch.done.Store(true)
		f.Close()
	}()
	fmt.Printf("started %s pid %d\n", name, cmd.Process.Pid)
	return ch, nil
}

func stopChild(ch *child) {
	if ch == nil || ch.cmd.Process == nil || ch.done.Load() {
		return
	}
	_ = syscall.Kill(-ch.cmd.Process.Pid, syscall.SIGTERM)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ch.done.Load() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = syscall.Kill(-ch.cmd.Process.Pid, syscall.SIGKILL)
}

func childAlive(children []*child, name string) error {
	for _, ch := range children {
		if ch.name == name && ch.done.Load() {
			return fmt.Errorf("%s exited %d", name, ch.code.Load())
		}
	}
	return nil
}

func portFree(addr string) error {
	c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return nil
	}
	c.Close()
	return fmt.Errorf("%s is already in use", addr)
}

func waitPort(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return nil
		}
		last = err
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s: %v", addr, last)
}

func dropHotelDatabases() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://"+hotel.Mongo))
	if err != nil {
		return err
	}
	defer client.Disconnect(context.Background())
	for _, name := range []string{
		"geo-db", "profile-db", "rate-db", "reservation-db",
		"recommendation-db", "user-db", "review-db", "attractions-db",
	} {
		if err := client.Database(name).Drop(ctx); err != nil {
			return fmt.Errorf("drop %s: %w", name, err)
		}
	}
	return nil
}

func tailLogs(children []*child) string {
	var b strings.Builder
	for _, ch := range children {
		if !ch.done.Load() {
			continue
		}
		fmt.Fprintf(&b, "--- %s ---\n%s\n", ch.name, tailFile(ch.log))
	}
	return b.String()
}

func tailFile(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	lines := strings.Split(string(data), "\n")
	if len(lines) > 40 {
		lines = lines[len(lines)-40:]
	}
	return strings.Join(lines, "\n")
}
