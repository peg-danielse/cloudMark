package main

// Mercury worker API and deployment-list helpers, adapted from
// deathstar-hotelReservations/operate (copied, not imported).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

type tuple struct {
	path     string
	hostname string
	id       string
}

// parseFile reads mercury-social.txt. Daemon ids are social-<basename>, the
// same ids the C++ operate uses, so rings and cgroups line up with old runs.
func parseFile(path string) ([]tuple, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var lines []tuple
	seen := map[string]bool{}
	for n, raw := range strings.Split(string(data), "\n") {
		if i := strings.IndexByte(raw, '#'); i >= 0 {
			raw = raw[:i]
		}
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("%s:%d: want exec_path and hostname", path, n+1)
		}
		id := "social-" + filepath.Base(fields[0])
		if seen[id] {
			return nil, fmt.Errorf("duplicate daemon id %s", id)
		}
		seen[id] = true
		lines = append(lines, tuple{path: resolveExec(fields[0]), hostname: fields[1], id: id})
	}
	if len(lines) == 0 {
		return nil, fmt.Errorf("%s has no programs", path)
	}
	return lines, nil
}

// serviceKey maps a binary name to its service-config.json key:
// UserTimelineService -> user-timeline-service.
func serviceKey(binary string) string {
	name := strings.TrimSuffix(binary, "Service")
	if name == "" {
		name = binary
	}
	var b strings.Builder
	for i, r := range name {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				b.WriteByte('-')
			}
			r += 'a' - 'A'
		}
		b.WriteRune(r)
	}
	return b.String() + "-service"
}

// listenAddr is where a daemon listens, or "" for WriteHomeTimelineService,
// which only consumes RabbitMQ.
func listenAddr(line tuple, cfg serviceConfig) (string, error) {
	base := filepath.Base(line.path)
	if _, err := strconv.Atoi(base); err == nil {
		return net.JoinHostPort("127.0.0.1", base), nil
	}
	switch base {
	case "mongod":
		if addr, err := cfg.address("social-graph-mongodb"); err == nil {
			return addr, nil // -config may move MongoDB (demo: 27018 next to hotel's 27017)
		}
		return "127.0.0.1:27017", nil
	case "redis-server":
		return "127.0.0.1:6379", nil
	case "rabbitmq-server":
		return "127.0.0.1:5672", nil
	case "WriteHomeTimelineService":
		return "", nil
	}
	return cfg.address(serviceKey(base))
}

// daemonEnv mirrors hotel: memcached and mongod wrappers turn these into -t
// and WiredTiger caps. The C++ Thrift services, Redis and RabbitMQ take none.
func daemonEnv(line tuple) map[string]string {
	limit := strconv.Itoa(autoMaxCores)
	switch {
	case strings.Contains(line.path, "/memcached/"):
		return map[string]string{"MEMCACHED_THREADS": limit}
	case filepath.Base(line.path) == "mongod":
		return map[string]string{
			"MONGO_WT_READ_TRANSACTIONS":  limit,
			"MONGO_WT_WRITE_TRANSACTIONS": limit,
		}
	}
	return nil
}

func formatEnv(env map[string]string) string {
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, key := range keys {
		parts[i] = key + "=" + env[key]
	}
	return strings.Join(parts, " ")
}

func mercuryURL(addr string) string {
	if strings.Contains(addr, "://") {
		return strings.TrimRight(addr, "/")
	}
	return "http://" + addr
}

func ping(base string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/ping", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("mercury %s /ping: %s %s", base, resp.Status, bytes.TrimSpace(body))
	}
	return nil
}

func post(base, path string, payload any) (int, []byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, body, err
}

func start(base string, line tuple, cpu int, env map[string]string) (int, error) {
	var out struct {
		Pid int `json:"pid"`
	}
	payload := map[string]any{"daemon_id": line.id, "exec_path": line.path, "cpu": cpu}
	if len(env) > 0 {
		payload["env"] = env
	}
	status, body, err := post(base, "/start", payload)
	if err != nil {
		return 0, err
	}
	if status != http.StatusOK || json.Unmarshal(body, &out) != nil || out.Pid <= 0 {
		return 0, fmt.Errorf("POST /start: %d %s", status, strings.TrimSpace(string(body)))
	}
	return out.Pid, nil
}

func kill(base, id string) error {
	status, body, err := post(base, "/kill", map[string]string{"daemon_id": id})
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("POST /kill %s: %d %s", id, status, strings.TrimSpace(string(body)))
	}
	fmt.Printf("killed %s\n", id)
	return nil
}

func stopAll(base string, lines []tuple) error {
	var failed error
	for i := len(lines) - 1; i >= 0; i-- {
		if err := kill(base, lines[i].id); err != nil && failed == nil {
			failed = err
		}
	}
	return failed
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
	return fmt.Errorf("timed out: %v", last)
}

// waitRedis sends PING until Redis answers +PONG (not -LOADING or another error) or timeout passes.
func waitRedis(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	last := "no reply"
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.SetDeadline(time.Now().Add(time.Second))
			buf := make([]byte, 256)
			if _, err = c.Write([]byte("PING\r\n")); err == nil {
				var n int
				n, err = c.Read(buf)
				reply := strings.TrimSpace(string(buf[:n]))
				if err == nil && strings.HasPrefix(reply, "+PONG") {
					c.Close()
					return nil
				}
				if reply != "" {
					last = reply
				}
			}
			c.Close()
		}
		if err != nil {
			last = err.Error()
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out after %s: %s", timeout, last)
}

func processAlive(pid int) bool {
	_, err := os.Stat("/proc/" + strconv.Itoa(pid))
	return err == nil
}

func findLine(lines []tuple, base string) (tuple, bool) {
	for _, line := range lines {
		if filepath.Base(line.path) == base {
			return line, true
		}
	}
	return tuple{}, false
}

func rampClients(maxUsers int) []int {
	up := []int{1}
	for up[len(up)-1] < maxUsers {
		next := up[len(up)-1] * 2
		if next <= up[len(up)-1] || next > maxUsers {
			next = maxUsers
		}
		up = append(up, next)
	}
	clients := make([]int, 0, len(up)*2-1)
	clients = append(clients, up...)
	for i := len(up) - 2; i >= 0; i-- {
		clients = append(clients, up[i])
	}
	return clients
}
func enableSaturate(base, id string, minCores, maxCores int) error {
	return enableScale(base, id, "saturate", minCores, maxCores)
}

// Policy for the service daemons (-policy, -tick-ms, -placement). The load generator stays on saturate.
var (
	servicePolicy    = "saturate"
	serviceTickMs    = 10
	servicePlacement = "least_loaded"
	servicePin       = true              // -pin: gnn pins busy threads (false: cpuset sizing only)
	saturateOnly     = map[string]bool{} // program base names kept on saturate under -policy gnn
)

// enableScale attaches policy (saturate or gnn) to a daemon with the -placement core hand-out.
// tick_ms is sent for gnn only.
func enableScale(base, id, policy string, minCores, maxCores int) error {
	req := map[string]any{
		"daemon_id": id,
		"policy":    policy,
		"min":       minCores,
		"max":       maxCores,
		"placement": servicePlacement,
	}
	if policy == "gnn" {
		req["tick_ms"] = serviceTickMs
		req["pin"] = servicePin
	}
	status, body, err := post(base, "/scale", req)
	if err != nil {
		return err
	}
	if status != http.StatusOK || !bytes.Contains(body, []byte(`"ok":true`)) {
		return fmt.Errorf("POST /scale %s: %d %s", id, status, bytes.TrimSpace(body))
	}
	return nil
}
func readCPUSet(id string) string {
	data, err := os.ReadFile(cgroupFile(id, "cpuset.cpus"))
	if err != nil {
		return "?"
	}
	text := strings.TrimSpace(string(data))
	if text == "" {
		return "?"
	}
	return text
}
func cpuSetList(lines []tuple, spec func(string) string) string {
	var b strings.Builder
	for i, line := range lines {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(filepath.Base(line.path))
		b.WriteByte('=')
		b.WriteString(spec(line.id))
	}
	return b.String()
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

type reading struct {
	arrivalRate uint64
	serviceRate uint64
	avgUs       float64
	p95Us       float64
	p99Us       float64
	errors      uint64
}

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
