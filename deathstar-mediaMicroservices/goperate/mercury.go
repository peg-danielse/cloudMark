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
)

type tuple struct {
	path     string
	hostname string
	id       string
}

// parseFile reads mercury-media.txt. Daemon ids are social-<basename>, the
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
		id := "media-" + filepath.Base(fields[0])
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

func processAlive(pid int) bool {
	_, err := os.Stat("/proc/" + strconv.Itoa(pid))
	return err == nil
}
func enableSaturate(base, id string, minCores, maxCores int) error {
	status, body, err := post(base, "/scale", map[string]any{
		"daemon_id": id,
		"policy":    "saturate",
		"min":       minCores,
		"max":       maxCores,
	})
	if err != nil {
		return err
	}
	if status != http.StatusOK || !bytes.Contains(body, []byte(`"ok":true`)) {
		return fmt.Errorf("POST /scale %s: %d %s", id, status, bytes.TrimSpace(body))
	}
	return nil
}
func readCPUSet(id string) string {
	data, err := os.ReadFile("/sys/fs/cgroup/mercury/" + id + "/cpuset.cpus")
	if err != nil {
		return "?"
	}
	text := strings.TrimSpace(string(data))
	if text == "" {
		return "?"
	}
	return text
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

// resolveExec maps a deployment-list path to the file Mercury execs: relative paths are under benchmarks/.
func resolveExec(p string) string {
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(benchmarksRoot(), p)
}
