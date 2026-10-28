package main

// Policy for the media daemons, as the hotel and social operators do it: every daemon gets Mercury's policy (saturate
// or gnn) between -svc-min and -svc-max cores, or a fixed count from -svc-fixed. The load generator stays on saturate.

import (
	"bytes"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
)

// Core bounds for the service daemons (-svc-min, -svc-max, -svc-fixed).
var (
	svcMinCores  = 1
	autoMaxCores = 16
	svcFixed     = map[string]int{}
)

// Policy for the service daemons (-policy, -tick-ms, -placement, -pin).
var (
	servicePolicy    = "saturate"
	serviceTickMs    = 10
	servicePlacement = "least_loaded"
	servicePin       = true // gnn pins busy threads; false sizes cpusets only
)

// enableScale attaches policy (saturate or gnn) to a daemon with the -placement core hand-out. tick_ms and pin are
// sent for gnn only.
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

// svcRange is the core bounds for one deployment line: its -svc-fixed count (min = max), else the defaults.
func svcRange(line tuple, dmin, dmax int) (int, int) {
	if n, ok := svcFixed[filepath.Base(line.path)]; ok {
		return n, n
	}
	return dmin, dmax
}

// parseSvcFixed reads "name=cores,..." for -svc-fixed, keyed by the deployment line's program name.
func parseSvcFixed(s string) error {
	for _, kv := range strings.Split(s, ",") {
		if kv == "" {
			continue
		}
		k, v, ok := strings.Cut(kv, "=")
		n, err := strconv.Atoi(v)
		if !ok || err != nil || n < 1 {
			return fmt.Errorf("-svc-fixed: bad entry %q (want name=cores)", kv)
		}
		svcFixed[k] = n
	}
	return nil
}
