package frontend

// Degrade (Mercury's degrade system, Mercury-zeroctl SPEC.md "degrade"): when MERCURY_POLICY names a file, the
// frontend polls it every 200 ms. It holds {"disabled": ["recommendations", ...]}: requests to a disabled route
// are answered at once with 503, so an overloaded system sheds optional work before it reaches any backend.
// A missing or unreadable file disables nothing. Without MERCURY_POLICY the frontend behaves as before.

import (
	"encoding/json"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

type degrade struct {
	disabled atomic.Pointer[map[string]bool]
}

func startDegrade() *degrade {
	path := os.Getenv("MERCURY_POLICY")
	if path == "" {
		return nil
	}
	d := &degrade{}
	empty := map[string]bool{}
	d.disabled.Store(&empty)
	go func() {
		last := ""
		for range time.Tick(200 * time.Millisecond) {
			set := map[string]bool{}
			if raw, err := os.ReadFile(path); err == nil {
				var policy struct {
					Disabled []string `json:"disabled"`
				}
				if json.Unmarshal(raw, &policy) == nil {
					for _, route := range policy.Disabled {
						set[route] = true
					}
				}
			}
			names := make([]string, 0, len(set))
			for route := range set {
				names = append(names, route)
			}
			sort.Strings(names)
			if now := strings.Join(names, ","); now != last {
				log.Info().Msgf("degrade: disabled routes [%s]", now)
				last = now
			}
			d.disabled.Store(&set)
		}
	}()
	log.Info().Msgf("degrade: following %s", path)
	return d
}

// wrap sheds requests to route while it is disabled.
func (d *degrade) wrap(route string, h http.Handler) http.Handler {
	if d == nil {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if (*d.disabled.Load())[route] {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			http.Error(w, "degraded: "+route+" is disabled under load", http.StatusServiceUnavailable)
			return
		}
		h.ServeHTTP(w, r)
	})
}
