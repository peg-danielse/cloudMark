package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sn "socialnetwork/goperate/social_network"
)

func defaultGraph() string {
	return filepath.Join(socialRoot(), "datasets/social-graph/socfb-Reed98/socfb-Reed98.edges")
}

// alreadyStored matches the messages the C++ services return for a user or
// edge that exists, the same test as operate/graph.cpp.
func alreadyStored(err error) bool {
	var se *sn.ServiceException
	if !errors.As(err, &se) {
		return false
	}
	m := se.GetMessage()
	return strings.Contains(m, "already existed") || strings.Contains(m, "matching document") ||
		strings.Contains(m, "No matching")
}

// initGraph registers every user of the graph as user<id>/password and
// follows every edge, the Go equivalent of `operate init` (C++). It is only
// run by the explicit init subcommand.
func initGraph(cfg serviceConfig, graphPath string, workers int) error {
	w, err := loadGraph(graphPath)
	if err != nil {
		return err
	}
	userAddr, err := cfg.address("user-service")
	if err != nil {
		return err
	}
	graphAddr, err := cfg.address("social-graph-service")
	if err != nil {
		return err
	}
	edges := make([][2]int64, 0, len(w.edges))
	for e := range w.edges {
		edges = append(edges, e)
	}
	fmt.Printf("init  %s users=%d edges=%d\n", graphPath, len(w.users), len(edges))
	carrier := map[string]string{}

	err = parallel(workers, len(w.users), userAddr, "registered", func(ctx context.Context, c *tconn, i int) error {
		id := w.users[i]
		return sn.NewUserServiceClient(c.client).RegisterUserWithId(ctx, rand.Int63(), "First", "Last",
			username(id), "password", id, carrier)
	})
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	err = parallel(workers, len(edges), graphAddr, "followed", func(ctx context.Context, c *tconn, i int) error {
		e := edges[i]
		return sn.NewSocialGraphServiceClient(c.client).Follow(ctx, rand.Int63(), e[0], e[1], carrier)
	})
	if err != nil {
		return fmt.Errorf("follow: %w", err)
	}
	fmt.Printf("init  done users=%d edges=%d\n", len(w.users), len(edges))
	return nil
}

// parallel runs fn for indexes 0..n-1 on workers connections, retrying each
// item up to 6 times and treating "already stored" as success.
func parallel(workers, n int, addr, verb string, fn func(context.Context, *tconn, int) error) error {
	var (
		next, done atomic.Int64
		once       sync.Once
		failed     error
		wg         sync.WaitGroup
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fail := func(err error) { once.Do(func() { failed = err; cancel() }) }
	for k := 0; k < workers; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := dialThrift(addr)
			if err != nil {
				fail(err)
				return
			}
			defer func() {
				if c != nil { // nil after a failed re-dial
					c.trans.Close()
				}
			}()
			for ctx.Err() == nil {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				var last error
				for attempt := 0; attempt < 6; attempt++ {
					last = fn(ctx, c, i)
					if last == nil || alreadyStored(last) {
						last = nil
						break
					}
					if transportBroken(last) {
						c.trans.Close()
						if c, err = dialThrift(addr); err != nil {
							fail(err)
							return
						}
					}
					time.Sleep(time.Duration(20*(attempt+1)) * time.Millisecond)
				}
				if last != nil {
					fail(fmt.Errorf("item %d: %w", i, last))
					return
				}
				if d := done.Add(1); d%2000 == 0 || d == int64(n) {
					fmt.Printf("init  %s %d/%d\n", verb, d, n)
				}
			}
		}()
	}
	wg.Wait()
	return failed
}

// seedPosts composes perUser posts for every user of the graph through ComposePostService (as compose-post does), so
// user timelines (MongoDB, redis) and home timelines (redis, via WriteHomeTimelineService) hold content before a run.
// Text, mentions and media come from a random source seeded per post, so every seed has the same posts.
func seedPosts(cfg serviceConfig, graphPath string, perUser, workers int) error {
	w, err := loadGraph(graphPath)
	if err != nil {
		return err
	}
	addr, err := cfg.address("compose-post-service")
	if err != nil {
		return err
	}
	n := len(w.users) * perUser
	fmt.Printf("seed  %s users=%d posts=%d\n", graphPath, len(w.users), n)
	carrier := map[string]string{}
	err = parallel(workers, n, addr, "composed", func(ctx context.Context, c *tconn, i int) error {
		r := rand.New(rand.NewSource(int64(i) + 1))
		id := w.users[i%len(w.users)]
		k := r.Intn(5)
		mediaIDs := make([]int64, k)
		mediaTypes := make([]string, k)
		for j := range mediaIDs {
			mediaIDs[j] = r.Int63()
			mediaTypes[j] = "png"
		}
		return sn.NewComposePostServiceClient(c.client).ComposePost(ctx, r.Int63(), username(id), id,
			composeText(r, w.users), mediaIDs, mediaTypes, sn.PostType_POST, carrier)
	})
	if err != nil {
		return fmt.Errorf("compose: %w", err)
	}
	fmt.Printf("seed  done posts=%d\n", n)
	return nil
}

// saveRedis waits until home-timeline-redis's key count has not changed for 5 s (WriteHomeTimelineService fans posts
// out from rabbitmq after ComposePost returns; at most 3 min), then SAVEs it to dump.rdb in the wrapper's data dir.
// redisPoll is saveRedis's interval between DBSIZE reads (shortened in tests).
var redisPoll = time.Second

func saveRedis(cfg serviceConfig) error {
	addr, err := cfg.address("home-timeline-redis")
	if err != nil {
		return err
	}
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	rd := bufio.NewReader(conn)
	cmd := func(name string) (string, error) {
		conn.SetDeadline(time.Now().Add(5 * time.Minute))
		if _, err := fmt.Fprintf(conn, "*1\r\n$%d\r\n%s\r\n", len(name), name); err != nil {
			return "", err
		}
		line, err := rd.ReadString('\n')
		return strings.TrimSpace(line), err
	}
	last, stable := "", 0
	for i := 0; i < 180 && stable < 5; i++ {
		n, err := cmd("DBSIZE")
		if err != nil {
			return fmt.Errorf("redis DBSIZE: %w", err)
		}
		if n == last {
			stable++
		} else {
			stable, last = 0, n
		}
		time.Sleep(redisPoll)
	}
	fmt.Printf("seed  redis keys %s, saving\n", strings.TrimPrefix(last, ":"))
	if r, err := cmd("SAVE"); err != nil || r != "+OK" {
		return fmt.Errorf("redis SAVE: %q %v", r, err)
	}
	return nil
}
