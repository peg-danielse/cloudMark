package main

import (
	"context"
	"math"
	"math/bits"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// histBuckets covers every uint64 duration in nanoseconds. Values below 16 get
// their own bucket; above that each power of two is split into 8 sub-buckets,
// so a bucket's width is at most 1/8 of its lower bound (about 12%).
const histBuckets = 61*8 + 8

// recShards is how many counter shards one recorder spreads its workers over.
// Workers map to shard worker%recShards, so a hot counter is shared by at most
// workers/recShards goroutines instead of all of them.
const recShards = 64

func bucketOf(ns uint64) int {
	if ns < 16 {
		return int(ns)
	}
	shift := bits.Len64(ns) - 4
	return shift*8 + int(ns>>uint(shift))
}

// bucketUpper is the largest value that lands in bucket b.
func bucketUpper(b int) uint64 {
	if b < 16 {
		return uint64(b)
	}
	shift := uint(b/8 - 1)
	m := uint64(b%8 + 8)
	return ((m + 1) << shift) - 1
}

type shard struct {
	sent  atomic.Uint64
	done  atomic.Uint64
	errs  atomic.Uint64
	sumNs atomic.Uint64
	hist  [histBuckets]atomic.Uint64
	_     [64]byte
}

func (s *shard) add(ns uint64) {
	s.done.Add(1)
	s.sumNs.Add(ns)
	s.hist[bucketOf(ns)].Add(1)
}

// recorder counts calls for one route without a lock on the request path.
// A measurement window is the difference of two snapshots.
type recorder struct {
	wg      sync.WaitGroup
	gate    sync.Mutex
	wake    sync.Cond
	active  atomic.Int32
	shards  []shard
	lastErr atomic.Value
	// Open loop (startPoisson): arrivals per second (float64 bits), the free in-flight slots, a request counter.
	poisson bool
	rate    atomic.Uint64
	slots   chan int
	seq     atomic.Int64
}

func newRecorder() *recorder {
	rec := &recorder{shards: make([]shard, recShards)}
	rec.wake.L = &rec.gate
	return rec
}

func (r *recorder) shard(worker int) *shard {
	return &r.shards[worker%len(r.shards)]
}

// setLevel applies one schedule level: the number of active workers (closed loop) or arrivals per second (open loop).
func (r *recorder) setLevel(v int) {
	if r.poisson {
		r.rate.Store(math.Float64bits(float64(v)))
		return
	}
	r.setActive(int32(v))
}

func (r *recorder) setActive(n int32) {
	r.gate.Lock()
	r.active.Store(n)
	r.wake.Broadcast()
	r.gate.Unlock()
}

// waitTurn returns without locking while the worker is active. Only parked
// workers take the gate.
func (r *recorder) waitTurn(ctx context.Context, worker int) bool {
	if int32(worker) < r.active.Load() {
		return ctx.Err() == nil
	}
	r.gate.Lock()
	defer r.gate.Unlock()
	for int32(worker) >= r.active.Load() && ctx.Err() == nil {
		r.wake.Wait()
	}
	return ctx.Err() == nil
}

type snap struct {
	sent  uint64
	done  uint64
	errs  uint64
	sumNs uint64
	hist  [histBuckets]uint64
}

func (r *recorder) snapshot() snap {
	var s snap
	for i := range r.shards {
		sh := &r.shards[i]
		s.sent += sh.sent.Load()
		s.done += sh.done.Load()
		s.errs += sh.errs.Load()
		s.sumNs += sh.sumNs.Load()
		for b := range sh.hist {
			s.hist[b] += sh.hist[b].Load()
		}
	}
	return s
}

// sub returns the counts between before and s.
func (s snap) sub(before snap) snap {
	d := snap{
		sent:  s.sent - before.sent,
		done:  s.done - before.done,
		errs:  s.errs - before.errs,
		sumNs: s.sumNs - before.sumNs,
	}
	for b := range s.hist {
		d.hist[b] = s.hist[b] - before.hist[b]
	}
	return d
}

// percentile returns the upper bound of the bucket holding rank ceil(p*n).
func (s snap) percentile(p float64) uint64 {
	var total uint64
	for _, c := range s.hist {
		total += c
	}
	if total == 0 {
		return 0
	}
	rank := uint64(p*float64(total) + 0.999999999)
	if rank < 1 {
		rank = 1
	}
	if rank > total {
		rank = total
	}
	var seen uint64
	for b, c := range s.hist {
		seen += c
		if seen >= rank {
			return bucketUpper(b)
		}
	}
	return bucketUpper(histBuckets - 1)
}

func startLoad(ctx context.Context, call func(context.Context, int) error, workers, active int) *recorder {
	rec := newRecorder()
	rec.setActive(int32(active))
	// Cancel has to wake parked workers or stop waits on them forever.
	go func() {
		<-ctx.Done()
		rec.gate.Lock()
		rec.wake.Broadcast()
		rec.gate.Unlock()
	}()
	for worker := 0; worker < workers; worker++ {
		rec.wg.Add(1)
		go func(worker int) {
			defer rec.wg.Done()
			sh := rec.shard(worker)
			for rec.waitTurn(ctx, worker) {
				start := time.Now()
				sh.sent.Add(1)
				cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				err := call(cctx, worker)
				cancel()
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					sh.errs.Add(1)
					rec.lastErr.Store(err.Error())
					continue
				}
				sh.add(uint64(time.Since(start)))
			}
		}(worker)
	}
	return rec
}

// startPoisson drives call open-loop: arrivals form a Poisson process (exponential gaps, mean 1/rate) at the rate set
// with setLevel, independent of earlier requests finishing. Each request's latency counts from its scheduled arrival,
// so a backlog in the client is latency, not hidden. At most inflight requests run at once; an arrival that finds no
// slot, a timeout and an error all count in errs. A request holds one slot 0..inflight-1 until it finishes and gets
// it as its worker number, so per-worker state (sized inflight) is never used by two requests at once.
func startPoisson(ctx context.Context, call func(context.Context, int) error, inflight int, timeout time.Duration, seed int64) *recorder {
	rec := newRecorder()
	rec.poisson = true
	rec.slots = make(chan int, inflight)
	for i := 0; i < inflight; i++ {
		rec.slots <- i
	}
	rng := rand.New(rand.NewSource(seed))
	launch := func(sched time.Time) {
		sh := rec.shard(int(rec.seq.Add(1)))
		sh.sent.Add(1)
		var w int
		select {
		case w = <-rec.slots:
		default:
			sh.errs.Add(1)
			rec.lastErr.Store("in-flight cap reached")
			return
		}
		rec.wg.Add(1)
		go func() {
			defer rec.wg.Done()
			defer func() { rec.slots <- w }()
			cctx, cancel := context.WithTimeout(ctx, timeout)
			err := call(cctx, w)
			cancel()
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				sh.errs.Add(1)
				rec.lastErr.Store(err.Error())
				return
			}
			sh.add(uint64(time.Since(sched)))
		}()
	}
	rec.wg.Add(1)
	go func() {
		defer rec.wg.Done()
		next := time.Now()
		for ctx.Err() == nil {
			rate := math.Float64frombits(rec.rate.Load())
			now := time.Now()
			if rate <= 0 {
				next = now
				time.Sleep(5 * time.Millisecond)
				continue
			}
			if now.Sub(next) > time.Second {
				next = now // the dispatcher itself stalled (or the rate just went up from 0): do not burst
			}
			for !next.After(now) {
				launch(next)
				next = next.Add(time.Duration(rng.ExpFloat64() / rate * float64(time.Second)))
			}
			if d := next.Sub(now); d > 0 {
				time.Sleep(d)
			}
		}
	}()
	return rec
}
