package main

// The routes the load generator drives. Each is one request built from the seeded data:
//
//   read-page          PageService.ReadPage for a random movie (the page the DeathStarBench front end reads)
//   compose-review     the review write of the DeathStarBench wrk2 script: user, text, movie id and unique id
//                      uploads, in parallel, as the nginx compose route does
//   read-movie-reviews MovieReviewService.ReadMovieReviews for a random movie
//
// read-user-reviews is not here: the seeded users have service-assigned ids, not the usernames the writes use.

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"mediaoperate/gen/media_service"
)

const (
	routeRead         = "read-page"
	routeCompose      = "compose-review"
	routeMovieReviews = "read-movie-reviews"
	reviewStart       = 0
	reviewStop        = 10
	composeTextLen    = 100
	textCharset       = "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890"
)

// routeDef is one route: the service-config keys it dials and the request it sends. call gets the route's state and
// the slot (worker or in-flight number) it runs in, so per-slot connections and random sources are never shared.
type routeDef struct {
	services []string
	call     func(ctx context.Context, rt *routeRun, slot int) error
}

var routeDefs = map[string]routeDef{
	routeRead: {
		services: []string{"page-service"},
		call:     callReadPage,
	},
	routeCompose: {
		services: []string{"user-service", "text-service", "movie-id-service", "unique-id-service"},
		call:     callComposeReview,
	},
	routeMovieReviews: {
		services: []string{"movie-review-service"},
		call:     callReadMovieReviews,
	},
}

// routeNames lists every route in its default order.
var routeNames = []string{routeRead, routeCompose, routeMovieReviews}

// routeRun is what one route's requests draw from: the seeded movies and users, one random source per slot, and one
// connection pool per service it dials.
type routeRun struct {
	movies []movie
	users  int
	rngs   []*rand.Rand
	pools  map[string]*connPool
}

func newRouteRun(name string, movies []movie, users, slots int, seed int64, addr map[string]string, timeout time.Duration) *routeRun {
	rt := &routeRun{movies: movies, users: users, rngs: make([]*rand.Rand, slots), pools: map[string]*connPool{}}
	for i := range rt.rngs {
		rt.rngs[i] = rand.New(rand.NewSource(seed + int64(i)))
	}
	for _, svc := range routeDefs[name].services {
		rt.pools[svc] = &connPool{addr: addr[svc], timeout: timeout, conns: make([]*pooledConn, slots)}
	}
	return rt
}

// connPool holds one Thrift connection per slot. A slot runs one request at a time, so its connection needs no lock.
type connPool struct {
	addr    string
	timeout time.Duration
	conns   []*pooledConn
}

type pooledConn struct {
	trans  thrift.TTransport
	client thrift.TClient
}

// do runs fn on the slot's connection, opening it on first use. A failed call closes the connection, because the
// framed stream may be out of step; the next call on this slot dials again.
func (p *connPool) do(slot int, fn func(c thrift.TClient) error) error {
	if p.conns[slot] == nil {
		cfg := &thrift.TConfiguration{ConnectTimeout: 3 * time.Second, SocketTimeout: p.timeout}
		sock := thrift.NewTSocketConf(p.addr, cfg)
		trans := thrift.NewTFramedTransportConf(sock, cfg)
		if err := trans.Open(); err != nil {
			return err
		}
		proto := thrift.NewTBinaryProtocolConf(trans, cfg)
		p.conns[slot] = &pooledConn{trans: trans, client: thrift.NewTStandardClient(proto, proto)}
	}
	c := p.conns[slot]
	err := fn(c.client)
	if err != nil {
		c.trans.Close()
		p.conns[slot] = nil
	}
	return err
}

func callReadPage(ctx context.Context, rt *routeRun, slot int) error {
	r := rt.rngs[slot]
	m := rt.movies[r.Intn(len(rt.movies))]
	return rt.pools["page-service"].do(slot, func(c thrift.TClient) error {
		_, err := media_service.NewPageServiceClient(c).ReadPage(ctx, r.Int63(), m.id, reviewStart, reviewStop,
			map[string]string{})
		return err
	})
}

func callReadMovieReviews(ctx context.Context, rt *routeRun, slot int) error {
	r := rt.rngs[slot]
	m := rt.movies[r.Intn(len(rt.movies))]
	return rt.pools["movie-review-service"].do(slot, func(c thrift.TClient) error {
		_, err := media_service.NewMovieReviewServiceClient(c).ReadMovieReviews(ctx, r.Int63(), m.id, reviewStart,
			reviewStop, map[string]string{})
		return err
	})
}

// callComposeReview sends the four uploads of one review in parallel, as the nginx compose route does, and fails if
// any of them fails. The request's random choices are made here, before the fan-out, on this slot's source.
func callComposeReview(ctx context.Context, rt *routeRun, slot int) error {
	r := rt.rngs[slot]
	reqID := r.Int63()
	m := rt.movies[r.Intn(len(rt.movies))]
	user := fmt.Sprintf("username_%d", 1+r.Intn(rt.users))
	text := randomText(r, composeTextLen)
	rating := int32(r.Intn(11))
	carrier := map[string]string{}
	calls := []func() error{
		func() error {
			return rt.pools["user-service"].do(slot, func(c thrift.TClient) error {
				return media_service.NewUserServiceClient(c).UploadUserWithUsername(ctx, reqID, user, carrier)
			})
		},
		func() error {
			return rt.pools["text-service"].do(slot, func(c thrift.TClient) error {
				return media_service.NewTextServiceClient(c).UploadText(ctx, reqID, text, carrier)
			})
		},
		func() error {
			return rt.pools["movie-id-service"].do(slot, func(c thrift.TClient) error {
				return media_service.NewMovieIdServiceClient(c).UploadMovieId(ctx, reqID, m.title, rating, carrier)
			})
		},
		func() error {
			return rt.pools["unique-id-service"].do(slot, func(c thrift.TClient) error {
				return media_service.NewUniqueIdServiceClient(c).UploadUniqueId(ctx, reqID, carrier)
			})
		},
	}
	errs := make([]error, len(calls))
	var wg sync.WaitGroup
	for i, fn := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn()
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func randomText(r *rand.Rand, n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteByte(textCharset[r.Intn(len(textCharset))])
	}
	return b.String()
}
