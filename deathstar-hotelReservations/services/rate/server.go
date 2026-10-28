package rate

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/boot"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/cachepolicy"
	pb "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/rate/proto"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/tls"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const name = "srv-rate"

// Server implements the rate service
type Server struct {
	pb.UnimplementedRateServer

	uuid string

	Tracer      opentracing.Tracer
	Port        int
	IpAddr      string
	MongoClient *mongo.Client
	// Registry    *registry.Client
	MemcClient *memcache.Client
}

// Run starts the server
func (s *Server) Run() error {
	opentracing.SetGlobalTracer(s.Tracer)

	if s.Port == 0 {
		return fmt.Errorf("server port must be set")
	}

	s.uuid = uuid.New().String()

	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Timeout: 120 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			PermitWithoutStream: true,
		}),
		grpc.UnaryInterceptor(
			otgrpc.OpenTracingServerInterceptor(s.Tracer),
		),
	}

	if tlsopt := tls.GetServerOpt(); tlsopt != nil {
		opts = append(opts, tlsopt)
	}

	srv := grpc.NewServer(opts...)

	pb.RegisterRateServer(srv, s)

	lis, err := net.Listen("tcp", boot.Bind(s.IpAddr, s.Port))
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}

	log.Info().Msg("skipped registering in consul")
	// err = s.Registry.Register(name, s.uuid, s.IpAddr, s.Port)
	// if err != nil {
	// 	return fmt.Errorf("failed register: %v", err)
	// }
	// log.Info().Msg("Successfully registered in consul")

	return srv.Serve(lis)
}

// Shutdown cleans up any processes
func (s *Server) Shutdown() {
	// s.Registry.Deregister(s.uuid)
}

// GetRates gets rates for hotels for specific date range.
func (s *Server) GetRates(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	res := new(pb.Result)

	ratePlans := make(RatePlans, 0)

	hotelIds := []string{}
	rateMap := make(map[string]struct{})
	for _, hotelID := range req.HotelIds {
		hotelIds = append(hotelIds, hotelID)
		rateMap[hotelID] = struct{}{}
	}
	// first check memcached(get-multi)
	memSpan, _ := opentracing.StartSpanFromContext(ctx, "memcached_get_multi_rate")
	memSpan.SetTag("span.kind", "client")

	resMap, err := s.MemcClient.GetMulti(hotelIds)
	memSpan.Finish()

	var wg sync.WaitGroup
	var mutex sync.Mutex
	if err != nil && err != memcache.ErrCacheMiss {
		// Panic restarts the pod, and startup used to insert another inventory
		// copy. A slow memcached read should fail this call only.
		log.Error().Err(err).Msgf("memcached get multi failed for hotels %v", hotelIds)
		return nil, status.Errorf(codes.Unavailable, "memcached get multi: %v", err)
	} else {
		for hotelId, item := range resMap {
			rateStrs := strings.Split(string(item.Value), "\n")
			log.Trace().Msgf("memc hit, hotelId = %s,rate strings: %v", hotelId, rateStrs)

			for _, rateStr := range rateStrs {
				if len(rateStr) != 0 {
					rateP := new(pb.RatePlan)
					json.Unmarshal([]byte(rateStr), rateP)
					ratePlans = append(ratePlans, rateP)
				}
			}

			delete(rateMap, hotelId)
		}

		wg.Add(len(rateMap))
		for hotelId := range rateMap {
			go func(id string) {
				log.Trace().Msgf("memc miss, hotelId = %s", id)
				log.Trace().Msg("memcached miss, set up mongo connection")

				mongoSpan, _ := opentracing.StartSpanFromContext(ctx, "mongo_rate")
				mongoSpan.SetTag("span.kind", "client")

				// One hotel only. A miss used to read the whole inventory and
				// store it under this key, which filled the cache after one call.
				collection := s.MongoClient.Database("rate-db").Collection("inventory")
				curr, err := collection.Find(context.TODO(), bson.D{{"hotelId", id}})
				if err != nil {
					log.Error().Msgf("Failed get rate data: ", err)
				} else {
					defer curr.Close(context.TODO())
				}

				tmpRatePlans := make(RatePlans, 0)
				if curr != nil {
					curr.All(context.TODO(), &tmpRatePlans)
				}
				if err != nil {
					log.Error().Msgf("Failed get rate data: ", err)
				}

				mongoSpan.Finish()

				memcStr := ""
				if err != nil {
					log.Panic().Msgf("Tried to find hotelId [%v], but got error", id, err.Error())
				} else {
					for _, r := range tmpRatePlans {
						mutex.Lock()
						ratePlans = append(ratePlans, r)
						mutex.Unlock()
						rateJson, err := json.Marshal(r)
						if err != nil {
							log.Error().Msgf("Failed to marshal plan [Code: %v] with error: %s", r.Code, err)
						}
						memcStr = memcStr + string(rateJson) + "\n"
					}
				}
				if cachepolicy.Hot(id) {
					go s.MemcClient.Set(&memcache.Item{Key: id, Value: []byte(memcStr)})
				}

				defer wg.Done()
			}(hotelId)
		}
	}
	wg.Wait()

	sort.Sort(ratePlans)
	res.RatePlans = ratePlans

	return res, nil
}

// WarmHot drops whatever is in memcached and stores the ids cachepolicy keeps.
func WarmHot(mongoClient *mongo.Client, memc *memcache.Client) {
	if err := memc.FlushAll(); err != nil {
		log.Error().Err(err).Msg("memcached-rate flush before hot warmup failed")
	}
	collection := mongoClient.Database("rate-db").Collection("inventory")
	warmed := 0
	for _, id := range cachepolicy.HotIDs() {
		curr, err := collection.Find(context.TODO(), bson.D{{"hotelId", id}})
		if err != nil {
			log.Error().Err(err).Str("hotelId", id).Msg("hot rate warmup query failed")
			continue
		}
		plans := make(RatePlans, 0)
		if err = curr.All(context.TODO(), &plans); err != nil {
			log.Error().Err(err).Str("hotelId", id).Msg("hot rate warmup decode failed")
			curr.Close(context.TODO())
			continue
		}
		curr.Close(context.TODO())
		memcStr := ""
		for _, r := range plans {
			rateJSON, err := json.Marshal(r)
			if err != nil {
				log.Error().Err(err).Str("hotelId", id).Msg("hot rate warmup marshal failed")
				continue
			}
			memcStr += string(rateJSON) + "\n"
		}
		if err := memc.Set(&memcache.Item{Key: id, Value: []byte(memcStr)}); err != nil {
			log.Error().Err(err).Str("hotelId", id).Msg("hot rate warmup set failed")
			continue
		}
		warmed++
	}
	log.Info().Int("keys", warmed).Msg("warmed hot rate keys")
}

type RatePlans []*pb.RatePlan

func (r RatePlans) Len() int {
	return len(r)
}

func (r RatePlans) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r RatePlans) Less(i, j int) bool {
	return r[i].RoomType.TotalRate > r[j].RoomType.TotalRate
}
