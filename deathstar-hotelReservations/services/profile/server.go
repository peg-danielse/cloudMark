package profile

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/boot"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/cachepolicy"
	pb "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/profile/proto"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/tls"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const name = "srv-profile"

// Server implements the profile service
type Server struct {
	pb.UnimplementedProfileServer

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

	log.Trace().Msgf("in run s.IpAddr = %s, port = %d", s.IpAddr, s.Port)

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

	pb.RegisterProfileServer(srv, s)

	lis, err := net.Listen("tcp", boot.Bind(s.IpAddr, s.Port))
	if err != nil {
		log.Fatal().Msgf("failed to configure listener: %v", err)
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

// GetProfiles returns hotel profiles for requested IDs
func (s *Server) GetProfiles(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	log.Trace().Msgf("In GetProfiles")

	var wg sync.WaitGroup
	var mutex sync.Mutex

	// one hotel should only have one profile
	hotelIds := make([]string, 0)
	profileMap := make(map[string]struct{})
	for _, hotelId := range req.HotelIds {
		hotelIds = append(hotelIds, hotelId)
		profileMap[hotelId] = struct{}{}
	}

	memSpan, _ := opentracing.StartSpanFromContext(ctx, "memcached_get_profile")
	memSpan.SetTag("span.kind", "client")
	resMap, err := s.MemcClient.GetMulti(hotelIds)
	memSpan.Finish()

	res := new(pb.Result)
	hotels := make([]*pb.Hotel, 0)

	if err != nil && err != memcache.ErrCacheMiss {
		log.Panic().Msgf("Tried to get hotelIds [%v], but got memmcached error = %s", hotelIds, err)
	} else {
		for hotelId, item := range resMap {
			profileStr := string(item.Value)
			log.Trace().Msgf("memc hit with %v", profileStr)

			hotelProf := new(pb.Hotel)
			json.Unmarshal(item.Value, hotelProf)
			hotels = append(hotels, hotelProf)
			delete(profileMap, hotelId)
		}

		wg.Add(len(profileMap))
		for hotelId := range profileMap {
			go func(hotelId string) {
				var hotelProf *pb.Hotel

				collection := s.MongoClient.Database("profile-db").Collection("hotels")

				mongoSpan, _ := opentracing.StartSpanFromContext(ctx, "mongo_profile")
				mongoSpan.SetTag("span.kind", "client")
				err := collection.FindOne(context.TODO(), bson.D{{"id", hotelId}}).Decode(&hotelProf)
				mongoSpan.Finish()

				if err != nil {
					log.Error().Msgf("Failed get hotels data: ", err)
				}

				mutex.Lock()
				hotels = append(hotels, hotelProf)
				mutex.Unlock()

				profJson, err := json.Marshal(hotelProf)
				if err != nil {
					log.Error().Msgf("Failed to marshal hotel [id: %v] with err:", hotelProf.Id, err)
				}
				memcStr := string(profJson)

				if cachepolicy.Hot(hotelId) {
					go s.MemcClient.Set(&memcache.Item{Key: hotelId, Value: []byte(memcStr)})
				}
				defer wg.Done()
			}(hotelId)
		}
	}
	wg.Wait()

	res.Hotels = hotels
	log.Trace().Msgf("In GetProfiles after getting resp")
	return res, nil
}

// WarmHot drops whatever is in memcached and stores the ids cachepolicy keeps.
func WarmHot(mongoClient *mongo.Client, memc *memcache.Client) {
	if err := memc.FlushAll(); err != nil {
		log.Error().Err(err).Msg("memcached-profile flush before hot warmup failed")
	}
	collection := mongoClient.Database("profile-db").Collection("hotels")
	warmed := 0
	for _, id := range cachepolicy.HotIDs() {
		var hotelProf pb.Hotel
		err := collection.FindOne(context.TODO(), bson.D{{"id", id}}).Decode(&hotelProf)
		if err != nil {
			log.Error().Err(err).Str("hotelId", id).Msg("hot profile warmup query failed")
			continue
		}
		profJSON, err := json.Marshal(&hotelProf)
		if err != nil {
			log.Error().Err(err).Str("hotelId", id).Msg("hot profile warmup marshal failed")
			continue
		}
		if err := memc.Set(&memcache.Item{Key: id, Value: profJSON}); err != nil {
			log.Error().Err(err).Str("hotelId", id).Msg("hot profile warmup set failed")
			continue
		}
		warmed++
	}
	log.Info().Int("keys", warmed).Msg("warmed hot profile keys")
}
