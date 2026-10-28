package main

import (
	"flag"
	"os"
	"time"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/boot"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/cachepolicy"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/reservation"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/tracing"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/tune"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	tune.Init()
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Caller().Logger()

	log.Info().Msg("Reading config...")
	cfg := boot.Load()

	log.Info().Msg("Initializing DB connection...")
	mongoClient, mongoClose := initializeDatabase(cfg.Mongo)
	defer mongoClose()

	log.Info().Msgf("Read profile memcashed address: %v", cfg.Memcached["reservation"])
	log.Info().Msg("Initializing Memcashed client...")
	memcClient := tune.NewMemCClient2(cfg.Memcached["reservation"])
	log.Info().Bool("cold", cachepolicy.Cold()).Msg("warming reservation capacities")
	reservation.WarmHot(mongoClient, memcClient)
	log.Info().Msg("Success")

	servIP, servPort, err := cfg.HostPort("srv-reservation")
	if err != nil {
		log.Fatal().Err(err).Msg("config")
	}

	var (
		jaegerAddr = flag.String("jaegeraddr", cfg.Jaeger, "Jaeger address")
		// consulAddr = flag.String("consuladdr", result["consulAddress"], "Consul address")
	)
	flag.Parse()
	servPort = boot.ListenPort(servPort)

	log.Info().Msgf("Initializing jaeger agent [service name: %v | host: %v]...", "reservation", *jaegerAddr)
	tracer, err := tracing.Init("reservation", *jaegerAddr)
	if err != nil {
		log.Panic().Msgf("Got error while initializing jaeger agent: %v", err)
	}
	log.Info().Msg("Jaeger agent initialized")

	// log.Info().Msgf("Initializing consul agent [host: %v]...", *consulAddr)
	// registry, err := registry.NewClient(*consulAddr)
	// if err != nil {
	// 	log.Panic().Msgf("Got error while initializing consul agent: %v", err)
	// }
	// log.Info().Msg("Consul agent initialized")
	log.Info().Msg("Consul agent skipped")

	srv := &reservation.Server{
		Tracer: tracer,
		// Registry:    registry,
		Port:        servPort,
		IpAddr:      servIP,
		MongoClient: mongoClient,
		MemcClient:  memcClient,
	}

	log.Info().Msg("Starting server...")
	if err := srv.Run(); err != nil {
		log.Fatal().Err(err).Msg("server stopped")
	}
}
