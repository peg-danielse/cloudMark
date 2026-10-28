package main

import (
	"flag"
	"os"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/boot"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/cachepolicy"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/rate"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/tracing"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/tune"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"time"
)

func main() {
	tune.Init()
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Caller().Logger()

	log.Info().Msg("Reading config...")
	cfg := boot.Load()

	log.Info().Msg("Initializing DB connection...")
	mongoClient, reseeded, mongoClose := initializeDatabase(cfg.Mongo)
	defer mongoClose()

	log.Info().Msgf("Read profile memcashed address: %v", cfg.Memcached["rate"])
	log.Info().Msg("Initializing Memcashed client...")
	memcClient := tune.NewMemCClient2(cfg.Memcached["rate"])
	log.Info().Bool("reseeded", reseeded).Bool("cold", cachepolicy.Cold()).Msg("warming rate keys")
	rate.WarmHot(mongoClient, memcClient)
	log.Info().Msg("Success")

	servIP, servPort, err := cfg.HostPort("srv-rate")
	if err != nil {
		log.Fatal().Err(err).Msg("config")
	}

	var (
		jaegerAddr = flag.String("jaegeraddr", cfg.Jaeger, "Jaeger address")
		// consulAddr = flag.String("consuladdr", result["consulAddress"], "Consul address")
	)
	flag.Parse()
	servPort = boot.ListenPort(servPort)

	log.Info().Msgf("Initializing jaeger agent [service name: %v | host: %v]...", "rate", *jaegerAddr)
	tracer, err := tracing.Init("rate", *jaegerAddr)
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

	srv := &rate.Server{
		Tracer: tracer,
		// Registry:    registry,
		Port:        servPort,
		IpAddr:      servIP,
		MongoClient: mongoClient,
		MemcClient:  memcClient,
	}

	log.Info().Msg("Starting server...")
	log.Fatal().Msg(srv.Run().Error())
}
