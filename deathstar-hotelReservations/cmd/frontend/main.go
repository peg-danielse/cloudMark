package main

import (
	"flag"
	"os"
	"time"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/boot"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/frontend"
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

	servIP, servPort, err := cfg.HostPort("frontend")
	if err != nil {
		log.Fatal().Err(err).Msg("config")
	}

	var (
		jaegerAddr = flag.String("jaegeraddr", cfg.Jaeger, "Jaeger address")
		// consulAddr = flag.String("consuladdr", result["consulAddress"], "Consul address")
	)
	flag.Parse()
	servPort = boot.ListenPort(servPort)
	log.Info().Msgf("Initializing jaeger agent [service name: %v | host: %v]...", "frontend", *jaegerAddr)
	tracer, err := tracing.Init("frontend", *jaegerAddr)
	if err != nil {
		log.Panic().Msgf("Got error while initializing jaeger agent: %v", err)
	}
	log.Info().Msg("Jaeger agent initialized")

	// log.Info().Msgf("Initializing consul agent [host: %v]...", *consulAddr)
	// registry, err := registry.NewClient(*consulAddr)
	// if err != nil {
	// log.Panic().Msgf("Got error while initializing consul agent: %v", err)
	// }
	// log.Info().Msg("Consul agent initialized")
	log.Info().Msg("Consul agent skipped")

	srv := &frontend.Server{
		// Registry:   registry,
		Tracer: tracer,
		IpAddr: servIP,
		// ConsulAddr: *consulAddr,
		Port: servPort,
	}

	log.Info().Msg("Starting server...")
	log.Fatal().Msg(srv.Run().Error())
}
