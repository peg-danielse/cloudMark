package main

import (
	"flag"
	"os"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/boot"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/attractions"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/tracing"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/tune"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"time"
	// "github.com/bradfitz/gomemcache/memcache"
)

func main() {
	tune.Init()
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Caller().Logger()

	log.Info().Msg("Reading config...")
	cfg := boot.Load()

	log.Info().Msgf("Read database URL: %v", cfg.Mongo)
	log.Info().Msg("Initializing DB connection...")
	mongo_session, mongoClose := initializeDatabase(cfg.Mongo)
	defer mongoClose()
	log.Info().Msg("Successfull")

	serv_ip, serv_port, err := cfg.HostPort("srv-attractions")
	if err != nil {
		log.Fatal().Err(err).Msg("config")
	}
	log.Info().Msgf("Read target port: %v", serv_port)
	log.Info().Msgf("Read jaeger address: %v", cfg.Jaeger)

	var (
		// port       = flag.Int("port", 8081, "The server port")
		jaegeraddr = flag.String("jaegeraddr", cfg.Jaeger, "Jaeger server addr")
		// consuladdr = flag.String("consuladdr", result["consulAddress"], "Consul address")
	)
	flag.Parse()
	serv_port = boot.ListenPort(serv_port)

	log.Info().Msgf("Initializing jaeger agent [service name: %v | host: %v]...", "attractions", *jaegeraddr)
	tracer, err := tracing.Init("attractions", *jaegeraddr)
	if err != nil {
		log.Panic().Msgf("Got error while initializing jaeger agent: %v", err)
	}
	log.Info().Msg("Jaeger agent initialized")

	// log.Info().Msgf("Initializing consul agent [host: %v]...", *consuladdr)
	// registry, err := registry.NewClient(*consuladdr)
	// if err != nil {
	// log.Panic().Msgf("Got error while initializing consul agent: %v", err)
	// }
	// log.Info().Msg("Consul agent initialized")
	log.Info().Msg("Consul agent skipped")

	srv := attractions.Server{
		Tracer: tracer,
		// Port:     *port,
		// Registry:    registry,
		Port:        serv_port,
		IpAddr:      serv_ip,
		MongoClient: mongo_session,
	}

	log.Info().Msg("Starting server...")
	log.Fatal().Msg(srv.Run().Error())
}
