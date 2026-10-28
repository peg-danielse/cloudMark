package tracing

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"github.com/uber/jaeger-client-go/config"
)

var (
	defaultSampleRatio float64 = 0.01
)

// collectorEndpoint maps legacy jaeger-agent UDP addresses and bare host:port
// values to the HTTP collector API used by modern Jaeger all-in-one images.
func collectorEndpoint(host string) string {
	if strings.HasPrefix(host, "http://") || strings.HasPrefix(host, "https://") {
		if strings.Contains(host, "/api/traces") {
			return host
		}
		return strings.TrimRight(host, "/") + "/api/traces"
	}

	hostPart, port, hasPort := strings.Cut(host, ":")
	if !hasPort {
		hostPart = host
		port = "6831"
	}

	switch port {
	case "6831", "5775", "5778":
		return fmt.Sprintf("http://%s:14268/api/traces", hostPart)
	case "14268":
		return fmt.Sprintf("http://%s:14268/api/traces", hostPart)
	default:
		return ""
	}
}

// Init returns a newly configured tracer
func Init(serviceName, host string) (opentracing.Tracer, error) {
	ratio := defaultSampleRatio
	if val, ok := os.LookupEnv("JAEGER_SAMPLE_RATIO"); ok {
		ratio, _ = strconv.ParseFloat(val, 64)
		if ratio > 1 {
			ratio = 1.0
		}
	}

	log.Info().Msgf("Jaeger client: adjusted sample ratio %f", ratio)

	reporter := &config.ReporterConfig{
		LogSpans:            false,
		BufferFlushInterval: 1 * time.Second,
	}
	if endpoint := collectorEndpoint(host); endpoint != "" {
		log.Info().Msgf("Jaeger client: HTTP collector %s", endpoint)
		reporter.CollectorEndpoint = endpoint
	} else {
		log.Info().Msgf("Jaeger client: UDP agent %s", host)
		reporter.LocalAgentHostPort = host
	}

	tempCfg := &config.Configuration{
		ServiceName: serviceName,
		Sampler: &config.SamplerConfig{
			Type:  "probabilistic",
			Param: ratio,
		},
		Reporter: reporter,
	}

	log.Info().Msg("Overriding Jaeger config with env variables")
	cfg, err := tempCfg.FromEnv()
	if err != nil {
		return nil, err
	}

	tracer, _, err := cfg.NewTracer()
	if err != nil {
		return nil, err
	}
	return tracer, nil
}
