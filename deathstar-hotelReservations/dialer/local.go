package dialer

import (
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/boot"
	opentracing "github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

// ServiceAddr is the host:port configured for name.
func ServiceAddr(cfg boot.Config, name string) (string, error) {
	return cfg.Address(name)
}

// DialService dials the address configured for name.
func DialService(name string, tracer opentracing.Tracer) (*grpc.ClientConn, error) {
	addr, err := ServiceAddr(boot.Load(), name)
	if err != nil {
		return nil, err
	}
	var opts []DialOption
	if tracer != nil {
		opts = append(opts, WithTracer(tracer))
	}
	return Dial(addr, opts...)
}
