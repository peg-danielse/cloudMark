#!/bin/sh
# Start the Jaeger v2 all-in-one built for Mercury/DSB tracing (bin/jaeger, config/mercury-<storage>.yaml).
# Usable three ways: by hand, as a bare helper process from the operators (-jaeger-mode bare), or as a
# Mercury daemon (-jaeger-mode mercury: Mercury execs it with no arguments, so everything is env).
#
# Env (or the matching flag):
#   JAEGER_PORT_OFFSET  --offset N    add N to every default port (0: 6831 6832 14268 14250 4317 4318 16686 16685 13133)
#   JAEGER_STORAGE      --storage S   memory (default) | badger
#   JAEGER_MAX_TRACES   --max-traces  memory storage bound (default 200000)
#   JAEGER_BADGER_DIR   --badger-dir  badger directory (default /tmp/jaeger-badger)
#   JAEGER_CPUS         --cpus LIST   taskset CPU list (user space; empty = inherit)
#   JAEGER_HOST         --host H      listen address (default 127.0.0.1)
#   JAEGER_GOMAXPROCS                 Go procs for the collector (default 2)
#   JAEGER_LOG                        log file (default: stdout/stderr)
# Individual JAEGER_*_PORT variables override the offset. --print-ports prints the ports and exits.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
offset=${JAEGER_PORT_OFFSET:-0}; storage=${JAEGER_STORAGE:-memory}; cpus=${JAEGER_CPUS:-}; print=0
while [ $# -gt 0 ]; do
  case $1 in
    --offset) offset=$2; shift 2;;
    --storage) storage=$2; shift 2;;
    --max-traces) export JAEGER_MAX_TRACES=$2; shift 2;;
    --badger-dir) export JAEGER_BADGER_DIR=$2; shift 2;;
    --cpus) cpus=$2; shift 2;;
    --host) export JAEGER_HOST=$2; shift 2;;
    --print-ports) print=1; shift;;
    --listen-fd) shift 2;;  # Mercury socket hand-off: Jaeger opens its own ports
    *) echo "unknown argument $1" >&2; exit 2;;
  esac
done
p() { eval "v=\${$1:-}"; [ -n "$v" ] || v=$(( $2 + offset )); export "$1=$v"; }
p JAEGER_THRIFT_COMPACT_PORT 6831; p JAEGER_THRIFT_BINARY_PORT 6832; p JAEGER_THRIFT_HTTP_PORT 14268
p JAEGER_GRPC_PORT 14250; p JAEGER_OTLP_GRPC_PORT 4317; p JAEGER_OTLP_HTTP_PORT 4318
p JAEGER_QUERY_HTTP_PORT 16686; p JAEGER_QUERY_GRPC_PORT 16685; p JAEGER_HEALTH_PORT 13133
if [ $print = 1 ]; then
  env | grep -E '^JAEGER_.*_PORT=' | sort; exit 0
fi
cfg=$here/config/mercury-$storage.yaml
[ -f "$cfg" ] || { echo "no config $cfg" >&2; exit 2; }
[ "$storage" = badger ] && mkdir -p "${JAEGER_BADGER_DIR:-/tmp/jaeger-badger}/keys" "${JAEGER_BADGER_DIR:-/tmp/jaeger-badger}/values"
export GOMAXPROCS=${JAEGER_GOMAXPROCS:-2}
# No self-tracing. Jaeger v2 always builds an OTel SDK tracer for itself whose
# exporter defaults to localhost:4317 (not offset, may be someone else's): sample
# nothing, and point the exporter at our own OTLP port anyway.
export OTEL_TRACES_SAMPLER=always_off
export OTEL_EXPORTER_OTLP_ENDPOINT=http://${JAEGER_HOST:-127.0.0.1}:$JAEGER_OTLP_GRPC_PORT
set -- "$here/bin/jaeger" --config "$cfg"
[ -n "$cpus" ] && set -- taskset -c "$cpus" "$@"
if [ -n "${JAEGER_LOG:-}" ]; then exec "$@" >>"$JAEGER_LOG" 2>&1; fi
exec "$@"
