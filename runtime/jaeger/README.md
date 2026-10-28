# Jaeger for Mercury / DeathStarBench (critical-path attribution)

Built on Apollo (Paul's PC) 2026-10-19; nothing here was built or run on dtn-01.

## Contents

| path | what |
|---|---|
| `bin/jaeger` | Jaeger **v2.22.0** all-in-one (commit 462bb49f), linux/amd64, `CGO_ENABLED=0`, statically linked, stripped, 75.5 MB. sha256 `eed3143da7c308eff6a458a9feab58705c9091127684cc9880a937a3ff24397c`. UI assets not embedded (placeholder page); the query API is complete. |
| `config/mercury-memory.yaml`, `config/mercury-badger.yaml` | collector + query config; every port from env |
| `run-jaeger.sh` | launcher (env- or flag-configured; works with no arguments as a Mercury daemon) |
| `hotel-jaeger.bundle`, `social-jaeger.bundle` | git bundles with branches `jaeger` and `jaeger-dtn01` |
| `patches/` | the same change as patches (see below) |
| `fake_mercury.py` | user-space stand-in for Mercury's /start /kill /cpus /scale, used for the PC end-to-end test only |

Build: `git clone --depth 1 -b v2.22.0 https://github.com/jaegertracing/jaeger src && cd src &&
GOTOOLCHAIN=go1.27.2 CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "-s -w
-X github.com/jaegertracing/jaeger/internal/version.commitSHA=462bb49f...
-X .../version.latestVersion=v2.22.0 -X .../version.date=2026-10-06T11:44:34Z" -o ../bin/jaeger ./cmd/jaeger`
(v2.22.0 needs Go >= 1.27; the toolchain was fetched in user space). Static, so dtn-01's glibc
(2.41) / kernel (6.12) do not matter.

## Ports

Default ports plus `JAEGER_PORT_OFFSET` (`--offset N`, operators: `-jaeger-port-offset N`):

| port | protocol | used by |
|---|---|---|
| 6831/udp | jaeger.thrift compact (agent) | social with `-trace-transport udp` |
| 6832/udp | jaeger.thrift binary | - |
| 14268 | jaeger.thrift over HTTP (`/api/traces`) | hotel (always), social (default) |
| 14250 | jaeger gRPC | - |
| 4317 / 4318 | OTLP gRPC / HTTP | - (also Jaeger's own disabled self-tracer) |
| 16686 | query HTTP: `/api/v3/traces`, `/api/v3/services` | operators' analyser |
| 16685 | query gRPC | - |
| 13133 | health check | - |

Everything listens on 127.0.0.1 (`JAEGER_HOST`). Suggested on dtn-01: `-jaeger-port-offset 20000`
(26831, 26832, 34268, 34250, 24317, 24318, 36686, 36685, 33133). The operators refuse an offset that
lands any port on `-jaeger-avoid-ports` (default `8080,18180-18199`, Kurt's) and check every port is
free before starting. Jaeger's own OTel self-tracing is off (`OTEL_TRACES_SAMPLER=always_off`, exporter
pointed at its own OTLP port), so it never dials an un-offset localhost:4317.

`./run-jaeger.sh --offset 20000 --print-ports` lists them. Other knobs: `--storage memory|badger`,
`--max-traces` (memory, default 200000), `--badger-dir`, `--cpus LIST` (taskset), `JAEGER_GOMAXPROCS`
(default 2), `JAEGER_LOG`, `JAEGER_LOG_LEVEL` (warn).

## Operators (branch `jaeger` in deathstar-hotelReservations and deathstar-socialNetwork)

Flags (same in `operate` and `goperate`):

```
-jaeger                    on/off (default off: nothing changes)
-jaeger-mode bare|mercury|external
                           bare (default): run-jaeger.sh as a child process, taskset -jaeger-cpus, no sudo
                           mercury: POST /start (cpu = -cpu) + POST /cpus -jaeger-cpus, no /scale policy;
                                    killed with the other daemons (id hotel-jaeger / social-jaeger)
                           external: use a running Jaeger
-jaeger-cpus 62            helper CPU(s), off the service pool
-jaeger-port-offset 0      see Ports;  -jaeger-host 127.0.0.1;  -jaeger-addr host:port (query API override)
-jaeger-storage memory     memory|badger;  -jaeger-max-traces 200000;  -jaeger-bin <dir>/run-jaeger.sh
-trace-sample 0.02         probabilistic rate the apps are started with
-trace-transport http      http|udp (udp only for social; hotel's client always posts HTTP)
-trace-lag 3s              wait after a window before querying it
-trace-depth 5000          max traces fetched per window (truncated=1 in the CSV if hit)
-trace-out                 default: traces.csv next to -gen-out
-trace-cp-lines true       also print "cp window ..." lines into the main output
-jaeger-avoid-ports 8080,18180-18199
```

How the apps are hooked in:
* hotel (Go, jaeger-client-go): `/start` env `JAEGER_SAMPLE_RATIO=<rate>`,
  `JAEGER_ENDPOINT=http://host:14268+off/api/traces`, `JAEGER_REPORTER_MAX_QUEUE_SIZE=10000`
  (read by `tracing.Init` / `FromEnv`). config.json untouched. Without -jaeger the services keep
  their built-in 1 % to 127.0.0.1:14268.
* social (C++, jaeger-client-cpp, reads only `config/jaeger-config.yml` relative to its cwd; the repo
  copy is `sampler: const 0`, i.e. tracing off): goperate writes `<traces dir>/social-cwd/config/`
  with a generated `jaeger-config.yml` (probabilistic `<rate>`, reporter endpoint or
  localAgentHostPort, flush 1 s, queue 10000) and a symlink to the real service-config.json, and
  passes `SOCIAL_RUNDIR` to `run/launch`, which cds there. No rebuild, repo config unchanged.

Per window the load generator prints `window <n> level <l> wi <i> clients <c> start_us <a> end_us <b>`
(route parsers only match `^route`). The operator sees it in its stdout copy, waits -trace-lag, GETs
`/api/v3/traces?query.startTimeMin=..&query.startTimeMax=..&query.searchDepth=..` asynchronously, keeps
traces whose root starts in the window, and computes a CRISP critical path per trace.

traces.csv (one row per window x route x daemon):

```
window,level,wi,clients,start_unix_us,end_unix_us,route,daemon,crit_share,crit_share_tail,rpc_share,
excl_ms_p50,excl_ms_p95,excl_ms_mean,span_count,sample_count,root_ms_p50,root_ms_p95,fetched,truncated
```

* `daemon` = Mercury program basename (geo, 11212, mongod, ComposePostService, redis-server, ...).
* `crit_share` = daemon's critical-path time / total request time over the window's sampled requests
  (sums to 1 per route); `crit_share_tail` the same over requests >= the route's p90;
  `rpc_share` = part of crit_share spent in client spans in front of that daemon (network/queueing).
* `excl_ms_*` = per-request critical-path ms of that daemon (0 when absent).
* `span_count` spans owned by the daemon; `sample_count` sampled requests of the route.
* `fetched` traces returned for the window, `truncated` 1 if = -trace-depth.

## dtn-01: what to do after Kurt's runs (nothing has been run there)

Two branches per repo in the bundles:
* `jaeger-dtn01`: dtn-01's committed HEAD + Kurt's uncommitted working tree as of 2026-10-19 10:05
  (snapshot commit) + the Jaeger commit. Use this on dtn-01.
* `jaeger`: the same Jaeger commit on Paul's `pc` branch (DSB_ROOT override, PC run/ paths).

```
cd ~/Benchmarks/deathstar-hotelReservations && git fetch ~/Benchmarks/jaeger/hotel-jaeger.bundle jaeger:jaeger jaeger-dtn01:jaeger-dtn01
cd ~/Benchmarks/deathstar-socialNetwork   && git fetch ~/Benchmarks/jaeger/social-jaeger.bundle jaeger:jaeger jaeger-dtn01:jaeger-dtn01
```

Or, on top of whatever Kurt has uncommitted then: `git apply --3way patches/<app>-jaeger-on-dtn01-worktree.patch`.
Then rebuild bin/loadgen (hotel `go build -o bin/loadgen ./operate`; social `cd goperate && go build -o bin/loadgen .`),
and e.g. `operate ... -jaeger -jaeger-port-offset 20000 -jaeger-cpus 62 autoscale`. With
`-jaeger-mode mercury` Jaeger runs as root under Mercury in its own cgroup; bare mode runs it as paul.

## Tests (PC)

* Unit tests: `go test ./operate` (hotel; `DSB_ROOT` defaults to this repository's `benchmarks/`) and
  `cd goperate && go test -mod=vendor .` (social): hand-built v3 traces with exact critical paths
  (sequential, parallel, async consumer, store calls, RPC overhead), real recorded hotel traces,
  a fake Jaeger HTTP server end to end, marker/sniffer/flags.
* End to end (hotel, PC, no sudo, fake_mercury.py instead of Mercury, user-space mongod):
  Jaeger bare on CPUs 30-31, offset 20000, 15 hotel daemons + loadgen, 1/2/4/8 users; every window
  analysed, 0 failed, sampled counts match rate x sampling, shares sum to 1.
* social end to end was not possible on the PC (the C++ binaries need libboost_thread 1.83).
