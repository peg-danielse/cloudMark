# perturb: vulnerability labels for the inference-aware policy

Labels for "how much does a route's rate/p95/p99 change when process d loses, moves or shares CPU",
in one CSV schema (`common.FIELDS` + `STRESS_FIELDS`), from three sources:

| source | tool | what |
|---|---|---|
| `perturb` | `perturb.py` / `perturb.sh` | controlled A-B-A on a frozen cpuset: remove, smt, l3, numa, migrate |
| `perturb` (kind `stress`) | same | stress-ng noisy neighbour on d's core / SMT sibling / L3 / NUMA node |
| `weak:*` | `weak_labels.py` | saturate/gnn add/remove/move events found in existing operator logs (read-only) |

## Training principle: vulnerability only shows under pressure

Perturbations under light load teach the model almost nothing, because a process only shows its
vulnerability when it's near its limit. Therefore:

- run stress-ng as a noisy neighbour on the same core, the SMT sibling, the same L3, or the same
  NUMA node (cpu/cache/memory-bandwidth/matrix stressors at several intensities);
- run perturbation experiments at loads near and beyond the 1024-user knee;
- hold out some stress-ng mixes from training to test generalisation to unseen pressure.

In the tools: `--kind stress --stressor cpu|cache|stream|vm|matrix --placement core|smt|l3|numa
--intensity low|mid|high`; `--clients 256,512,1024,2048` with the operator at `-users 2048`;
`--heldout stream:l3,matrix:smt,vm:numa` (default) labels those mixes `split=heldout`.

## Files

- `perturb.py`: attach to a running app under Mercury. Snapshots cpusets, freezes with
  `POST /scale policy none`, applies B with `POST /cpus` (or starts pinned stress-ng), A-B-A per
  window of the operator output, reverts, re-enables saturate. Restores and kills stress-ng on exit,
  SIGINT/SIGTERM/SIGHUP and exceptions; state in `--state` for `perturb.py restore`.
- `perturb.sh`: end-to-end run like `~/Mercury/src/test/dsb_ab.sh` (own Mercury 8080, pool 1-60,
  operator ramp, perturb.py). Refuses to start if any other dsb_ab/mercury/operator/loadgen/
  stress-ng process or app cgroup exists.
- `sweep.sh`: the matrix (daemons x {5 cpuset kinds, 5 stressors x 4 placements x 2 intensities}).
- `weak_labels.py`: read-only extraction; `labels/weak_labels.csv` + `.summary.json`.
- `test_perturb.sh`: offline test with a fake Mercury, fake cgroups and fake stress-ng.

## Commands

    cd ~/Benchmarks/perturb
    nice python3 weak_labels.py                       # read-only
    ./test_perturb.sh                                 # offline
    ./perturb.sh hotel labels/smoke "-users 64 -window-ms 2000" --daemon hotel-geo --kind smt --clients 64
    ./perturb.sh hotel labels/runs "-users 2048 -window-ms 3000" --daemon hotel-geo --kind stress \
        --stressor cache --placement smt --intensity high --clients 256,1024,2048
    ./sweep.sh hotel labels/sweep hotel-geo hotel-mongod

## Caveats

- Windows are timestamped on arrival (operator lines carry none); the plan switches at window
  boundaries, so each window is fully A or B. Each level gets 4 windows on the up-ramp: 1 warm-up
  is discarded, leaving A,B,A. Use longer `-window-ms` for less noise.
- Freezing (policy none) is itself a change from saturate; labels are "under frozen cpusets".
- Weak labels are confounded (policy picks the moment); `valid=1` keeps only clean single-daemon
  changes at constant users with a settled after-state. Hotel `.out` files have no cpu.stat, so
  `usage_cores_*` is empty there.

## Optional counters (`--counters cgroup|cpu`)

`counters.py` opens hardware counters with perf_event_open(2) directly (same kernel interface and
permissions as `perf stat`; the `perf` binary is **not installed** on dtn-01, so it is not used):
cycles, instructions, cache-misses and LLC read misses, either for the target daemon's cgroup
(`cgroup`, counted on every CPU in `--allowed`) or system-wide on the target's CPUs (`cpu`). It also
sums `/proc/<tid>/schedstat` run-delay over the daemon's threads (`cgroup.threads`).

Extra columns: `ipc_*`, `cache_mpki_*`, `llc_mpki_*` (misses per 1k instructions), `run_delay_ms_s_*`
(ms runqueue wait per second), with `_before` = A windows, `_after` = B windows, plus `counters`
(backend status). Raw cumulative counts go to the windows CSV.

Permissions: dtn-01 has `kernel.perf_event_paranoid = -1`, so cgroup/CPU-wide events work as paul.
If it is raised to 1 or higher, events fail and the columns stay empty, with the reason in
`counters`. To re-enable: `sudo sysctl kernel.perf_event_paranoid=-1` (or 0), or give python3 CAP_PERFMON.
Zen 1 (EPYC 7301) may not map the generic LLC event (ENOENT): `llc_mpki` is then empty and
`cache_mpki` (generic cache-misses) is the fallback. If you want `perf stat`, install `linux-perf`.

### AMD Zen 1 raw events (`--zen auto`, default; family 17h only)

Codes from the kernel pmu-events amdzen1 JSON / family 17h PPR, checked against the sysfs formats of
`cpu` (type 4), `amd_l3` (type 11, config event 0-7, umask 8-15, slicemask 48-51, threadmask 56-63,
cpumask = one CPU per CCX: 0,2,...,30) and `amd_df` (type 10, event 0-7+32-35, umask 8-15,
cpumask = one CPU per die: 0,4,...,28), and read live for 0.3 s on idle CPU 44 (2026-10-18).

| column | events | attribution |
|---|---|---|
| `l2_mpki` | 0x64/0x09 l2_cache_req_stat.ic_dc_miss_in_l2 (also what generic `cache-misses` maps to, so `cache_mpki` = L2 MPKI on Zen 1) | daemon (cgroup) |
| `stall_frac` | (0x87/0x02 + 0x87/0x01) / cycles: fetch stalls upstream + back-pressure (= generic stalled-cycles-frontend/backend) | daemon |
| raw only, `--zen full` | 0xAF/0x7F, 0xAE/0xFE dispatch token stalls 0/1 (8 core events > 6 counters: multiplexed) | daemon |
| `l3_miss_rate`, `l3_mpki` | amd_l3 0x01/0x80 accesses, 0x06/0x01 misses, on the L3 slices of the daemon's CPUs | **shared slice context** |
| `df_gbs` | amd_df dram_channel_data_controller_0..7 (umask 0x38) x 64 B on the daemon's NUMA node(s) | **shared node context** |
| `sib_ghz` | cycles (0x76) on the SMT siblings of the daemon's CPUs that are outside its cpuset | **SMT contention proxy** |

Shared context: the L3 PMU is per CCX and system-wide only, and the DF is per die, so these count
everything that runs on the slice/node (the daemon, its neighbours, stress-ng). They describe the
pressure around the daemon, not the daemon's own misses; `l3_mpki` divides slice misses by the
daemon's own instructions and is only meaningful as a relative signal. Zen 1 has no SMT-contention
event; `sib_ghz` (busy GHz of the siblings) is the proxy. The generic LLC event is not mapped on
Zen 1 (ENOENT), so `llc_mpki` stays empty. DF slot 4 reads 0 on dtn-01 and counts differ per die;
treat `df_gbs` as a relative bandwidth proxy, not calibrated GB/s. Counts are scaled per read interval by time_enabled/time_running, so multiplexing cannot make deltas negative. Probe: `python3 counters.py <cpu> 0.3`.

## Pinning A/B/C (`abc.sh`, `placer.py`, `ctrlog.py`, `pin_compare.py`)

Question: Kurt's GNN autoscaler did better without pinning. Is that true of pinning in general, or only
of static one-busy-thread-per-CPU pinning when there is no interference to react to?

- `placer.py` side-car (sudo, no Mercury changes): `none` (full cpuset mask), `static` (busy threads
  one per CPU, like `GnnController::pin()` without the model), `react-thread` (move a thread only when
  wait share >= 0.2 for 3 ticks AND it is stacked / its SMT sibling is busy / its L3 slice is hot;
  escalation same L3 -> node -> anywhere; hysteresis 0.2/0.05, 1 s cooldown, max 4 moves per 100 ms),
  `react-cgroup` (swap a daemon CPU whose L3/sibling is hot for a free CPU in a cool slice via
  `POST /cpus`, 10 s cooldown). Restores affinity and cpusets on exit. Log: placer.jsonl.
- `ctrlog.py`: per-second IPC / stall fraction / L2 MPKI per daemon (by cpuset), L3 miss rate per
  slice, DF GB/s per node.
- `abc.sh hotel|social OUT [REPS]`: modes x stress {off,on} at `-users 2048` (past the knee), then
  `pin_compare.py` summary. `POLICY=gnn` sizes with Kurt's GNN (`-pin=false`), and `MODES` may add
  `gnn-pin` for his built-in pinning.
- Tests: `test_placer.sh` (fake cgroups, /proc, sysfs, Mercury, affinity).

    ./abc.sh hotel labels/abc 2
    ./abc.sh social labels/abc 2
    POLICY=gnn MODES="gnn-pin none static react-thread react-cgroup" ./abc.sh hotel labels/abc-gnn 2
    python3 pin_compare.py ~/Mercury/data/dsb/social-pin/20261018-120642   # Kurt's run, read-only
