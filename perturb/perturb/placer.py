#!/usr/bin/env python3
"""Thread/cpuset placement side-car for Mercury daemons (no Mercury code changes).

Mercury (saturate, or gnn with -pin=false) decides HOW MANY CPUs each daemon has; this tool
decides WHERE threads run inside that cpuset, or shifts the cpuset away from hot L3 slices.
Modes:
  none          every thread gets the full cpuset mask (kernel load balancing), re-applied each tick
  static        busy threads (run share >= --busy) pinned one per CPU, round-robin by busyness,
                sticky; like GnnController::pin() but without the model (A/B control)
  react-thread  threads stay wide; a thread is moved only when interference is detected:
                runqueue wait share >= --wait-hi for --k ticks AND (another busy thread shares its
                CPU, or its SMT sibling is busy >= --sib-busy, or its L3 slice miss rate >= --l3-hot).
                Target = escalation: idle core in same L3 -> same NUMA node -> anywhere in the cpuset.
                Back to wide after wait share < --wait-lo for --k-lo ticks. Cooldown per thread, cap per tick.
  react-cgroup  every --cg-period s, for each daemon CPU whose L3 slice (or SMT sibling) is hot,
                swap it via POST /cpus for an unused CPU in the coolest slice; cooldown per daemon.
Signals: /proc/<tid>/schedstat (run, wait ns), /proc/<tid>/stat field 39 (last CPU), /proc/stat
per-CPU busy, amd_l3 per slice via counters.py when available (else L3 heat = 0 and only
wait/sibling/stacking triggers fire).

Needs root for sched_setaffinity on root-owned daemon threads: run with sudo -n.
On exit (SIGINT/SIGTERM/HUP, exceptions) every touched thread is widened back to its cpuset
and cpusets moved by react-cgroup are restored with POST /cpus. Log: one JSON line per tick.

  sudo -n python3 placer.py --mode react-thread --prefix hotel- --log placer.jsonl
"""
import argparse, atexit, json, os, signal, sys, time, urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import format_cpuset, parse_cpuset

CG = os.environ.get("PERTURB_CG", "/sys/fs/cgroup/mercury")
PROC = os.environ.get("PLACER_PROC", "/proc")
SYS = os.environ.get("PLACER_SYS", "/sys/devices/system")
AFF_LOG = os.environ.get("PLACER_FAKE_AFFINITY")  # test: record instead of sched_setaffinity


def rd(p, d=""):
    try:
        with open(p) as f:
            return f.read().strip()
    except OSError:
        return d


def set_aff(tid, cpus):
    if AFF_LOG:
        with open(AFF_LOG, "a") as f:
            f.write(f"{tid} {format_cpuset(cpus)}\n")
        return True
    try:
        os.sched_setaffinity(int(tid), cpus)
        return True
    except OSError:
        return False  # thread exited or not permitted


def post(base, path, payload):
    req = urllib.request.Request(base + path, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=10) as r:
        body = r.read().decode()
    if '"ok":true' not in body:
        raise RuntimeError(f"POST {path}: {body}")


def sibling(c):
    return [x for x in parse_cpuset(rd(f"{SYS}/cpu/cpu{c}/topology/thread_siblings_list")) if x != c]


def l3_of(c):
    return tuple(parse_cpuset(rd(f"{SYS}/cpu/cpu{c}/cache/index3/shared_cpu_list")) or [c])


def node_of(c):
    for n in sorted(os.listdir(f"{SYS}/node")) if os.path.isdir(f"{SYS}/node") else []:
        if n.startswith("node") and c in parse_cpuset(rd(f"{SYS}/node/{n}/cpulist")):
            return int(n[4:])
    return 0


class CpuBusy:
    def __init__(self):
        self.last = {}

    def sample(self):
        out = {}
        for line in rd(f"{PROC}/stat").splitlines():
            if line.startswith("cpu") and line[3:4].isdigit():
                f = line.split()
                c = int(f[0][3:])
                v = list(map(int, f[1:9]))
                idle = v[3] + v[4]
                tot = sum(v)
                if c in self.last:
                    dt, di = tot - self.last[c][0], idle - self.last[c][1]
                    out[c] = 1 - di / dt if dt > 0 else 0.0
                self.last[c] = (tot, idle)
        return out


class L3Heat:
    """Miss rate per L3 slice from amd_l3 (shared context); empty when unavailable."""

    def __init__(self, cpus):
        self.fds, self.last = {}, {}
        try:
            import counters as C
            t = C.pmu_type("amd_l3")
            if t is None or os.environ.get("PERTURB_COUNTERS") == "fake":
                return
            for rep in sorted({l3_of(c)[0] for c in cpus}):
                self.fds[rep] = (C._open(t, C.ZEN1_L3["l3_access"], -1, rep, 0), C._open(t, C.ZEN1_L3["l3_miss"], -1, rep, 0))
            self.C = C
        except Exception:
            self.fds = {}

    def sample(self):
        out = {}
        for rep, (fa, fm) in self.fds.items():
            a, m = self.C._raw(fa)[0], self.C._raw(fm)[0]
            if rep in self.last:
                da, dm = a - self.last[rep][0], m - self.last[rep][1]
                out[rep] = dm / da if da > 0 else 0.0
            self.last[rep] = (a, m)
        return out


def threads(d):
    return rd(f"{CG}/{d}/cgroup.threads").split()


def tstat(tid):
    s = rd(f"{PROC}/{tid}/schedstat").split()
    st = rd(f"{PROC}/{tid}/stat")
    try:
        last = int(st.rsplit(")", 1)[1].split()[36]) if st else -1  # field 39
        return int(s[0]), int(s[1]), last
    except (IndexError, ValueError):
        return None


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", required=True, choices=["none", "static", "react-thread", "react-cgroup"])
    ap.add_argument("--prefix", default="hotel-")
    ap.add_argument("--exclude", default="loadgen")
    ap.add_argument("--mercury", default="http://127.0.0.1:8080")
    ap.add_argument("--allowed", default="1-60", help="Mercury's daemon pool (react-cgroup swap targets)")
    ap.add_argument("--tick-ms", type=int, default=100)
    ap.add_argument("--busy", type=float, default=0.3, help="run share that makes a thread busy")
    ap.add_argument("--wait-hi", type=float, default=0.2)
    ap.add_argument("--wait-lo", type=float, default=0.05)
    ap.add_argument("--k", type=int, default=3, help="ticks above wait-hi before a move")
    ap.add_argument("--k-lo", type=int, default=20, help="ticks below wait-lo before unpinning")
    ap.add_argument("--sib-busy", type=float, default=0.5)
    ap.add_argument("--l3-hot", type=float, default=0.3, help="L3 slice miss rate counted as hot")
    ap.add_argument("--cooldown-s", type=float, default=1.0)
    ap.add_argument("--max-moves", type=int, default=4, help="per tick, all daemons")
    ap.add_argument("--cg-period", type=float, default=2.0)
    ap.add_argument("--cg-cooldown-s", type=float, default=10.0)
    ap.add_argument("--duration-s", type=float, default=0, help="0 = until signalled")
    ap.add_argument("--log", default="placer.jsonl")
    a = ap.parse_args()

    pool = parse_cpuset(a.allowed)
    busy_cpu, heat = CpuBusy(), L3Heat(pool)
    prev, touched, cg_orig = {}, {}, {}
    hi_cnt, lo_cnt, pinned, cool, cg_cool = {}, {}, {}, {}, {}
    log = open(a.log, "a")
    done = []

    def cleanup(*_):
        if done:
            return
        done.append(1)
        for d, spec in cg_orig.items():
            try:
                post(a.mercury, "/cpus", {"daemon_id": d, "cpus": spec})
            except Exception as e:
                print("restore cpuset failed", d, e, file=sys.stderr)
        for tid, d in touched.items():
            cs = parse_cpuset(rd(f"{CG}/{d}/cpuset.cpus")) or pool
            set_aff(tid, cs)
        log.write(json.dumps({"t": time.time(), "event": "restored", "threads": len(touched), "cpusets": len(cg_orig)}) + "\n")
        log.close()

    def on_sig(sig, _):
        cleanup()
        sys.exit(128 + sig)

    atexit.register(cleanup)
    for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(s, on_sig)

    t_end = time.time() + a.duration_s if a.duration_s else None
    last_cg = 0.0
    while t_end is None or time.time() < t_end:
        t0 = time.time()
        daemons = sorted(x for x in (os.listdir(CG) if os.path.isdir(CG) else []) if x.startswith(a.prefix) and a.exclude not in x)
        cb, hot = busy_cpu.sample(), heat.sample()
        moves, info = [], {}
        for d in daemons:
            cs = parse_cpuset(rd(f"{CG}/{d}/cpuset.cpus"))
            if not cs:
                continue
            ts = []
            for tid in threads(d):
                s = tstat(tid)
                if not s:
                    continue
                if tid in prev:
                    dt = max(t0 - prev[tid][3], 1e-3) * 1e9
                    run, wait = (s[0] - prev[tid][0]) / dt, (s[1] - prev[tid][1]) / dt
                    ts.append((tid, run, wait, s[2]))
                prev[tid] = (s[0], s[1], s[2], t0)
            busy = sorted([x for x in ts if x[1] >= a.busy], key=lambda x: -x[1])
            info[d] = {"cpus": format_cpuset(cs), "threads": len(ts), "busy": len(busy),
                       "wait": round(sum(x[2] for x in ts), 3)}
            if a.mode == "none":
                for tid, *_ in ts:
                    if tid not in touched or tid in pinned:
                        set_aff(tid, cs)
                        touched[tid] = d
                        pinned.pop(tid, None)
            elif a.mode == "static":
                used = {pinned[t] for t, *_ in busy if t in pinned and pinned[t] in cs}
                free = [c for c in cs if c not in used]
                for i, (tid, *_r) in enumerate(busy):
                    if tid in pinned and pinned[tid] in cs:
                        continue
                    c = free.pop(0) if free else cs[i % len(cs)]
                    if set_aff(tid, [c]):
                        pinned[tid], touched[tid] = c, d
                        moves.append((d, tid, c, "static"))
                for tid, run, *_ in ts:  # no longer busy -> wide
                    if tid in pinned and run < a.busy / 2:
                        set_aff(tid, cs)
                        pinned.pop(tid)
            elif a.mode == "react-thread":
                on_cpu = {}
                for tid, run, wait, last in busy:
                    on_cpu.setdefault(last, []).append(tid)
                for tid, run, wait, last in ts:
                    if wait >= a.wait_hi:
                        hi_cnt[tid] = hi_cnt.get(tid, 0) + 1
                        lo_cnt[tid] = 0
                    elif wait < a.wait_lo:
                        lo_cnt[tid] = lo_cnt.get(tid, 0) + 1
                        hi_cnt[tid] = 0
                    if tid in pinned and (lo_cnt.get(tid, 0) >= a.k_lo or pinned[tid] not in cs):
                        set_aff(tid, cs)
                        pinned.pop(tid)
                        moves.append((d, tid, format_cpuset(cs), "unpin"))
                        continue
                    if hi_cnt.get(tid, 0) < a.k or cool.get(tid, 0) > t0 or len(moves) >= a.max_moves:
                        continue
                    stacked = len(on_cpu.get(last, [])) >= 2
                    sib = max([cb.get(s, 0) for s in sibling(last)] or [0]) >= a.sib_busy
                    l3hot = hot.get(l3_of(last)[0], 0) >= a.l3_hot if last >= 0 else False
                    if not (stacked or sib or l3hot):
                        continue  # waiting without interference: a sizing problem, leave it to Mercury
                    claimed = {pinned[t] for t in pinned} | {m[2] for m in moves if isinstance(m[2], int)}
                    cands = [c for c in cs if c != last and c not in claimed]

                    def score(c):
                        step = 0 if l3_of(c) == l3_of(last) else 1 if node_of(c) == node_of(last) else 2
                        sibb = max([cb.get(s, 0) for s in sibling(c)] or [0])
                        return (step + 3 * cb.get(c, 0) + 2 * sibb + 2 * hot.get(l3_of(c)[0], 0), c)
                    if not cands:
                        continue
                    tgt = min(cands, key=score)
                    if score(tgt)[0] >= 3 * cb.get(last, 1.0) + 1:  # not clearly better: stay
                        continue
                    if set_aff(tid, [tgt]):
                        pinned[tid], touched[tid] = tgt, d
                        cool[tid] = t0 + a.cooldown_s
                        hi_cnt[tid] = 0
                        moves.append((d, tid, tgt, "stacked" if stacked else "sibling" if sib else "l3"))
            elif a.mode == "react-cgroup" and t0 - last_cg >= a.cg_period and cg_cool.get(d, 0) <= t0:
                allc = {c for x in daemons for c in parse_cpuset(rd(f"{CG}/{x}/cpuset.cpus"))}
                gen = {c for x in os.listdir(CG) if a.exclude in x for c in parse_cpuset(rd(f"{CG}/{x}/cpuset.cpus"))}
                free = [c for c in pool if c not in allc and c not in gen]

                def heat_of(c):
                    return hot.get(l3_of(c)[0], 0) + max([cb.get(s, 0) for s in sibling(c)] or [0])
                worst = max(cs, key=heat_of)
                if free and heat_of(worst) >= a.l3_hot + a.sib_busy * 0.5:
                    best = min(free, key=heat_of)
                    if heat_of(best) < heat_of(worst) * 0.5:
                        new = sorted([c for c in cs if c != worst] + [best])
                        try:
                            cg_orig.setdefault(d, format_cpuset(cs))
                            post(a.mercury, "/cpus", {"daemon_id": d, "cpus": format_cpuset(new)})
                            cg_cool[d] = t0 + a.cg_cooldown_s
                            moves.append((d, "cgroup", f"{worst}->{best}", "hot"))
                        except Exception as e:
                            moves.append((d, "cgroup", str(e), "error"))
        if a.mode == "react-cgroup" and t0 - last_cg >= a.cg_period:
            last_cg = t0
        live = {t for d in daemons for t in threads(d)}
        for tid in list(prev):
            if tid not in live:
                for m in (prev, hi_cnt, lo_cnt, pinned, cool, touched):
                    m.pop(tid, None)
        log.write(json.dumps({"t": round(t0, 3), "mode": a.mode, "moves": moves, "pinned": len(pinned),
                              "l3_hot": {k: round(v, 3) for k, v in hot.items()}, "daemons": info}) + "\n")
        log.flush()
        time.sleep(max(0.0, a.tick_ms / 1000 - (time.time() - t0)))
    cleanup()


if __name__ == "__main__":
    main()
