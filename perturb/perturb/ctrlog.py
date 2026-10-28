#!/usr/bin/env python3
"""Per-second counter log for A/B/C runs: per-CPU core events, per-L3-slice and per-node DF,
attributed to daemons by their current cpuset (CPU-scope, so neighbours on a daemon's CPUs count too).

  python3 ctrlog.py --prefix hotel- --cpus 1-60 --out counters.jsonl [--duration-s N]
Graceful: missing PMUs/events are skipped and listed in the first line ("status").
"""
import argparse, json, os, signal, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import counters as C
from common import parse_cpuset

CG = os.environ.get("PERTURB_CG", "/sys/fs/cgroup/mercury")
CORE = {"cycles": (0, 0), "instructions": (0, 1)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", default="hotel-")
    ap.add_argument("--cpus", default="1-60")
    ap.add_argument("--out", default="counters.jsonl")
    ap.add_argument("--period-s", type=float, default=1.0)
    ap.add_argument("--duration-s", type=float, default=0)
    a = ap.parse_args()
    cpus = parse_cpuset(a.cpus)
    fake = os.environ.get("PERTURB_COUNTERS") == "fake"
    specs, errs = {}, {}
    core = C.pmu_type("cpu")
    ev = dict(CORE)
    if core is not None and C.cpu_family() == 0x17:
        ev.update({k: (core, C.ZEN1_CORE[k]) for k in ("l2_miss", "stall_fe", "stall_be")})
    else:  # Intel etc.: generic cache-misses = LLC misses on Intel
        ev["cache_misses"] = (0, 3)
    sib_of = {c: [x for x in C._siblings(c) if x != c] for c in cpus}
    for c in cpus:
        for k, (t, cfg) in ev.items():
            specs[("cpu", c, k)] = (t, cfg, c)
    l3, df = C.pmu_type("amd_l3"), C.pmu_type("amd_df")
    if l3 is not None:
        for rep in sorted({C.l3_rep(c) for c in cpus} - {None}):
            for k, cfg in C.ZEN1_L3.items():
                specs[("l3", rep, k)] = (l3, cfg, rep)
    if df is not None:
        for rep in sorted({C.df_rep(c) for c in cpus} - {None}):
            for k, cfg in C.ZEN1_DF.items():
                specs[("df", rep, k)] = (df, cfg, rep)
    fds = {}
    if not fake:
        for key, (t, cfg, c) in specs.items():
            try:
                fds[key] = C._open(t, cfg, -1, c, 0)
            except OSError as e:
                errs[key[2]] = e.strerror
    out = open(a.out, "a")
    out.write(json.dumps({"status": "fake" if fake else f"{len(fds)}/{len(specs)} counters", "missing": errs,
                          "paranoid": C.paranoid()}) + "\n")
    stop = []
    signal.signal(signal.SIGTERM, lambda *_: stop.append(1))
    signal.signal(signal.SIGINT, lambda *_: stop.append(1))
    last, n = {}, 0
    t_end = time.time() + a.duration_s if a.duration_s else None
    while not stop and (t_end is None or time.time() < t_end):
        time.sleep(a.period_s)
        n += 1
        cur = {}
        for key in specs:
            if fake:
                cur[key] = (1e8 * n * (2 if key[2] == "instructions" else 1), n, n)
            elif key in fds:
                cur[key] = C._raw(fds[key])
        delta = {}
        for key, (v, en, run) in cur.items():
            v0, en0, run0 = last.get(key, (0, 0, 0))
            if key in last and run > run0:
                delta[key] = (v - v0) * (en - en0) / (run - run0)
        last = cur
        if not delta:
            continue
        rec = {"t": round(time.time(), 3), "daemons": {}, "l3": {}, "df_gbs": {}}
        for d in sorted(os.listdir(CG)) if os.path.isdir(CG) else []:
            if not d.startswith(a.prefix):
                continue
            cs = parse_cpuset(open(f"{CG}/{d}/cpuset.cpus").read() if os.path.exists(f"{CG}/{d}/cpuset.cpus") else "")
            agg = {k: sum(delta.get(("cpu", c, k), 0) for c in cs) for k in ev}
            r = {"cpus": len(cs)}
            if agg.get("cycles"):
                r["ipc"] = round(agg["instructions"] / agg["cycles"], 3)
                if "stall_fe" in agg:
                    r["stall_frac"] = round((agg["stall_fe"] + agg["stall_be"]) / agg["cycles"], 3)
            sibs = {x for c in cs for x in sib_of.get(c, []) if x not in cs}
            if sibs:
                r["sib_ghz"] = round(sum(delta.get(("cpu", x, "cycles"), 0) for x in sibs) / a.period_s / 1e9, 3)
            if agg.get("instructions") and "cache_misses" in agg:
                r["llc_mpki"] = round(1000 * agg["cache_misses"] / agg["instructions"], 2)
            if agg.get("instructions") and "l2_miss" in agg:
                r["l2_mpki"] = round(1000 * agg["l2_miss"] / agg["instructions"], 2)
            rec["daemons"][d] = r
        for (kind, rep, k), v in delta.items():
            if kind == "l3":
                rec["l3"].setdefault(str(rep), {})[k] = v
            elif kind == "df":
                rec["df_gbs"][str(rep)] = rec["df_gbs"].get(str(rep), 0) + v * 64 / a.period_s / 1e9
        rec["l3"] = {k: round(v.get("l3_miss", 0) / v["l3_access"], 4) for k, v in rec["l3"].items() if v.get("l3_access")}
        rec["df_gbs"] = {k: round(v, 3) for k, v in rec["df_gbs"].items()}
        out.write(json.dumps(rec) + "\n")
        out.flush()
    out.close()


if __name__ == "__main__":
    main()
