#!/usr/bin/env python3
"""Read-only comparison of policy runs (e.g. gnn vs gnn-nopin vs saturate) from dsb_ab.sh run dirs.

  pin_compare.py RUN_DIR [RUN_DIR ...]      (each RUN_DIR holds <rep>-<policy>/operate.out)

Per policy and user level: total throughput (sum of route service_rate, mean over windows),
mean of per-route p95/p99, cores held by service daemons, and from sample.jsonl the mean
busy_threads / collision_cpus / collision_threads / stacked_cpus; from mercury.log the gnn
stacking-breaker firings. Prints a table and a summary over levels.
"""
import glob, json, os, re, sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import count_cpus, group_windows, mean, parse_line


def run_stats(d):
    rows = [r for r in (parse_line(l) for l in open(os.path.join(d, "operate.out"), errors="replace")) if r]
    lv = defaultdict(lambda: {"tput": [], "p95": [], "p99": [], "cores": [], "bound": 0, "n": 0})
    for w in group_windows(rows):
        s = lv[w["clients"]]
        s["tput"].append(sum(r["rate"] for r in w["routes"].values()))
        s["p95"].append(mean([r["p95"] for r in w["routes"].values()]))
        s["p99"].append(mean([r["p99"] for r in w["routes"].values()]))
        s["cores"].append(sum(count_cpus(v) for k, v in w["cpus"].items() if "loadgen" not in k))
        s["bound"] += w["gen_bound"] == "1"
        s["n"] += 1
    samp = defaultdict(list)
    p = os.path.join(d, "sample.jsonl")
    if os.path.exists(p):
        for line in open(p):
            try:
                j = json.loads(line)
            except ValueError:
                continue
            for k in ("busy_threads", "collision_cpus", "collision_threads", "stacked_cpus"):
                if k in j:
                    samp[k].append(j[k])
    breaks = 0
    p = os.path.join(d, "mercury.log")
    if os.path.exists(p):
        for line in open(p, errors="replace"):
            m = re.search(r" breaks=(\d+)", line)
            if m:
                breaks += int(m.group(1))
    return lv, {k: mean(v) for k, v in samp.items()}, breaks


def main():
    pol = defaultdict(list)
    for rd in sys.argv[1:]:
        for d in sorted(glob.glob(os.path.join(rd, "*-*"))):
            if os.path.exists(os.path.join(d, "operate.out")):
                pol[os.path.basename(d).split("-", 1)[1]].append(run_stats(d))
    levels = sorted({u for runs in pol.values() for lv, _, _ in runs for u in lv})
    print("policy".ljust(28), "users", "tput", "p95_us", "p99_us", "cores", "gen_bound", sep="\t")
    summ = {}
    for name, runs in sorted(pol.items()):
        allt, allp95, allp99, allc = [], [], [], []
        for u in levels:
            vals = [lv[u] for lv, _, _ in runs if u in lv]
            if not vals:
                continue
            t = mean([mean(v["tput"]) for v in vals]); p95 = mean([mean(v["p95"]) for v in vals])
            p99 = mean([mean(v["p99"]) for v in vals]); c = mean([mean(v["cores"]) for v in vals])
            b = sum(v["bound"] for v in vals)
            allt.append(t); allp95.append(p95); allp99.append(p99); allc.append(c)
            print(name.ljust(28), u, f"{t:.0f}", f"{p95:.0f}", f"{p99:.0f}", f"{c:.1f}", b, sep="\t")
        sm = defaultdict(list)
        for _, s, _ in runs:
            for k, v in s.items():
                if v is not None:
                    sm[k].append(v)
        summ[name] = {"reps": len(runs), "mean_tput": round(mean(allt) or 0), "max_tput": round(max(allt or [0])),
                      "mean_p95_us": round(mean(allp95) or 0), "mean_p99_us": round(mean(allp99) or 0),
                      "mean_cores": round(mean(allc) or 0, 1), "breaks": sum(b for _, _, b in runs),
                      **{k: round(mean(v), 2) for k, v in sm.items()}}
    print()
    for k, v in summ.items():
        print(k, json.dumps(v))


if __name__ == "__main__":
    main()
