"""Shared parsing and CSV schema for perturbation and weak labels.

Operator lines (hotel operate/, social goperate/):
  route R clients N arrival_rate A service_rate S avg_us X p95_us P [p99_us Q] errors E cpus d=SPEC ... [gen_cpus G gen_busy_ms B gen_resized 0|1 gen_bound 0|1]
"""
import re

LINE = re.compile(
    r"^route (?P<route>\S+) clients (?P<clients>\d+) arrival_rate (?P<arr>\d+) service_rate (?P<svc>\d+) "
    r"avg_us (?P<avg>\S+) p95_us (?P<p95>\S+)(?: p99_us (?P<p99>\S+))? errors (?P<err>\d+) cpus (?P<rest>.*)$")

# One row per (event, route). Perturbation and weak labels share this schema.
FIELDS = [
    "source", "run", "app", "policy", "event_id", "kind", "daemon", "partner",
    "cpus_before", "cpus_after", "cores_before", "cores_after", "clients", "route",
    "rate_before", "rate_after", "p95_before", "p95_after", "p99_before", "p99_after",
    "errors_before", "errors_after", "d_rate_pct", "d_p95_pct", "d_p99_pct",
    "usage_cores_before", "usage_cores_after", "windows_before", "windows_after", "valid", "note",
]
# Interference labels (empty for plain perturbations and weak labels).
STRESS_FIELDS = ["stressor", "stress_placement", "intensity", "stress_cpus", "stress_workers", "split"]
# Optional counters (perturb.py --counters cgroup|cpu): target daemon (cgroup scope) or its CPUs (cpu scope).
COUNTER_FIELDS = [f"{m}_{ph}" for m in ("ipc", "cache_mpki", "llc_mpki", "run_delay_ms_s", "l2_mpki", "stall_frac", "l3_miss_rate", "l3_mpki", "df_gbs", "sib_ghz") for ph in ("before", "after")] + ["counters"]


def parse_line(line):
    m = LINE.match(line.strip())
    if not m:
        return None
    rest = m.group("rest").split()
    cpus, gen, i = {}, {}, 0
    while i < len(rest):
        tok = rest[i]
        if tok.startswith("gen_") and i + 1 < len(rest):
            gen[tok] = rest[i + 1]
            i += 2
            continue
        if "=" in tok:
            k, v = tok.split("=", 1)
            cpus[k] = v
        i += 1
    p99 = m.group("p99")
    return {
        "route": m.group("route"), "clients": int(m.group("clients")),
        "rate": float(m.group("svc")), "arrival": float(m.group("arr")),
        "avg": float(m.group("avg")), "p95": float(m.group("p95")),
        "p99": float(p99) if p99 is not None else None, "errors": int(m.group("err")),
        "cpus": cpus, "gen": gen,
    }


def group_windows(rows):
    """Split consecutive route lines into windows: a window ends when a route repeats."""
    windows, cur = [], {}
    for r in rows:
        if r["route"] in cur:
            windows.append(cur)
            cur = {}
        cur[r["route"]] = r
    if cur:
        windows.append(cur)
    out = []
    for w in windows:
        any_r = next(iter(w.values()))
        out.append({
            "routes": w, "clients": any_r["clients"], "cpus": any_r["cpus"],
            "gen_resized": any_r["gen"].get("gen_resized", "0"),
            "gen_bound": any_r["gen"].get("gen_bound", "0"),
        })
    return out


def count_cpus(spec):
    if not spec or spec == "?":
        return 0
    n = 0
    for part in spec.split(","):
        if "-" in part:
            a, b = part.split("-", 1)
            n += int(b) - int(a) + 1
        else:
            n += 1
    return n


def parse_cpuset(spec):
    out = []
    for part in (spec or "").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return sorted(set(out))


def format_cpuset(cpus):
    cpus = sorted(set(cpus))
    if not cpus:
        return ""
    parts, start, prev = [], cpus[0], cpus[0]
    for c in cpus[1:]:
        if c == prev + 1:
            prev = c
            continue
        parts.append(f"{start}-{prev}" if start != prev else f"{start}")
        start = prev = c
    parts.append(f"{start}-{prev}" if start != prev else f"{start}")
    return ",".join(parts)


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def pct(before, after):
    if before is None or after is None or before == 0:
        return None
    return 100.0 * (after - before) / before


def route_rows(base, before_ws, after_ws):
    """One CSV row per route: means over before/after windows, deltas in percent."""
    rows = []
    routes = sorted(set().union(*[w["routes"].keys() for w in before_ws + after_ws]))
    for route in routes:
        b = [w["routes"][route] for w in before_ws if route in w["routes"]]
        a = [w["routes"][route] for w in after_ws if route in w["routes"]]
        if not b or not a:
            continue
        rb, ra = mean([x["rate"] for x in b]), mean([x["rate"] for x in a])
        pb, pa = mean([x["p95"] for x in b]), mean([x["p95"] for x in a])
        qb, qa = mean([x["p99"] for x in b]), mean([x["p99"] for x in a])
        eb, ea = sum(x["errors"] for x in b), sum(x["errors"] for x in a)
        row = dict(base)
        row.update({
            "clients": b[0]["clients"], "route": route,
            "rate_before": fmt(rb), "rate_after": fmt(ra), "p95_before": fmt(pb), "p95_after": fmt(pa),
            "p99_before": fmt(qb), "p99_after": fmt(qa), "errors_before": eb, "errors_after": ea,
            "d_rate_pct": fmt(pct(rb, ra)), "d_p95_pct": fmt(pct(pb, pa)), "d_p99_pct": fmt(pct(qb, qa)),
            "windows_before": len(b), "windows_after": len(a),
        })
        rows.append(row)
    return rows


def fmt(x):
    return "" if x is None else f"{x:.3f}"
