#!/usr/bin/env python3
"""Extract natural experiments (policy-driven cpuset changes) from operator logs.

Read-only. Inputs: hotel output/autoscale*.out and Mercury DSB A/B runs
(~/Mercury/data/dsb/<app>/<ts>/<rep>-<policy>/operate.out). Output: CSV in the
common.FIELDS schema plus a JSON summary.

An event is a window boundary where one daemon's cpuset differs between window
i-1 and window i. The change happened during window i, so i is skipped:
before = windows i-1 (and i-2 if identical state), after = i+1 (and i+2).
valid=1 only if the user count is the same across those windows, nothing else
changed in them, exactly one daemon changed at this boundary, and the load
generator did not resize or saturate (gen_resized=0, gen_bound=0 when present).

  weak_labels.py --out labels/weak_labels.csv [--hotel-out DIR] [--dsb DIR] [--skip-recent-s 120]
"""
import argparse, csv, glob, json, os, sys, time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import FIELDS, STRESS_FIELDS, COUNTER_FIELDS, count_cpus, group_windows, parse_line, route_rows

HOME = os.path.expanduser("~")


def windows_of(path):
    rows = []
    with open(path, errors="replace") as f:
        for line in f:
            r = parse_line(line)
            if r:
                rows.append(r)
    return group_windows(rows)


def policy_of(path):
    with open(path, errors="replace") as f:
        for line in f:
            if line.startswith("policy ") and "users" in line:
                return line.split()[1]
    return "saturate"


def app_of(ws):
    for w in ws:
        for d in w["cpus"]:
            if d in ("geo", "search", "frontend", "mongod"):
                return "hotel"
            if d.endswith("Service"):
                return "social"
    return "?"


def same_state(a, b):
    return a["clients"] == b["clients"] and a["cpus"] == b["cpus"]


def gen_ok(ws):
    return all(w["gen_resized"] == "0" and w["gen_bound"] == "0" for w in ws)


def events(path, source, run, skip_recent):
    if skip_recent and time.time() - os.path.getmtime(path) < skip_recent:
        return [], "skipped (still being written)"
    ws = windows_of(path)
    app, policy = app_of(ws), policy_of(path)
    out = []
    for i in range(1, len(ws) - 1):
        prev, cur, nxt = ws[i - 1], ws[i], ws[i + 1]
        changed = [d for d in cur["cpus"] if prev["cpus"].get(d) != cur["cpus"].get(d)]
        if not changed:
            continue
        before = [prev]
        if i >= 2 and same_state(ws[i - 2], prev):
            before.insert(0, ws[i - 2])
        after = [nxt]
        if i + 2 < len(ws) and same_state(ws[i + 2], nxt):
            after.append(ws[i + 2])
        notes = []
        if not (prev["clients"] == cur["clients"] == nxt["clients"]):
            notes.append("clients-changed")
        if cur["cpus"] != nxt["cpus"]:
            notes.append("after-not-settled")
        if len(changed) > 1:
            notes.append("multi:" + "+".join(changed))
        if not gen_ok(before + after + [cur]):
            notes.append("gen-resized-or-bound")
        for d in changed:
            cb, ca = prev["cpus"].get(d, ""), nxt["cpus"].get(d, "")
            nb, na = count_cpus(cb), count_cpus(ca)
            kind = "add" if na > nb else "remove" if na < nb else "move"
            base = {
                "source": source, "run": run, "app": app, "policy": policy,
                "event_id": f"{run}:w{i}:{d}", "kind": kind, "daemon": d, "partner": "",
                "cpus_before": cb, "cpus_after": ca, "cores_before": nb, "cores_after": na,
                "usage_cores_before": "", "usage_cores_after": "",
                "valid": int(not notes), "note": ";".join(notes), "split": "train",
            }
            out.extend(route_rows(base, before, after))
    return out, f"{len(ws)} windows"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "labels", "weak_labels.csv"))
    ap.add_argument("--hotel-out", default=f"{HOME}/Benchmarks/deathstar-hotelReservations/output")
    ap.add_argument("--dsb", default=f"{HOME}/Mercury/data/dsb")
    ap.add_argument("--skip-recent-s", type=float, default=120, help="skip logs modified this recently (a run in progress)")
    args = ap.parse_args()

    inputs = [(p, "weak:hotel-autoscale", os.path.basename(p)) for p in sorted(glob.glob(os.path.join(args.hotel_out, "autoscale*.out")))]
    for p in sorted(glob.glob(os.path.join(args.dsb, "*", "*", "*", "operate.out"))):
        rel = os.path.relpath(os.path.dirname(p), args.dsb)  # app/ts/rep-policy
        inputs.append((p, "weak:dsb-ab", rel))

    rows, per_input = [], {}
    for path, source, run in inputs:
        r, status = events(path, source, run, args.skip_recent_s)
        rows.extend(r)
        per_input[run] = {"status": status, "events": len({x["event_id"] for x in r}), "rows": len(r)}

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS + STRESS_FIELDS + COUNTER_FIELDS, restval="")
        w.writeheader()
        w.writerows(rows)

    ev = {}
    for r in rows:
        ev.setdefault(r["event_id"], r)
    summary = {
        "rows": len(rows), "events": len(ev),
        "valid_events": sum(1 for e in ev.values() if e["valid"] == 1),
        "by_kind": dict(Counter(e["kind"] for e in ev.values())),
        "valid_by_kind": dict(Counter(e["kind"] for e in ev.values() if e["valid"] == 1)),
        "by_app_policy": dict(Counter(f'{e["app"]}/{e["policy"]}' for e in ev.values())),
        "valid_by_daemon": dict(Counter(e["daemon"] for e in ev.values() if e["valid"] == 1).most_common()),
        "notes": dict(Counter(n for e in ev.values() for n in (e["note"].split(";") if e["note"] else ["ok"]) if not n.startswith("multi:")) | Counter("multi" for e in ev.values() if "multi:" in e["note"])),
        "inputs": per_input,
    }
    with open(os.path.splitext(args.out)[0] + ".summary.json", "w") as f:
        json.dump(summary, f, indent=1)
    print(json.dumps({k: v for k, v in summary.items() if k != "inputs"}, indent=1))
    print(f"wrote {args.out} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
