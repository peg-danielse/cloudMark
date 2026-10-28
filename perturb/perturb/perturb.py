#!/usr/bin/env python3
"""Apply one CPU perturbation to one Mercury daemon, hold it, revert, record deltas.

Attach mode: a DSB app (hotel or social) is already running under Mercury and an
operator (operate / goperate autoscale) is writing route lines to --watch.
This tool:
  1. snapshots every app daemon's cpuset (/sys/fs/cgroup/mercury/<id>/cpuset.cpus)
     and writes a state file for `perturb.py restore`;
  2. freezes cpusets: POST /scale policy "none" for each app daemon (the load
     generator keeps its own policy);
  3. waits for --clients (default: the next window at any level), then runs the
     window plan (default A,B,B,A): A = original cpuset, B = perturbed cpuset,
     applied with POST /cpus exactly like Mercury's own clients do;
  4. reverts, re-enables --restore-policy (saturate min/max by default) and
     appends one row per route to --csv (common.FIELDS schema) plus raw windows to
     --raw. Restoration also runs on SIGINT/SIGTERM/exceptions.

Kinds (target daemon d, partner p = busiest other daemon unless --partner):
  remove   drop d's last CPU (needs >= 2 CPUs)
  smt      replace d's last CPU with the SMT sibling of one of p's CPUs
  l3       replace d's last CPU with a CPU in p's L3 group that d does not use
  numa     move d's last CPU to an idle CPU on another NUMA node
  migrate  move d's whole cpuset to idle CPUs in another L3 group (same size)
  socket   move d's whole cpuset to idle CPUs on the other socket (same size; cross-socket)
  stress   d's cpuset unchanged; during B, stress-ng workers run pinned (taskset)
           next to d: --stressor cpu|cache|stream|vm|matrix, --placement
           core (d's own CPUs) | smt (their siblings) | l3 (rest of d's L3) |
           numa (rest of d's NUMA node), --intensity low|mid|high
           (1 worker / half the placement CPUs / all of them; cpu: load 50/80/100%).
           stress-ng is started in its own process group and killed on every exit path.

Load levels: --clients 256,1024,2048 runs the plan once per level on the operator's
up-ramp (first window of each level is discarded as warm-up). Rows carry
stressor/placement/intensity and split=train|heldout (--heldout mixes, e.g.
"stream:l3,matrix:smt"; plain perturbations are always train).
"""
import argparse, atexit, csv, json, os, signal, subprocess, sys, time, urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import FIELDS, STRESS_FIELDS, COUNTER_FIELDS
from counters import ALL_EVENTS as RAW_EVENTS, Counters, derived, run_delay_ns
from common import format_cpuset, parse_cpuset, parse_line, route_rows

CG = os.environ.get("PERTURB_CG", "/sys/fs/cgroup/mercury")  # override for tests
SYS = "/sys/devices/system/cpu"


def rd(path, default=""):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return default


def post(base, path, payload):
    req = urllib.request.Request(base + path, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body = r.read().decode()
            code = r.status
    except urllib.error.HTTPError as e:
        body, code = e.read().decode(), e.code
    if code != 200 or '"ok":true' not in body:
        raise RuntimeError(f"POST {path} {payload}: {code} {body.strip()}")


def set_cpus(base, d, cpus):
    post(base, "/cpus", {"daemon_id": d, "cpus": cpus if isinstance(cpus, str) else format_cpuset(cpus)})


def set_policy(base, d, policy, lo, hi, placement):
    post(base, "/scale", {"daemon_id": d, "policy": policy, "min": lo, "max": hi, "placement": placement})


def usage_usec(d):
    for line in rd(f"{CG}/{d}/cpu.stat").splitlines():
        if line.startswith("usage_usec"):
            return int(line.split()[1])
    return None


def sibling(c):
    return [x for x in parse_cpuset(rd(f"{SYS}/cpu{c}/topology/thread_siblings_list")) if x != c]


def l3(c):
    return parse_cpuset(rd(f"{SYS}/cpu{c}/cache/index3/shared_cpu_list"))


def socket_of(c):
    return int(rd(f"{SYS}/cpu{c}/topology/physical_package_id"))


def node_of(c):
    for n in os.listdir("/sys/devices/system/node"):
        if n.startswith("node") and c in parse_cpuset(rd(f"/sys/devices/system/node/{n}/cpulist")):
            return int(n[4:])
    return -1


def plan_perturbation(kind, d, partner, cpus, mercury_cpu, allowed):
    """Return (perturbed cpuset list, partner used, note)."""
    mine = cpus[d]
    used = set().union(*cpus.values()) | {mercury_cpu}
    idle = [c for c in allowed if c not in used]
    last = mine[-1]
    keep = mine[:-1]
    if kind == "remove":
        if len(mine) < 2:
            raise SystemExit(f"remove: {d} has only {format_cpuset(mine)}")
        return keep, "", "drop " + str(last)
    if kind == "smt":
        for c in cpus[partner]:
            for s in sibling(c):
                if s not in mine and s != mercury_cpu and s in allowed:
                    return sorted(keep + [s]), partner, f"{last}->{s} (sibling of {partner} cpu {c})"
        raise SystemExit(f"smt: no usable sibling of {partner} {format_cpuset(cpus[partner])}")
    if kind == "l3":
        for c in cpus[partner]:
            for s in l3(c):
                if s not in mine and s != mercury_cpu and s in allowed and s not in sibling(c) + [c]:
                    return sorted(keep + [s]), partner, f"{last}->{s} (L3 of {partner} cpu {c})"
        raise SystemExit(f"l3: no free CPU in {partner}'s L3")
    if kind == "numa":
        here = node_of(last)
        far = [c for c in idle if node_of(c) != here]
        if not far:
            raise SystemExit("numa: no idle CPU on another node")
        return sorted(keep + [far[0]]), "", f"{last}->{far[0]} (node {here}->{node_of(far[0])})"
    if kind == "migrate":
        groups = {}
        for c in idle:
            groups.setdefault(tuple(l3(c)), []).append(c)
        mine_l3 = {tuple(l3(c)) for c in mine}
        for g, free in sorted(groups.items(), key=lambda kv: -len(kv[1])):
            if g not in mine_l3 and len(free) >= len(mine):
                return sorted(free[:len(mine)]), "", f"{format_cpuset(mine)}->{format_cpuset(free[:len(mine)])}"
        raise SystemExit("migrate: no other L3 group with enough idle CPUs")
    if kind == "socket":
        here = {socket_of(c) for c in mine}
        far = [c for c in idle if socket_of(c) not in here]
        if len(far) < len(mine):
            raise SystemExit("socket: not enough idle CPUs on another socket")
        return sorted(far[:len(mine)]), "", f"{format_cpuset(mine)}->{format_cpuset(far[:len(mine)])} (socket {sorted(here)}->{socket_of(far[0])})"
    raise SystemExit("unknown kind " + kind)


def _stress_ng():
    # The binary cmake builds. STRESS_NG overrides; DSB_ROOT selects another benchmarks tree.
    env = os.environ.get("STRESS_NG")
    if env:
        return env
    root = os.environ.get("DSB_ROOT")
    if root:
        candidate = os.path.join(root, "stress-ng", "stress-ng")
        if os.access(candidate, os.X_OK):
            return candidate
    here = os.path.dirname(os.path.realpath(__file__))
    return os.path.abspath(os.path.join(here, "..", "..", "stress-ng", "stress-ng"))


STRESS_NG = _stress_ng()
INTENSITY = {"low": (lambda n: 1, 50), "mid": (lambda n: max(1, n // 2), 80), "high": (lambda n: n, 100)}


def stress_cpus(placement, d, cpus, mercury_cpu, allowed):
    mine = cpus[d]
    if placement == "core":
        out = list(mine)
    elif placement == "smt":
        out = [s for c in mine for s in sibling(c)]
    elif placement == "l3":
        out = [x for c in mine for x in l3(c)]
    elif placement == "numa":
        nodes = {node_of(c) for c in mine}
        out = [c for c in allowed if node_of(c) in nodes]
    else:
        raise SystemExit("unknown placement " + placement)
    if placement in ("l3", "numa"):  # around d, not on d or its siblings
        near = set(mine) | {s for c in mine for s in sibling(c)}
        out = [c for c in out if c not in near]
    out = sorted({c for c in out if c != mercury_cpu})
    if not out:
        raise SystemExit(f"stress: no CPUs for placement {placement} around {format_cpuset(mine)}")
    return out


def stress_cmd(stressor, intensity, where):
    workers_of, load = INTENSITY[intensity]
    n = workers_of(len(where))
    cmd = [STRESS_NG, "--taskset", format_cpuset(where), "--timeout", "600s", "--quiet"]
    if stressor == "cpu":
        cmd += ["--cpu", str(n), "--cpu-load", str(load)]
    elif stressor == "cache":
        cmd += ["--cache", str(n)]
    elif stressor == "stream":
        cmd += ["--stream", str(n)]
    elif stressor == "vm":
        cmd += ["--vm", str(n), "--vm-bytes", "256M"]
    elif stressor == "matrix":
        cmd += ["--matrix", str(n)]
    else:
        raise SystemExit("unknown stressor " + stressor)
    return cmd, n


class Stress:
    def __init__(self, cmd):
        self.cmd, self.p = cmd, None

    def start(self):
        if not self.p:
            self.p = subprocess.Popen(self.cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)

    def stop(self):
        if self.p:
            for sig in (signal.SIGTERM, signal.SIGKILL):
                try:
                    os.killpg(self.p.pid, sig)
                except ProcessLookupError:
                    break
                try:
                    self.p.wait(timeout=3)
                    break
                except subprocess.TimeoutExpired:
                    pass
            self.p = None


class Tail:
    """Follow the operator output from its current end; yield complete windows."""

    def __init__(self, path, routes_hint=None):
        self.f = open(path, errors="replace")
        self.f.seek(0, 2)
        self.buf, self.cur, self.routes = "", {}, set(routes_hint or [])

    def windows(self, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            chunk = self.f.read()
            if not chunk:
                time.sleep(0.05)
                continue
            self.buf += chunk
            *lines, self.buf = self.buf.split("\n")
            for line in lines:
                r = parse_line(line)
                if not r:
                    continue
                if r["route"] in self.cur:  # a route repeated: previous window is complete
                    self.routes |= set(self.cur)
                    w, self.cur = self.cur, {}
                    self.cur[r["route"]] = r
                    yield w
                    continue
                self.cur[r["route"]] = r
                if self.routes and set(self.cur) >= self.routes:
                    w, self.cur = self.cur, {}
                    yield w
        raise TimeoutError(f"no operator window in {timeout}s")


def wrap(routes, phase, t):
    r0 = next(iter(routes.values()))
    return {"routes": routes, "clients": r0["clients"], "cpus": r0["cpus"], "phase": phase, "t": t,
            "gen_resized": r0["gen"].get("gen_resized", "0"), "gen_bound": r0["gen"].get("gen_bound", "0")}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", nargs="?", default="run", choices=["run", "restore"])
    ap.add_argument("--mercury", default="http://127.0.0.1:8080")
    ap.add_argument("--prefix", default="hotel-", help="daemon id prefix of the app (hotel- or social-)")
    ap.add_argument("--daemon", help="target daemon id, e.g. hotel-geo")
    ap.add_argument("--kind", default="remove", choices=["remove", "smt", "l3", "numa", "migrate", "socket", "stress"])
    ap.add_argument("--partner", help="partner daemon for smt/l3 (default: busiest other daemon)")
    ap.add_argument("--stressor", default="cpu", choices=["cpu", "cache", "stream", "vm", "matrix"])
    ap.add_argument("--placement", dest="stress_placement", default="smt", choices=["core", "smt", "l3", "numa"])
    ap.add_argument("--intensity", default="mid", choices=list(INTENSITY))
    ap.add_argument("--heldout", default="stream:l3,matrix:smt,vm:numa", help="stressor:placement mixes labelled split=heldout")
    ap.add_argument("--watch", help="operator or loadgen output file being written")
    ap.add_argument("--clients", default="0", help="comma list of user levels to perturb at (0 = first window seen)")
    ap.add_argument("--plan", default="", help="per-window phases (default A,B,B,A for one level, A,B,A for several)")
    ap.add_argument("--mercury-cpu", type=int, default=0, help="CPU Mercury pinned itself to (never handed out)")
    ap.add_argument("--allowed", default="1-60", help="CPUs Mercury manages")
    ap.add_argument("--restore-policy", default="saturate", help="policy re-enabled afterwards (saturate, gnn, none)")
    ap.add_argument("--min", type=int, default=1)
    ap.add_argument("--max", type=int, default=16)
    ap.add_argument("--scale-placement", default="least_loaded")
    ap.add_argument("--exclude", default="loadgen", help="daemon ids containing this are never frozen or touched")
    ap.add_argument("--timeout", type=float, default=600, help="seconds to wait for all levels")
    ap.add_argument("--csv", default="labels/perturb.csv")
    ap.add_argument("--raw", default="labels/perturb_windows.csv")
    ap.add_argument("--state", default="labels/perturb_state.json")
    ap.add_argument("--counters", default="off", choices=["off", "cgroup", "cpu"],
                    help="hardware counters + run-delay: per target cgroup, or system-wide on the target's CPUs")
    ap.add_argument("--zen", default="auto", choices=["auto", "full", "off"],
                    help="AMD family 17h raw events (full adds dispatch stalls, multiplexed): L2, stalls, L3 slice (amd_l3), DRAM (amd_df), SMT sibling")
    ap.add_argument("--dry-run", action="store_true", help="print the plan, change nothing")
    a = ap.parse_args()

    if a.cmd == "restore":
        subprocess.run(["pkill", "-u", str(os.getuid()), "-f", "^" + STRESS_NG], check=False)
        st = json.load(open(a.state))
        for d, spec in st["cpus"].items():
            set_cpus(st["mercury"], d, spec)
        if st["policy"] != "none":
            for d in st["cpus"]:
                set_policy(st["mercury"], d, st["policy"], st["min"], st["max"], st["placement"])
        os.remove(a.state)
        print("restored", len(st["cpus"]), "daemons from", a.state)
        return

    if not a.daemon or not a.watch:
        ap.error("--daemon and --watch are required")
    if os.path.exists(a.state):
        raise SystemExit(f"{a.state} exists: a previous run did not restore; run `perturb.py restore --state {a.state}` first")
    daemons = sorted(x for x in os.listdir(CG) if x.startswith(a.prefix) and a.exclude not in x)
    if a.daemon not in daemons:
        raise SystemExit(f"{a.daemon} not in {CG} ({', '.join(daemons)})")
    orig = {d: rd(f"{CG}/{d}/cpuset.cpus") for d in daemons}
    cpus = {d: parse_cpuset(v) for d, v in orig.items()}
    allowed = parse_cpuset(a.allowed)
    levels = [int(x) for x in a.clients.split(",") if x.strip()]
    plan = [x.strip().upper() for x in (a.plan or ("A,B,B,A" if len(levels) == 1 else "A,B,A")).split(",")]

    partner, stress, sinfo = a.partner, None, {"stressor": "", "stress_placement": "", "intensity": "", "stress_cpus": "", "stress_workers": "", "split": "train"}
    if a.kind == "stress":
        gen_cpus = {c for x in os.listdir(CG) if a.exclude in x for c in parse_cpuset(rd(f"{CG}/{x}/cpuset.cpus"))}
        where = [c for c in stress_cpus(a.stress_placement, a.daemon, cpus, a.mercury_cpu, allowed) if c not in gen_cpus]
        if not where:
            raise SystemExit("stress: placement CPUs are all used by the load generator")
        cmd, n = stress_cmd(a.stressor, a.intensity, where)
        stress, pert = Stress(cmd), cpus[a.daemon]
        note = " ".join(cmd[1:])
        held = {m.strip() for m in a.heldout.split(",") if m.strip()}
        sinfo = {"stressor": a.stressor, "stress_placement": a.stress_placement, "intensity": a.intensity,
                 "stress_cpus": format_cpuset(where), "stress_workers": n,
                 "split": "heldout" if f"{a.stressor}:{a.stress_placement}" in held else "train"}
    else:
        if a.kind in ("smt", "l3") and not partner:
            # busiest other daemon whose CPUs offer a usable sibling / L3 peer (e.g. not Mercury's CPU)
            u0 = {d: usage_usec(d) or 0 for d in daemons}
            time.sleep(1.0)
            ranked = sorted((d for d in daemons if d != a.daemon), key=lambda d: -((usage_usec(d) or 0) - u0[d]))
            for cand in ranked:
                try:
                    pert, partner, note = plan_perturbation(a.kind, a.daemon, cand, cpus, a.mercury_cpu, allowed)
                    break
                except SystemExit as e:
                    print(f"partner {cand} skipped: {e}", flush=True)
            else:
                raise SystemExit(f"{a.kind}: no daemon offers a usable CPU")
        else:
            pert, partner, note = plan_perturbation(a.kind, a.daemon, partner, cpus, a.mercury_cpu, allowed)
    print(f"target {a.daemon} {orig[a.daemon]} -> {format_cpuset(pert)} kind {a.kind} {note} levels {levels} plan {plan} split {sinfo['split']}", flush=True)
    if a.dry_run:
        return

    os.makedirs(os.path.dirname(os.path.abspath(a.csv)), exist_ok=True)
    json.dump({"mercury": a.mercury, "cpus": orig, "policy": a.restore_policy, "min": a.min, "max": a.max,
               "placement": a.scale_placement}, open(a.state, "w"))
    restored = []

    def restore(*_):
        if restored:
            return
        restored.append(1)
        if stress:
            stress.stop()
        errs = []
        for d, spec in orig.items():
            if not os.path.isdir(f"{CG}/{d}"):
                continue  # daemon already gone (operator exited): nothing to restore
            try:
                if rd(f"{CG}/{d}/cpuset.cpus") != spec:
                    set_cpus(a.mercury, d, spec)
            except Exception as e:  # keep going: restore everything we can
                errs.append(str(e))
        if a.restore_policy != "none":
            for d in daemons:
                if not os.path.isdir(f"{CG}/{d}"):
                    continue
                try:
                    set_policy(a.mercury, d, a.restore_policy, a.min, a.max, a.scale_placement)
                except Exception as e:
                    errs.append(str(e))
        if errs:
            print("RESTORE ERRORS (state kept in " + a.state + "):\n  " + "\n  ".join(errs), file=sys.stderr)
        else:
            os.remove(a.state)
            print("restored cpusets and policy", a.restore_policy, "; stress-ng stopped", flush=True)

    def on_signal(sig, _):
        restore()
        sys.exit(128 + sig)

    atexit.register(restore)
    for sg in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sg, on_signal)

    def apply(phase):
        if stress:
            stress.start() if phase == "B" else stress.stop()
        else:
            set_cpus(a.mercury, a.daemon, pert if phase == "B" else orig[a.daemon])

    ctr = None
    if a.counters != "off":
        ctr = Counters(a.counters, a.daemon, cpus[a.daemon] if a.counters == "cpu" else allowed, CG,
                       target_cpus=cpus[a.daemon], zen=a.zen)
        print("counters:", ctr.status(), flush=True)

    def csample():
        return (time.time(), ctr.sample() if ctr else {}, run_delay_ns(CG, a.daemon) if ctr else None)

    for d in daemons:
        set_policy(a.mercury, d, "none", a.min, a.max, a.scale_placement)
    print("froze", len(daemons), "daemons", flush=True)

    gen = Tail(a.watch).windows(a.timeout)
    next(gen)  # the window in progress at freeze time
    results, todo, seen_max = [], list(levels), 0
    while todo:
        want = todo[0]
        got, uses, state, warm = [], [], "A", want != 0
        samples = []
        for routes in gen:
            lvl = next(iter(routes.values()))["clients"]
            if want and lvl < seen_max and lvl < want:  # operator is ramping down: level missed
                print(f"level {want} missed (ramp down at {lvl})", flush=True)
                break
            seen_max = max(seen_max, lvl)
            if want and lvl != want and not got:
                continue
            if warm:
                warm = False
                samples = [csample()]
                continue
            if not samples:
                samples = [csample()]  # no warm-up: first window has no counter baseline
            if got and lvl != got[0]["clients"]:
                print(f"level changed mid-plan at {want}; keeping {len(got)} windows", flush=True)
                break
            now = time.time()
            uses.append((now, usage_usec(a.daemon), usage_usec(partner) if partner else None))
            got.append(wrap(routes, state, now))
            samples.append(csample())
            print(f"window {len(got)} phase {state} clients {lvl} " +
                  " ".join(f"{k}={v['rate']:.0f}/{v['p95']:.0f}us" for k, v in sorted(routes.items())), flush=True)
            if len(got) >= len(plan):
                break
            if plan[len(got)] != state:
                state = plan[len(got)]
                apply(state)
        if state == "B":
            apply("A")
        results.append((want, got, uses, samples))
        todo.pop(0)
    restore()
    cstatus = ctr.status() if ctr else ""
    if ctr:
        ctr.close()

    for want, got, uses, samples in results:
        before = [w for w in got if w["phase"] == "A"]
        after = [w for w in got if w["phase"] == "B"]

        def cores(phase):
            tot_u = tot_t = 0.0
            for i in range(1, len(uses)):
                if got[i]["phase"] == phase and uses[i][1] is not None and uses[i - 1][1] is not None:
                    tot_u += uses[i][1] - uses[i - 1][1]
                    tot_t += uses[i][0] - uses[i - 1][0]
            return f"{tot_u / 1e6 / tot_t:.3f}" if tot_t else ""

        valid = int(bool(before) and bool(after) and all(w["gen_resized"] == "0" for w in got))
        eid = f"perturb:{int(time.time())}:{a.daemon}:{a.kind}:{want}"
        base = {"source": "perturb", "run": os.path.basename(a.watch), "app": a.prefix.rstrip("-"),
                "policy": "frozen", "event_id": eid, "kind": a.kind, "daemon": a.daemon, "partner": partner or "",
                "cpus_before": orig[a.daemon], "cpus_after": format_cpuset(pert),
                "cores_before": len(cpus[a.daemon]), "cores_after": len(pert),
                "usage_cores_before": cores("A"), "usage_cores_after": cores("B"),
                "valid": valid, "note": note, **sinfo}
        if ctr:
            # samples[i+1] closes window i; samples[0] is its start (or window 0's end without warm-up)
            off = len(samples) - len(got)
            pairs = {ph: [(samples[i - 1 + off], samples[i + off]) for i in range(len(got)) if got[i]["phase"] == ph and i - 1 + off >= 0]
                     for ph in ("A", "B")}
            da, db = derived(pairs["A"]), derived(pairs["B"])
            for k in da:
                base[f"{k}_before"], base[f"{k}_after"] = da[k], db[k]
            base["counters"] = f"{a.counters}: {cstatus}"
        rows = route_rows(base, before, after) if before and after else []
        new = not os.path.exists(a.csv)
        with open(a.csv, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS + STRESS_FIELDS + COUNTER_FIELDS, restval="")
            if new:
                w.writeheader()
            w.writerows(rows)
        def cnt(i):
            if not ctr:
                return [""] * (len(RAW_EVENTS) + 1)
            sm = samples[i + len(samples) - len(got)]
            return [("" if sm[1].get(k) is None else f"{sm[1][k]:.0f}") for k in RAW_EVENTS] + ["" if sm[2] is None else sm[2]]
        rnew = not os.path.exists(a.raw)
        with open(a.raw, "a", newline="") as f:
            w = csv.writer(f)
            if rnew:
                w.writerow(["event_id", "t", "window", "phase", "clients", "route", "rate", "avg_us", "p95_us", "p99_us", "errors", "target_usage_usec", "partner_usage_usec",
                        *RAW_EVENTS, "run_delay_ns"])
            for i, win in enumerate(got):
                for route, r in sorted(win["routes"].items()):
                    w.writerow([eid, f"{win['t']:.3f}", i, win["phase"], r["clients"], route, r["rate"], r["avg"], r["p95"],
                                r["p99"] if r["p99"] is not None else "", r["errors"], uses[i][1], uses[i][2] or ""] + cnt(i))
        for r in rows:
            print(f"[{want}] {r['route']}: rate {r['d_rate_pct']}% p95 {r['d_p95_pct']}% p99 {r['d_p99_pct']}%")
        print(f"level {want}: {len(rows)} rows to {a.csv} (valid={valid}, usage cores A {base['usage_cores_before']} B {base['usage_cores_after']})")


if __name__ == "__main__":
    main()
