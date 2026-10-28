"""Optional hardware counters and scheduler delay for perturb.py.

Backend: perf_event_open(2) via ctypes (same kernel interface and permission rules as
`perf stat`; the perf binary is not needed). Two scopes:
  cgroup  events of one Mercury daemon cgroup (pid = cgroup fd, PERF_FLAG_PID_CGROUP),
          opened on every CPU in --allowed (a cgroup event counts only on its CPU)
  cpu     system-wide events on given CPUs (pid = -1)
Events: cycles, instructions, cache-misses (generic), LLC read misses (HW_CACHE; Zen 1
often reports ENOENT for it, the column is then empty). Counts are scaled by
time_enabled/time_running when multiplexed.

Needs kernel.perf_event_paranoid <= 0 for cgroup/cpu-wide events as a normal user
(or CAP_PERFMON). Any open error degrades to empty columns with a reason string.

Run-delay: sum over the daemon's threads of /proc/<tid>/schedstat field 2 (ns waited on
a runqueue); reported as ms of wait per second of wall time.

PERTURB_COUNTERS=fake returns synthetic counts (offline test).
"""
import ctypes, os, struct

PERF_TYPE_HARDWARE, PERF_TYPE_HW_CACHE = 0, 3
EVENTS = {
    "cycles": (PERF_TYPE_HARDWARE, 0),
    "instructions": (PERF_TYPE_HARDWARE, 1),
    "cache_misses": (PERF_TYPE_HARDWARE, 3),
    "llc_load_misses": (PERF_TYPE_HW_CACHE, 2 | (0 << 8) | (1 << 16)),  # LL, READ, MISS
}
PERF_FLAG_FD_CLOEXEC, PERF_FLAG_PID_CGROUP = 1 << 3, 1 << 2
NR_PERF_EVENT_OPEN = 298  # x86_64
_libc = ctypes.CDLL(None, use_errno=True)


def paranoid():
    try:
        return int(open("/proc/sys/kernel/perf_event_paranoid").read())
    except OSError:
        return None


def _open(etype, config, pid, cpu, flags):
    attr = bytearray(128)
    # type, size, config, sample_period, sample_type, read_format, flags(bitfield)
    struct.pack_into("IIQQQQQ", attr, 0, etype, 128, config, 0, 0, 1 | 2, 0)
    buf = (ctypes.c_char * 128).from_buffer(attr)
    fd = _libc.syscall(NR_PERF_EVENT_OPEN, buf, pid, cpu, -1, flags | PERF_FLAG_FD_CLOEXEC)
    if fd < 0:
        e = ctypes.get_errno()
        raise OSError(e, os.strerror(e))
    return fd


def _raw(fd):
    return struct.unpack("QQQ", os.read(fd, 24))  # value, time_enabled, time_running


PMU = "/sys/bus/event_source/devices"


def pmu_type(name):
    try:
        return int(open(f"{PMU}/{name}/type").read())
    except (OSError, ValueError):
        return None


def cpu_family():
    try:
        for line in open("/proc/cpuinfo"):
            if line.startswith("cpu family"):
                return int(line.split(":")[1])
    except OSError:
        pass
    return None


def _df(ev, umask):
    """amd_df config: event=config:0-7,32-35, umask=config:8-15."""
    return (ev & 0xFF) | (((ev >> 8) & 0xF) << 32) | (umask << 8)


# AMD family 17h model 01h (Zen 1, EPYC 7301). Codes from the kernel pmu-events amdzen1 JSON and
# the family 17h PPR; verified against /sys/bus/event_source/devices/{cpu,amd_l3,amd_df} formats and
# a live read on dtn-01 (2026-10-18). Core events use the core PMU ("cpu", type 4).
ZEN1_CORE = {
    "l2_ref": 0xFF60,         # l2_request_g1 all (= generic cache-references on Zen 1)
    "l2_miss": 0x0964,        # l2_cache_req_stat.ic_dc_miss_in_l2 (= generic cache-misses)
    "stall_fe": 0x0287,       # ic_fetch_stall upstream (= generic stalled-cycles-frontend)
    "stall_be": 0x0187,       # ic_fetch_stall back-pressure (= generic stalled-cycles-backend)
    "disp_stall0": 0x7FAF,    # de_dis_dispatch_token_stalls0 (ALU/AGU/retire/etc. tokens)
    "disp_stall1": 0xFEAE,    # de_dis_dispatch_token_stalls1 (int/fp regs, LdQ, StQ, ...)
}
# L3 PMU ("amd_l3"): per CCX (L3 slice), system-wide only; slicemask/threadmask = all.
ZEN1_L3 = {
    "l3_access": 0x01 | (0x80 << 8) | (0xF << 48) | (0xFF << 56),  # l3_request_g1.caching_l3_cache_accesses
    "l3_miss": 0x06 | (0x01 << 8) | (0xF << 48) | (0xFF << 56),    # l3_comb_clstr_state.request_miss
}
# Data fabric ("amd_df"): one instance per die/NUMA node. dram_channel_data_controller_0..7
# (umask 0x38), 64-byte beats as seen by that die's DF; slot 4 reads 0 on dtn-01.
ZEN1_DF = {f"df_ch{i}": _df(ev, 0x38) for i, ev in enumerate([0x07, 0x47, 0x87, 0xC7, 0x107, 0x147, 0x187, 0x1C7])}

ALL_EVENTS = list(EVENTS) + list(ZEN1_CORE) + list(ZEN1_L3) + list(ZEN1_DF) + ["sib_cycles"]


def l3_rep(c):
    """First CPU of c's L3 slice: the CPU the amd_l3 PMU is opened on (cpumask 0,2,4,...)."""
    try:
        from common import parse_cpuset
        return parse_cpuset(open(f"/sys/devices/system/cpu/cpu{c}/cache/index3/shared_cpu_list").read())[0]
    except (OSError, IndexError):
        return None


def df_rep(c):
    """First CPU of c's NUMA node: the CPU the amd_df PMU is opened on (cpumask 0,4,8,...)."""
    from common import parse_cpuset
    for n in sorted(os.listdir("/sys/devices/system/node")):
        if n.startswith("node"):
            cpus = parse_cpuset(open(f"/sys/devices/system/node/{n}/cpulist").read())
            if c in cpus:
                return cpus[0]
    return None


class Counters:
    """Open counters for one scope; sample() returns cumulative counts per event (None if unavailable).

    Daemon-attributed: generic EVENTS and ZEN1_CORE (cgroup scope: only the daemon's threads; cpu
    scope: everything on the target's CPUs). Shared context, never the daemon's alone: ZEN1_L3 on the
    L3 slices of the target's CPUs, ZEN1_DF on their NUMA nodes, sib_cycles on their SMT siblings.
    """

    def __init__(self, scope, target, cpus, cg_root, target_cpus=None, zen="auto"):
        self.fds, self.errors, self.cgfd = {}, {}, None
        self.fake = os.environ.get("PERTURB_COUNTERS") == "fake"
        self.n = 0
        self.zen = False
        if self.fake:
            return
        try:
            if scope == "cgroup":
                self.cgfd = os.open(os.path.join(cg_root, target), os.O_RDONLY)
                pid, flags = self.cgfd, PERF_FLAG_PID_CGROUP
            else:
                pid, flags = -1, 0
        except OSError as e:
            self.errors["all"] = str(e)
            return
        self.last, self.total = {}, {}
        fam, core = cpu_family(), pmu_type("cpu")
        self.zen = zen != "off" and fam == 0x17 and core is not None
        # Zen 1 has 6 core counters: open 6 core events by default so nothing multiplexes.
        # cache-misses = l2_miss and LLC is unmapped on Zen 1, so the generic ones are skipped there.
        for name, (t, c) in EVENTS.items():
            if self.zen and name in ("cache_misses", "llc_load_misses"):
                continue
            self._add(name, [(t, c, pid, cpu, flags) for cpu in cpus])
        if zen == "off":
            return
        if not self.zen:
            self.errors["zen1"] = f"cpu family {fam} is not 17h (Zen 1/2); raw events skipped"
            return
        for name, c in ZEN1_CORE.items():
            if name.startswith("disp_stall") and zen != "full":
                continue
            self._add(name, [(core, c, pid, cpu, flags) for cpu in cpus])
        tc = sorted(set(target_cpus or cpus))
        l3, df = pmu_type("amd_l3"), pmu_type("amd_df")
        slices = sorted({r for r in (l3_rep(c) for c in tc) if r is not None})
        nodes = sorted({r for r in (df_rep(c) for c in tc) if r is not None})
        sib = sorted({s for c in tc for s in _siblings(c)} - set(tc))
        self.slices, self.nodes, self.sibs = slices, nodes, sib
        if l3 is None:
            self.errors["l3"] = "no amd_l3 PMU"
        else:
            for name, c in ZEN1_L3.items():
                self._add(name, [(l3, c, -1, r, 0) for r in slices])
        if df is None:
            self.errors["df"] = "no amd_df PMU"
        else:
            for name, c in ZEN1_DF.items():
                self._add(name, [(df, c, -1, r, 0) for r in nodes])
        self._add("sib_cycles", [(core, 0x76, -1, s, 0) for s in sib])

    def _add(self, name, specs):
        fds = []
        try:
            for t, c, pid, cpu, flags in specs:
                fds.append(_open(t, c, pid, cpu, flags))
            if fds:
                self.fds[name] = fds
        except OSError as e:
            for fd in fds:
                os.close(fd)
            self.errors[name] = e.strerror

    def sample(self):
        if self.fake:
            self.n += 1
            n = self.n
            f = {"cycles": 1e9, "instructions": 1.5e9, "cache_misses": 2e6, "llc_load_misses": None,
                 "l2_ref": 6e6, "l2_miss": 2e6, "stall_fe": 1e8, "stall_be": 2e8, "disp_stall0": 5e7, "disp_stall1": 5e7,
                 "l3_access": 4e6, "l3_miss": 1e6, "sib_cycles": 5e8}
            out = {k: (None if v is None else v * n) for k, v in f.items()}
            out.update({f"df_ch{i}": (0 if i == 4 else 1.5625e7 * n) for i in range(8)})  # 7 x 1 GB/window
            return out
        # Monotonic multiplex-scaled totals: per fd, add dvalue * denabled / drunning since the last read.
        for name, fds in self.fds.items():
            for fd in fds:
                v, en, run = _raw(fd)
                v0, en0, run0 = self.last.get(fd, (0, 0, 0))
                self.last[fd] = (v, en, run)
                if run > run0:
                    self.total[fd] = self.total.get(fd, 0.0) + (v - v0) * (en - en0) / (run - run0)
        out = {name: (sum(self.total.get(fd, 0.0) for fd in self.fds[name]) if name in self.fds else None) for name in ALL_EVENTS}
        if self.zen and out.get("cache_misses") is None:
            out["cache_misses"] = out.get("l2_miss")
        return out

    def status(self):
        if self.fake:
            return "fake"
        if not self.fds:
            p = paranoid()
            return f"unavailable ({'; '.join(f'{k}: {v}' for k, v in self.errors.items())}; perf_event_paranoid={p})"
        ok = [k for k in ALL_EVENTS if k in self.fds]
        extra = ""
        if self.zen:
            extra = f" zen1 slices={self.slices} df_nodes={self.nodes} siblings={self.sibs}"
        miss = [k for k in self.errors]
        return f"perf_event_open {len(ok)} events{extra}" + (f" missing: {', '.join(miss)}" if miss else "")

    def close(self):
        for fds in self.fds.values():
            for fd in fds:
                os.close(fd)
        self.fds = {}
        if self.cgfd is not None:
            os.close(self.cgfd)
            self.cgfd = None


def _siblings(c):
    try:
        from common import parse_cpuset
        return [x for x in parse_cpuset(open(f"/sys/devices/system/cpu/cpu{c}/topology/thread_siblings_list").read()) if x != c]
    except OSError:
        return []


def run_delay_ns(cg_root, daemon):
    """Total runqueue wait (ns) of all threads in the daemon's cgroup; None if unreadable."""
    if os.environ.get("PERTURB_COUNTERS") == "fake":
        run_delay_ns.n = getattr(run_delay_ns, "n", 0) + 1
        return 5e7 * run_delay_ns.n
    try:
        tids = open(os.path.join(cg_root, daemon, "cgroup.threads")).read().split()
    except OSError:
        return None
    tot, ok = 0, False
    for t in tids:
        try:
            tot += int(open(f"/proc/{t}/schedstat").read().split()[1])
            ok = True
        except (OSError, IndexError, ValueError):
            pass
    return tot if ok else None


def derived(samples):
    """samples: (start, end) pairs of (t, counts, run_delay_ns) for one phase.
    Daemon metrics: ipc, cache_mpki (Zen 1: = L2 misses), llc_mpki, l2_mpki, stall_frac.
    Shared-slice context: l3_miss_rate, l3_mpki (slice misses per 1k *daemon* instructions),
    df_gbs (DRAM beats x 64 B on the target's node(s)), sib_ghz (SMT sibling busy cycles)."""
    d = {k: 0.0 for k in ALL_EVENTS}
    have = {k: bool(samples) for k in ALL_EVENTS}
    rd = dt = 0.0
    rd_ok = bool(samples)
    for (t0, c0, r0), (t1, c1, r1) in samples:
        dt += t1 - t0
        for k in ALL_EVENTS:
            if c0.get(k) is None or c1.get(k) is None:
                have[k] = False
            else:
                d[k] += c1[k] - c0[k]
        if r0 is None or r1 is None:
            rd_ok = False
        else:
            rd += r1 - r0

    def g(k):
        return d[k] if have[k] else None
    ins = g("instructions") if g("instructions") else None
    cyc = g("cycles") if g("cycles") else None

    def ratio(a, b, k=1.0):
        return None if a is None or not b else k * a / b
    df = [g(k) for k in ZEN1_DF]
    stall = None if g("stall_fe") is None or g("stall_be") is None else g("stall_fe") + g("stall_be")

    def f(x):
        return "" if x is None else f"{x:.4f}"
    return {
        "ipc": f(ratio(ins, cyc)),
        "cache_mpki": f(ratio(g("cache_misses"), ins, 1000)),
        "llc_mpki": f(ratio(g("llc_load_misses"), ins, 1000)),
        "run_delay_ms_s": f(rd / 1e6 / dt if rd_ok and dt > 0 else None),
        "l2_mpki": f(ratio(g("l2_miss"), ins, 1000)),
        "stall_frac": f(ratio(stall, cyc)),
        "l3_miss_rate": f(ratio(g("l3_miss"), g("l3_access"))),
        "l3_mpki": f(ratio(g("l3_miss"), ins, 1000)),
        "df_gbs": f(None if any(x is None for x in df) or not dt else sum(df) * 64 / dt / 1e9),
        "sib_ghz": f(ratio(g("sib_cycles"), dt * 1e9) if dt else None),
    }


if __name__ == "__main__":
    # Live probe: python3 counters.py CPU [seconds<=0.5] -- system-wide counters on one CPU, its L3
    # slice, NUMA node DF and SMT sibling; prints raw deltas, derived metrics and status.
    import sys, time
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    cpu = int(sys.argv[1])
    secs = min(float(sys.argv[2]) if len(sys.argv) > 2 else 0.3, 0.5)
    c = Counters("cpu", None, [cpu], "/sys/fs/cgroup/mercury", target_cpus=[cpu])
    s0 = (time.time(), c.sample(), None)
    time.sleep(secs)
    s1 = (time.time(), c.sample(), None)
    print(c.status())
    print({k: (None if s0[1][k] is None else int(s1[1][k] - s0[1][k])) for k in ALL_EVENTS})
    print(derived([(s0, s1)]))
    c.close()
