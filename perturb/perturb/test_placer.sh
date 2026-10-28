#!/bin/bash
# Offline test for placer.py: fake cgroups, /proc, sysfs topology, Mercury and affinity.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd); t=$(mktemp -d); trap 'kill $(jobs -p) 2>/dev/null; rm -rf $t' EXIT
# topology: 8 CPUs, sibling n<->n+4, L3 {0,1,4,5} {2,3,6,7}, nodes 0:{0,1,4,5} 1:{2,3,6,7}
for c in $(seq 0 7); do mkdir -p $t/sys/cpu/cpu$c/topology $t/sys/cpu/cpu$c/cache/index3
  echo "$(( c % 4 )),$(( c % 4 + 4 ))" > $t/sys/cpu/cpu$c/topology/thread_siblings_list
  if [ $(( c % 4 )) -lt 2 ]; then echo 0-1,4-5; else echo 2-3,6-7; fi > $t/sys/cpu/cpu$c/cache/index3/shared_cpu_list; done
mkdir -p $t/sys/node/node0 $t/sys/node/node1; echo 0-1,4-5 > $t/sys/node/node0/cpulist; echo 2-3,6-7 > $t/sys/node/node1/cpulist
mkdir -p $t/cg/app-a $t/cg/app-loadgen; echo 1-3 > $t/cg/app-a/cpuset.cpus; echo 7 > $t/cg/app-loadgen/cpuset.cpus
printf "101\n102\n103\n" > $t/cg/app-a/cgroup.threads
# thread 101,102 busy and stacked on cpu1 with high wait; 103 idle
python3 - $t <<'PY' &
import sys, time, os
t = sys.argv[1]; run = {101: 0, 102: 0, 103: 0}; wait = dict(run); busy = [0]*8; idle=[0]*8
for i in range(400):
    for tid, (r, w) in {101: (0.5, 0.5), 102: (0.5, 0.5), 103: (0.0, 0.0)}.items():
        run[tid] += int(r * 25e6); wait[tid] += int(w * 25e6)
        os.makedirs(f"{t}/proc/{tid}", exist_ok=True)
        open(f"{t}/proc/{tid}/schedstat.tmp", "w").write(f"{run[tid]} {wait[tid]} 1\n"); os.replace(f"{t}/proc/{tid}/schedstat.tmp", f"{t}/proc/{tid}/schedstat")
        f = ["S"] + ["0"] * 40; f[36] = "1"
        open(f"{t}/proc/{tid}/stat.tmp", "w").write(f"{tid} (svc) " + " ".join(f) + "\n"); os.replace(f"{t}/proc/{tid}/stat.tmp", f"{t}/proc/{tid}/stat")
    for c in range(8):  # cpu5 (sibling of 1) and cpu6 (sibling of 2) busy
        b = 25 if c in (1, 5, 6) else 1; busy[c] += b; idle[c] += 25 - b
    lines = "cpu 0 0 0 0 0 0 0 0\n" + "".join(f"cpu{c} {busy[c]} 0 0 {idle[c]} 0 0 0 0\n" for c in range(8))
    open(f"{t}/proc/stat.tmp", "w").write(lines); os.replace(f"{t}/proc/stat.tmp", f"{t}/proc/stat")
    time.sleep(0.025)
PY
cat > $t/merc.py <<PY
import http.server, json
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(s):
        b = json.loads(s.rfile.read(int(s.headers["Content-Length"])))
        open("$t/cg/%s/cpuset.cpus" % b["daemon_id"], "w").write(b["cpus"]); open("$t/calls", "a").write(json.dumps(b) + "\n")
        s.send_response(200); s.end_headers(); s.wfile.write(b'{"ok":true}')
    def log_message(s, *a): pass
http.server.HTTPServer(("127.0.0.1", 18081), H).serve_forever()
PY
python3 $t/merc.py & sleep 0.5
export PERTURB_CG=$t/cg PLACER_PROC=$t/proc PLACER_SYS=$t/sys PERTURB_COUNTERS=fake
run() { m=$1; shift; : > $t/aff; PLACER_FAKE_AFFINITY=$t/aff python3 $here/placer.py --mode $m --prefix app- --allowed 1-6 --tick-ms 50 --duration-s 1.2 --mercury http://127.0.0.1:18081 --log $t/$m.jsonl "$@"; }
fail() { echo "FAIL: $*"; exit 1; }
run none;   grep -q "^101 1-3$" $t/aff || fail none; echo "none ok: $(sort -u $t/aff | tr '\n' ' ')"
run static; grep -qE "^10[12] [123]$" $t/aff || fail static; tail -3 $t/aff | grep -q " 1-3$" || fail "static restore"; echo "static ok: $(sort -u $t/aff | tr '\n' ' ')"
run react-thread --k 2
python3 - $t <<'PY' || fail react-thread
import json, sys
mv = [m for l in open(sys.argv[1] + "/react-thread.jsonl") for m in json.loads(l).get("moves", [])]
print("react-thread moves:", mv[:4]); assert mv and mv[0][3] in ("stacked", "sibling"), mv
assert all(m[2] != 1 for m in mv if isinstance(m[2], int))
PY
tail -3 $t/aff | grep -q " 1-3$" || fail "react restore"
run react-cgroup --cg-period 0.2 --l3-hot 0.2
cat $t/calls; head -1 $t/calls | grep -q '"app-a"' || fail "react-cgroup swap"; [ "$(cat $t/cg/app-a/cpuset.cpus)" = 1-3 ] || fail "react-cgroup restore"
python3 $here/ctrlog.py --prefix app- --cpus 1-6 --out $t/cl.jsonl --duration-s 1.6 --period-s 0.5
grep -q "\"app-a\": {\"cpus\": 3, \"ipc\": 2.0" $t/cl.jsonl || fail ctrlog; echo "ctrlog ok: $(sed -n 3p $t/cl.jsonl | cut -c1-90)"
echo PLACER-PASS
