#!/bin/bash
# Offline test: fake cgroup tree, fake Mercury (/cpus writes the fake cgroup), fake operator output.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd); t=$(mktemp -d); trap 'kill $(jobs -p) 2>/dev/null; rm -rf $t' EXIT
for d in hotel-geo:6,22 hotel-mongod:5 hotel-loadgen:60; do mkdir -p $t/cg/${d%%:*}; echo ${d#*:} > $t/cg/${d%%:*}/cpuset.cpus; echo "usage_usec 1000" > $t/cg/${d%%:*}/cpu.stat; done
cat > $t/merc.py <<PY
import http.server, json
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(s):
        b = json.loads(s.rfile.read(int(s.headers["Content-Length"])))
        if s.path == "/cpus": open("$t/cg/%s/cpuset.cpus" % b["daemon_id"], "w").write(b["cpus"])
        open("$t/calls", "a").write(s.path + " " + json.dumps(b) + "\n")
        s.send_response(200); s.end_headers(); s.wfile.write(b'{"ok":true}')
    def log_message(s, *a): pass
http.server.HTTPServer(("127.0.0.1", 18080), H).serve_forever()
PY
python3 $t/merc.py & sleep 0.5; : > $t/op.out
( for i in $(seq 1 45); do sleep 0.4; g=$(cat $t/cg/hotel-geo/cpuset.cpus); p=$([ "$g" = 6 ] && echo 900 || echo 500)
  for r in geo search; do echo "route $r clients 64 arrival_rate 1000 service_rate 1000 avg_us 400 p95_us $p p99_us 2000 errors 0 cpus geo=$g mongod=5 gen_cpus 60 gen_busy_ms 1 gen_resized 0 gen_bound 0" >> $t/op.out; done; done ) &
cd $t && PERTURB_CG=$t/cg python3 $here/perturb.py --mercury http://127.0.0.1:18080 --daemon hotel-geo --kind remove --watch $t/op.out --clients 64 --timeout 20 --allowed 1-63
cat > $t/sng <<SH
#!/bin/bash
echo "\$@" >> $t/stress_calls; exec sleep 100
SH
chmod +x $t/sng; STRESS_NG=$t/sng PERTURB_CG=$t/cg python3 $here/perturb.py --mercury http://127.0.0.1:18080 --daemon hotel-geo --kind stress --stressor stream --placement l3 --intensity high --watch $t/op.out --clients 64 --timeout 20 --allowed 1-63 --csv labels/s.csv || true; cat $t/stress_calls; pgrep -f $t/sng && echo LEAK || echo nostress; cut -d, -f6,14,24,32- labels/s.csv
echo "--- counters (fake)"; PERTURB_COUNTERS=fake PERTURB_CG=$t/cg python3 $here/perturb.py --mercury http://127.0.0.1:18080 --daemon hotel-geo --kind remove --watch $t/op.out --clients 64 --timeout 20 --allowed 1-63 --counters cgroup --csv labels/c.csv --raw labels/cw.csv | grep -E "counters|wrote|level"
python3 - <<PY2
import csv; r=next(csv.DictReader(open("labels/c.csv")))
print({k: r[k] for k in ["ipc_before","l2_mpki_after","stall_frac_before","l3_miss_rate_before","l3_mpki_after","df_gbs_before","sib_ghz_after","llc_mpki_after","run_delay_ms_s_before","counters"]})
assert r["ipc_before"] == "1.5000" and r["l2_mpki_after"] == "1.3333" and r["stall_frac_before"] == "0.3000", r
assert r["l3_miss_rate_before"] == "0.2500" and r["l3_mpki_after"] == "0.6667" and float(r["df_gbs_before"]) > 0, r
assert r["llc_mpki_after"] == "" and r["run_delay_ms_s_before"], r  # unsupported event -> empty column
w=list(csv.DictReader(open("labels/cw.csv"))); assert w[0]["instructions"] and w[0]["l3_miss"] and w[0]["df_ch4"] == "0", w[0]
PY2
echo "--- counters (real, must degrade on a fake cgroup)"; PERTURB_CG=$t/cg python3 $here/perturb.py --mercury http://127.0.0.1:18080 --daemon hotel-geo --kind remove --watch $t/op.out --clients 64 --timeout 20 --allowed 1-63 --counters cgroup --csv labels/d.csv --raw labels/dw.csv | grep -E "counters|wrote"
python3 -c "import csv; r=next(csv.DictReader(open('labels/d.csv'))); assert r['ipc_before']=='' and ('unavailable' in r['counters'] or 'missing' in r['counters']), r; print('status:', r['counters'][:160])" && echo DEGRADE-OK
cat $t/calls; cat labels/perturb.csv | cut -d, -f6-26; test ! -e labels/perturb_state.json && grep -q '6,22' $t/cg/hotel-geo/cpuset.cpus && echo PASS
