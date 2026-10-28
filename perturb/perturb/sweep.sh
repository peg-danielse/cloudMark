#!/bin/bash
# Pressure sweep: each line is one perturb.sh run (one operator ramp, levels near and past the knee).
# Held-out mixes (perturb.py --heldout default: stream:l3, matrix:smt, vm:numa) are included so the
# eval split exists; train on split=train only.
#   sweep.sh hotel labels/sweep [daemon ...]
# Resume: runs whose key is in OUT/DONE.txt are skipped (SKIP_DONE=1, the default; SKIP_DONE=0 reruns all).
# A run's key ("daemon kind" or "daemon stress stressor placement intensity") is appended when perturb.sh exits 0.
set -u
SYS=$1; OUT=$2; shift 2; HERE=$(cd "$(dirname "$0")" && pwd)
DAEMONS=${@:-hotel-geo hotel-mongod hotel-frontend hotel-search hotel-profile hotel-rate}
LEVELS=${LEVELS:-256,512,1024,2048}; OPF=${OPF:--users 2048 -window-ms 3000}
KINDS="remove smt l3 numa migrate socket"; N=0; for d in $DAEMONS; do N=$((N + 6 + 40)); done; I=0; mkdir -p $OUT
P=$OUT/PROGRESS.txt; DONE=$OUT/DONE.txt; touch $DONE; SKIP_DONE=${SKIP_DONE:-1}
prog() { I=$((I + 1)); KEY="$*"; if [ "$SKIP_DONE" = 1 ] && grep -qxF "$KEY" $DONE; then echo "$(date '+%F %T') run $I/$N: $* (done, skipped)" >> $P; return 1; fi
  echo "$(date '+%F %T') run $I/$N: $*" >> $P; }
ok() { [ $1 = 0 ] && echo "$KEY" >> $DONE; true; }
echo "$(date '+%F %T') start: $N runs, daemons: $DAEMONS, levels $LEVELS, heldout stream:l3 matrix:smt vm:numa" >> $P
for d in $DAEMONS; do
  for k in $KINDS; do prog $d $k || continue; $HERE/perturb.sh $SYS $OUT "$OPF" --daemon $d --kind $k --clients $LEVELS; ok $?; done
  for s in cpu cache stream vm matrix; do for p in core smt l3 numa; do for i in low high; do
    prog $d stress $s $p $i || continue; $HERE/perturb.sh $SYS $OUT "$OPF" --daemon $d --kind stress --stressor $s --placement $p --intensity $i --clients $LEVELS; ok $?
  done; done; done
done
echo "$(date '+%F %T') done" >> $P
