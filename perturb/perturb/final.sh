#!/bin/bash
# Final combined hotel experiment: baselines and every progress step, stress off/on, same -gen-min floor.
#   saturate bare          static saturate baseline (no side-car)
#   saturate none          + side-car observing only
#   saturate react-thread
#   gnn gnn-pin            Kurt's gnn with built-in pinning
#   gnn bare               gnn unpinned (-pin=false), no side-car
#   gnn react-thread / react-cgroup
# gnn model: Mercury's default (models/gnn_v3.json; v5 exists but is documented as "not default").
#   final.sh OUTDIR [REPS]
HERE=$(cd "$(dirname "$0")" && pwd); OUT=${1:?usage: final.sh OUTDIR [REPS]}; REPS=${2:-2}
TS=$(date +%Y%m%d-%H%M%S); mkdir -p $OUT/$TS
POLICY=saturate MODES="bare none react-thread" $HERE/abc.sh hotel $OUT/$TS/saturate $REPS || exit $?
POLICY=gnn MODES="gnn-pin bare react-thread react-cgroup" $HERE/abc.sh hotel $OUT/$TS/gnn $REPS || exit $?
echo FINAL_DIR=$OUT/$TS
