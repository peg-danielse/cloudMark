#!/bin/bash
# Snapshot the seeded media data: MongoDB's data directory and redis's dump, for dsb_ab.sh SEED.
# Run it with the stack stopped (mediaoperate stop), so the copies are consistent.
#   scripts/snapshot_seed.sh [OUT_DIR]    default: data/dsb/seeds/media-seed-<timestamp>
set -eu
MEDIA=$(cd "$(dirname "$0")/.." && pwd)
REPO=$(cd "$MEDIA/../.." && pwd)
OUT=${1:-$REPO/data/dsb/seeds/media-seed-$(date +%Y%m%d-%H%M%S)}
MONGO_DIR=$REPO/benchmarks/runtime/mongodb/27018/db
REDIS_DUMP=$MEDIA/run/data/redis/dump.rdb
pgrep -x mongod >/dev/null && { echo "mongod is running: stop the stack first"; exit 1; }
mkdir -p "$OUT/mongodb" "$OUT/redis"
cp -a "$MONGO_DIR/." "$OUT/mongodb/"
cp -p "$REDIS_DUMP" "$OUT/redis/dump.rdb"
echo "$OUT"
