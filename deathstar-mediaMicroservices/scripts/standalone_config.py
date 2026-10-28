#!/usr/bin/env python3
"""Set up the standalone media stack under run/, for one host.

Writes run/config/service-config.json (every backend on 127.0.0.1: MongoDB on 27018, one memcached per service on
11300 upward, redis on 6379, the 13 services on 9200 upward) and run/config/jaeger-config.yml (tracing off). Adds the
links Mercury execs: run/<Service> -> launch, and run/<port> -> the memcached wrapper (the wrapper reads its port from
its name). Run it again after changing a port; it overwrites what it wrote.

The source config (docker hostnames, every service on 9090) is only read. One memcached per service matches the Docker
setup: services key their entries by id, so a shared instance lets one service's entries overwrite another's.
"""
import json
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN = os.path.join(ROOT, "run")
CONFIG = os.path.join(RUN, "config")
SRC_CONFIG = os.path.join(ROOT, "config", "service-config.json")
SRC_JAEGER = os.path.join(ROOT, "config", "jaeger-config.yml")

# Services in the order the seed scripts index them (cast-info is 0, user is 12).
BINARIES = ["CastInfoService", "ComposeReviewService", "MovieIdService", "MovieInfoService", "MovieReviewService",
            "PageService", "PlotService", "RatingService", "ReviewStorageService", "TextService", "UniqueIdService",
            "UserReviewService", "UserService"]
BASE_PORT = 9200
MONGO_PORT = 27018  # the run/mongod wrapper starts mongod here; its data is runtime/mongodb/27018/db
REDIS_PORT = 6379
MEMCACHED_BASE = 11300


def config_key(binary):
    # PageService -> page-service, as the operator and the C++ services name them.
    return re.sub(r"(?<!^)([A-Z])", r"-\1", binary.replace("Service", "")).lower() + "-service"


def main():
    cfg = json.load(open(SRC_CONFIG))
    ports = {config_key(b): BASE_PORT + i for i, b in enumerate(BINARIES)}
    memcached = []
    for key, val in cfg.items():
        if not isinstance(val, dict) or "addr" not in val:
            continue
        val["addr"] = "127.0.0.1"
        if key in ports:
            val["port"] = ports[key]
        elif key.endswith("-mongodb"):
            val["port"] = MONGO_PORT
        elif key.endswith("-redis"):
            val["port"] = REDIS_PORT
        elif key.endswith("-memcached"):
            val["port"] = MEMCACHED_BASE + len(memcached)
            memcached.append(val["port"])

    os.makedirs(CONFIG, exist_ok=True)
    os.makedirs(os.path.join(RUN, "data", "redis"), exist_ok=True)
    with open(os.path.join(CONFIG, "service-config.json"), "w") as f:
        json.dump(cfg, f, indent=2)
        f.write("\n")
    # Tracing points at a jaeger host that does not exist here: switch it off, keep the rest of the source file.
    jaeger = [line for line in open(SRC_JAEGER).read().splitlines() if not line.startswith("disabled:")]
    with open(os.path.join(CONFIG, "jaeger-config.yml"), "w") as f:
        f.write("\n".join(["disabled: true"] + jaeger) + "\n")

    link([os.path.join(RUN, b) for b in BINARIES], "launch")
    link([os.path.join(RUN, str(p)) for p in memcached], "../../runtime/memcached/memcached-server")

    print("wrote", os.path.join(CONFIG, "service-config.json"))
    for b in BINARIES:
        print("%-22s %d" % (config_key(b), ports[config_key(b)]))
    print("mongodb %d  redis %d  memcached %s" % (MONGO_PORT, REDIS_PORT, memcached))


def link(names, to):
    for name in names:
        if os.path.lexists(name):
            os.remove(name)
        os.symlink(to, name)


if __name__ == "__main__":
    main()
