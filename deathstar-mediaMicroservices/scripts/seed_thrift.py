"""Seed the media services over Thrift, bypassing nginx (the same writes the wrk2 Lua scripts make).

Users: 1000 (register_users.sh). Casts, plots, movie info and movie registration: from datasets/tmdb,
the conversion write_movie_info.py does. Services use the standalone ports from media-runtime/run config.
"""
import argparse
import json
import random
import sys
from concurrent.futures import ThreadPoolExecutor

from media_service import CastInfoService, MovieIdService, MovieInfoService, PlotService, UserService
from media_service.ttypes import Cast
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

BASE_PORT = 9200
PORT = {"cast-info": 0, "movie-id": 2, "movie-info": 3, "plot": 6, "user": 12}
CARRIER = {}


def client(cls, name):
    transport = TTransport.TFramedTransport(TSocket.TSocket("127.0.0.1", BASE_PORT + PORT[name]))
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    transport.open()
    return cls.Client(protocol), transport


def call(cls, name, method, *args):
    c, t = client(cls, name)
    try:
        return getattr(c, method)(*args)
    finally:
        t.close()


def rid():
    return random.getrandbits(63)


def write_cast(raw):
    # Thrift's Python encoder skips None fields, which the service then reads as null and rejects.
    call(CastInfoService, "cast-info", "WriteCastInfo", rid(), int(raw["id"]), raw["name"] or "",
         raw["gender"] == 2, raw["biography"] or "", CARRIER)


KNOWN_CASTS = set()  # cast ids written from casts.json; the page service fails on any other cast reference
SKIPPED = [0]


def write_movie(raw):
    # The page service rejects duplicate cast ids and needs every reference to have a cast record.
    seen = set()
    refs = []
    for c in raw["cast"]:
        cid = int(c["id"])
        if cid in KNOWN_CASTS and cid not in seen:
            seen.add(cid)
            refs.append(c)
    SKIPPED[0] += len(raw["cast"]) - len(refs)
    casts = [Cast(cast_id=int(c["cast_id"]), character=c["character"] or "", cast_info_id=int(c["id"]))
             for c in refs]
    plot_id = int(raw["id"])
    title = raw["title"] or ""
    # The plot service reads an empty plot as missing (it answers with a SUCCESS status), so give it text.
    overview = raw["overview"] or "No plot available."
    call(PlotService, "plot", "WritePlot", rid(), plot_id, overview, CARRIER)
    call(MovieInfoService, "movie-info", "WriteMovieInfo", rid(), str(raw["id"]), title, casts,
         plot_id, [raw["poster_path"] or ""], [], [], float(raw["vote_average"] or 0), int(raw["vote_count"] or 0),
         CARRIER)
    call(MovieIdService, "movie-id", "RegisterMovieId", rid(), title, str(raw["id"]), CARRIER)


def register_user(i):
    call(UserService, "user", "RegisterUser", rid(), "first_name_%d" % i, "last_name_%d" % i,
         "username_%d" % i, "password_%d" % i, CARRIER)


def run(label, items, fn, workers):
    failed = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for n, (_, err) in enumerate(ex.map(lambda x: _try(fn, x), items), 1):
            if err is not None:
                failed.append(err)
            if n % 500 == 0:
                print(label, n, "done, failures so far", len(failed), flush=True)
    print(label, "finished:", len(items), "items,", len(failed), "failed", flush=True)
    for e in failed[:5]:
        print("  first error:", e, flush=True)
    return len(failed)


def _try(fn, item):
    try:
        fn(item)
        return item, None
    except Exception as e:  # report and keep going
        return item, "%s: %s" % (type(e).__name__, str(e)[:200])


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--casts", default="datasets/tmdb/casts.json")
    ap.add_argument("--movies", default="datasets/tmdb/movies.json")
    ap.add_argument("--users", type=int, default=1000)
    ap.add_argument("--workers", type=int, default=16)
    a = ap.parse_args()
    casts = json.load(open(a.casts))
    movies = json.load(open(a.movies))
    KNOWN_CASTS.update(int(c["id"]) for c in casts)
    print("loaded", len(casts), "casts,", len(movies), "movies", flush=True)
    bad = 0
    bad += run("users", range(1, a.users + 1), register_user, a.workers)
    bad += run("casts", casts, write_cast, a.workers)
    bad += run("movies", movies, write_movie, a.workers)
    print("cast references skipped (no cast record):", SKIPPED[0], flush=True)
    print("total failures", bad, flush=True)
    sys.exit(0)
