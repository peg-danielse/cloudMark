#!/usr/bin/env python3
"""User-space stand-in for Mercury's worker API, for testing the operators on a
machine without root (no cgroups, no pinning). Implements /ping, /start (exec
with only the given env plus PATH/HOME, like Mercury), /kill, /cpus, /scale.
Usage: fake_mercury.py PORT [CWD]"""
import json, os, signal, subprocess, sys, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

procs = {}
lock = threading.Lock()
cwd = sys.argv[2] if len(sys.argv) > 2 else os.getcwd()
logdir = os.environ.get("FAKE_MERCURY_LOGS", "/tmp/fake-mercury")
os.makedirs(logdir, exist_ok=True)

class H(BaseHTTPRequestHandler):
    def log_message(self, fmt, *a):
        sys.stderr.write("fake-mercury: " + (fmt % a) + "\n")
    def reply(self, code, obj):
        b = json.dumps(obj, separators=(",", ":")).encode()
        self.send_response(code); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        if self.path == "/ping":
            b = b"pong\n"; self.send_response(200); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
        else:
            self.reply(404, {"ok": False})
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0)); req = json.loads(self.rfile.read(n) or b"{}")
        did = req.get("daemon_id", "")
        if self.path == "/start":
            env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": os.environ.get("HOME", "/tmp")}
            env.update({k: str(v) for k, v in (req.get("env") or {}).items()})
            log = open(os.path.join(logdir, did + ".log"), "ab")
            try:
                p = subprocess.Popen([req["exec_path"]], cwd=cwd, env=env, stdout=log, stderr=log, start_new_session=True)
            except OSError as e:
                return self.reply(400, {"ok": False, "error": str(e)})
            with lock:
                procs[did] = p
            return self.reply(200, {"ok": True, "pid": p.pid})
        if self.path == "/kill":
            with lock:
                p = procs.pop(did, None)
            if p is None:
                return self.reply(404, {"ok": False, "error": "unknown daemon"})
            try:
                os.killpg(p.pid, signal.SIGTERM); p.wait(timeout=10)
            except Exception:
                try: os.killpg(p.pid, signal.SIGKILL)
                except Exception: pass
            return self.reply(200, {"ok": True})
        if self.path in ("/cpus", "/scale"):
            return self.reply(200 if did in procs else 404, {"ok": did in procs})
        self.reply(404, {"ok": False})

def shutdown(*_):
    with lock:
        for p in procs.values():
            try: os.killpg(p.pid, signal.SIGTERM)
            except Exception: pass
    os._exit(0)
signal.signal(signal.SIGTERM, shutdown); signal.signal(signal.SIGINT, shutdown)
ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
