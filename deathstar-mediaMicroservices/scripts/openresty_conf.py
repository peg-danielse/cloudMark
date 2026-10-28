import json
import os
import re

MEDIA = os.path.expanduser("~/Mercury/benchmarks/deathstar-mediaMicroservices")
PREFIX = os.path.expanduser("~/Mercury/benchmarks/runtime/openresty/install/nginx")
LUA_LOCAL = os.path.join(PREFIX, "lua-local")
CFG = os.path.join(MEDIA, "run", "config", "service-config.json")

# Local ports from the standalone config (the services listen there, not on 9090).
cfg = json.load(open(CFG))
ports = {k: v["port"] for k, v in cfg.items() if isinstance(v, dict) and k.endswith("-service")}

os.makedirs(LUA_LOCAL, exist_ok=True)
os.makedirs(os.path.join(PREFIX, "conf"), exist_ok=True)

# GenericObjectPool: the scripts connect to "<name>-service" on 9090 (Docker DNS); map to 127.0.0.1:<local port>.
src = open(os.path.join(MEDIA, "docker/openresty-thrift/lua-thrift/GenericObjectPool.lua")).read()
table = "local LOCAL_PORTS = {\n" + "".join('  ["%s"] = %d,\n' % (k, p) for k, p in sorted(ports.items())) + "}\n"
patched = src.replace(
    "function GenericObjectPool:connection(thriftClient,ip,port)\n",
    table + "function GenericObjectPool:connection(thriftClient,ip,port)\n"
    "    if LOCAL_PORTS[ip] then\n"
    "        port = LOCAL_PORTS[ip]\n"
    "        ip = \"127.0.0.1\"\n"
    "    end\n", 1)
assert patched != src, "connection signature not found"
open(os.path.join(LUA_LOCAL, "GenericObjectPool.lua"), "w").write(patched)

# nginx.conf: the repo's config with the Docker prefix, the Jaeger plugin and its Lua path replaced.
conf = open(os.path.join(MEDIA, "nginx-web-server/conf/nginx.conf")).read()
conf = "\n".join(l for l in conf.split("\n") if "opentracing_load_tracer" not in l)
lua_path = ";".join([
    os.path.join(LUA_LOCAL, "?.lua"),
    os.path.join(MEDIA, "nginx-web-server/lua-scripts/?.lua"),
    os.path.join(MEDIA, "gen-lua/?.lua"),
    os.path.join(MEDIA, "docker/openresty-thrift/lua-thrift/?.lua"),
    os.path.join(MEDIA, "docker/openresty-thrift/lua-bridge-tracer/?.lua"),
    os.path.join(MEDIA, "docker/openresty-thrift/lua-json/?.lua"),
])
conf = re.sub(r"lua_package_path\s+'[^']*';", "lua_package_path '%s;;';" % lua_path, conf)
conf = conf.replace("/usr/local/openresty/nginx", PREFIX)
conf = "\n".join(l for l in conf.split("\n") if "ngx_http_opentracing_module" not in l and not l.strip().startswith("opentracing "))
conf = conf.replace("  lua_package_path", "  lua_package_cpath '%s/bin/?.so;;';\n  lua_package_path" % PREFIX, 1)
open(os.path.join(PREFIX, "conf/nginx.conf"), "w").write(conf)
print("wrote", os.path.join(PREFIX, "conf/nginx.conf"), "and", os.path.join(LUA_LOCAL, "GenericObjectPool.lua"))
print("ports:", ports)
