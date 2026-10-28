local _M = { host = "127.0.0.1" }

local cjson = require "cjson"
local config_path = "/home/paul/Benchmarks/deathstar-socialNetwork/config/service-config.json"
local config

local function load()
  if config ~= nil then
    return config
  end
  local file = io.open(config_path, "r")
  if file == nil then
    error("cannot open " .. config_path)
  end
  local body = file:read("*a")
  file:close()
  config = cjson.decode(body)
  return config
end

function _M.port(service)
  local entry = load()[service]
  if entry == nil or entry.port == nil then
    error("no port for " .. service)
  end
  return entry.port
end

return _M
