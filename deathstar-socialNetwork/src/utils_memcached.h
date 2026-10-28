#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_UTILS_MEMCACHED_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_UTILS_MEMCACHED_H_

#include <libmemcached/memcached.h>
#include <libmemcached/util.h>
#include "localhost.h"

namespace social_network {

memcached_pool_st *init_memcached_client_pool(
    const json &config_json,
    const std::string &service_name,
    uint32_t min_size,
    uint32_t max_size
) {
  const std::string addr = kDialHost;
  int port = config_json[service_name + "-memcached"]["port"];
  int use_binary_protocol = config_json[service_name + "-memcached"]["binary_protocol"];
  std::string config_str = "--SERVER=" + addr + ":" + std::to_string(port);
  auto memcached_client = memcached(config_str.c_str(), config_str.length());
  memcached_behavior_set(memcached_client, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);
  memcached_behavior_set(memcached_client, MEMCACHED_BEHAVIOR_TCP_NODELAY, 1);
  // NO_BLOCK reports CONNECTION FAILURE on the first command while connect()
  // is still in progress. The same error comes back after the socket has been
  // idle long enough for IPVS to drop its conntrack entry: the pooled client
  // still believes the socket is open. A zero retry timeout makes the next
  // command reconnect. Callers must treat that first failure as a cache miss
  // and read MongoDB; the value was never only in memcached.
  memcached_behavior_set(memcached_client, MEMCACHED_BEHAVIOR_RETRY_TIMEOUT, 0);
  if (use_binary_protocol == 1) {
    memcached_behavior_set(memcached_client, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
  }

  auto memcached_client_pool =
      memcached_pool_create(memcached_client, min_size, max_size);
  return memcached_client_pool;
}

} // namespace social_network

#endif //SOCIAL_NETWORK_MICROSERVICES_SRC_UTILS_MEMCACHED_H_
