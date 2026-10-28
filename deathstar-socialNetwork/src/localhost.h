#ifndef SOCIAL_NETWORK_MICROSERVICES_LOCALHOST_H
#define SOCIAL_NETWORK_MICROSERVICES_LOCALHOST_H

namespace social_network {

// Every outbound dial uses this host. Callers pass only a port.
inline constexpr char kDialHost[] = "127.0.0.1";

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_LOCALHOST_H
