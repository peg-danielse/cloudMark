#ifndef SOCIAL_NETWORK_OPERATE_ROUTES_H
#define SOCIAL_NETWORK_OPERATE_ROUTES_H

#include <string>

// test_routes dials 127.0.0.1 and the port from config for every Thrift method.
// It prints one pass/FAIL line per method and throws if any call fails.
void test_routes(const std::string& config);

#endif
