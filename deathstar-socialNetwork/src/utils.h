#ifndef SOCIAL_NETWORK_MICROSERVICES_UTILS_H
#define SOCIAL_NETWORK_MICROSERVICES_UTILS_H

#include <string>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <cstdlib>

#include "logger.h"

namespace social_network{
using json = nlohmann::json;

// consume_advertise_port removes --advertise-port from argv and returns it.
// Zero means the caller should keep the port from configuration.
inline int consume_advertise_port(int &argc, char **argv) {
  int port = 0;
  int write = 1;
  for (int i = 1; i < argc; ++i) {
    std::string arg(argv[i]);
    if (arg == "--advertise-port" && i + 1 < argc) {
      port = std::stoi(argv[++i]);
      continue;
    }
    const std::string prefix = "--advertise-port=";
    if (arg.compare(0, prefix.size(), prefix) == 0) {
      port = std::stoi(arg.substr(prefix.size()));
      continue;
    }
    argv[write++] = argv[i];
  }
  argc = write;
  argv[write] = nullptr;
  return port;
}

int load_config_file(const std::string &file_name, json *config_json) {
  std::ifstream json_file;
  json_file.open(file_name);
  if (json_file.is_open()) {
    json_file >> *config_json;
    json_file.close();
    return 0;
  }
  else {
    LOG(error) << "Cannot open service-config.json";
    return -1;
  }
};

} //namespace social_network

#endif //SOCIAL_NETWORK_MICROSERVICES_UTILS_H
