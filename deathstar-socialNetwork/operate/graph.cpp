#include "graph.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>

#include "SocialGraphService.h"
#include "UserService.h"

namespace {

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::transport::TFramedTransport;
using apache::thrift::transport::TSocket;
using social_network::ServiceException;

const char* kHost = "127.0.0.1";
const int kWorkers = 8;
const int kAttempts = 6;

struct Edge {
  int64_t from = 0;
  int64_t to = 0;
};

struct EdgeHash {
  std::size_t operator()(const Edge& edge) const {
    return std::hash<int64_t>{}(edge.from) ^ (std::hash<int64_t>{}(edge.to) << 1);
  }
};

bool operator==(const Edge& left, const Edge& right) {
  return left.from == right.from && left.to == right.to;
}

struct Link {
  std::shared_ptr<TSocket> socket;
  std::shared_ptr<TFramedTransport> transport;
  std::shared_ptr<TBinaryProtocol> protocol;
};

template <class Client>
Client dial(Link& link, int port) {
  link.socket = std::make_shared<TSocket>(kHost, port);
  link.socket->setConnTimeout(3000);
  link.socket->setRecvTimeout(120000);
  link.socket->setSendTimeout(120000);
  link.transport = std::make_shared<TFramedTransport>(link.socket);
  link.protocol = std::make_shared<TBinaryProtocol>(link.transport);
  link.transport->open();
  return Client(link.protocol);
}

int json_port(const std::string& json, const std::string& key) {
  const auto object = json.find("\"" + key + "\"");
  if (object == std::string::npos) {
    throw std::runtime_error("config has no " + key);
  }
  const auto port_key = json.find("\"port\"", object);
  if (port_key == std::string::npos) {
    throw std::runtime_error("config " + key + " has no port");
  }
  auto at = json.find(':', port_key);
  if (at == std::string::npos) {
    throw std::runtime_error("config " + key + " has no port");
  }
  return std::stoi(json.substr(at + 1));
}

std::string trim(std::string line) {
  if (!line.empty() && line.back() == '\r') {
    line.pop_back();
  }
  return line;
}

int64_t node_count(const std::string& edges_path) {
  const std::string suffix = ".edges";
  if (edges_path.size() < suffix.size() ||
      edges_path.compare(edges_path.size() - suffix.size(), suffix.size(), suffix) != 0) {
    return 0;
  }
  const std::string nodes_path = edges_path.substr(0, edges_path.size() - suffix.size()) + ".nodes";
  std::ifstream in(nodes_path);
  int64_t count = 0;
  if (!(in >> count) || count < 0) {
    return 0;
  }
  return count;
}

std::vector<Edge> read_edges(const std::string& path) {
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("cannot read " + path);
  }
  std::unordered_set<Edge, EdgeHash> seen;
  std::vector<Edge> edges;
  std::string raw;
  int number = 0;
  while (std::getline(in, raw)) {
    ++number;
    raw = trim(std::move(raw));
    if (raw.empty() || raw[0] == '%' || raw[0] == '#') {
      continue;
    }
    std::istringstream row(raw);
    int64_t from = 0;
    int64_t to = 0;
    int64_t extra = 0;
    if (!(row >> from >> to) || (row >> extra)) {
      continue;
    }
    if (from == to) {
      continue;
    }
    if (seen.insert(Edge{from, to}).second) {
      edges.push_back(Edge{from, to});
    }
  }
  if (edges.empty()) {
    throw std::runtime_error(path + " has no edges");
  }
  return edges;
}

std::vector<int64_t> user_ids(int64_t count, const std::vector<Edge>& edges) {
  std::unordered_set<int64_t> ids;
  if (count > 0) {
    ids.reserve(static_cast<std::size_t>(count));
    for (int64_t id = 0; id < count; ++id) {
      ids.insert(id);
    }
  }
  for (const Edge& edge : edges) {
    ids.insert(edge.from);
    ids.insert(edge.to);
  }
  return std::vector<int64_t>(ids.begin(), ids.end());
}

bool already_stored(const std::string& message) {
  return message.find("already existed") != std::string::npos ||
         message.find("matching document") != std::string::npos ||
         message.find("No matching") != std::string::npos;
}

struct Gate {
  std::mutex mu;
  std::string message;
  std::atomic<bool> stop{false};

  void fail(const std::string& text) {
    std::lock_guard lock(mu);
    if (message.empty()) {
      message = text;
    }
    stop.store(true);
  }
};

void run_workers(int workers, const std::function<void(int)>& worker) {
  std::vector<std::thread> pool;
  pool.reserve(static_cast<std::size_t>(workers));
  for (int i = 0; i < workers; ++i) {
    pool.emplace_back(worker, i);
  }
  for (auto& thread : pool) {
    thread.join();
  }
}

std::string basename_of(const std::string& path) {
  const auto slash = path.find_last_of('/');
  return slash == std::string::npos ? path : path.substr(slash + 1);
}

}  // namespace

void init_graph(const std::string& config, const std::string& graph_path) {
  const int user_port = json_port(config, "user-service");
  const int social_graph_port = json_port(config, "social-graph-service");
  const std::vector<Edge> edges = read_edges(graph_path);
  const std::vector<int64_t> users = user_ids(node_count(graph_path), edges);
  std::unordered_map<int64_t, std::vector<int64_t>> expected;
  expected.reserve(users.size());
  for (const Edge& edge : edges) {
    expected[edge.from].push_back(edge.to);
  }
  for (auto& [id, followees] : expected) {
    (void)id;
    std::sort(followees.begin(), followees.end());
  }

  const std::map<std::string, std::string> carrier;
  Gate gate;
  std::atomic<std::size_t> next{0};
  std::atomic<std::size_t> done{0};
  std::mutex print_mu;

  std::cout << "init  " << basename_of(graph_path) << " users=" << users.size()
            << " edges=" << edges.size() << std::endl;

  run_workers(kWorkers, [&](int) {
    try {
      Link link;
      auto client = dial<social_network::UserServiceClient>(link, user_port);
      while (!gate.stop.load()) {
        const std::size_t index = next.fetch_add(1);
        if (index >= users.size()) {
          return;
        }
        const int64_t user_id = users[index];
        const std::string name = "user" + std::to_string(user_id);
        bool stored = false;
        for (int attempt = 0; attempt < kAttempts && !stored; ++attempt) {
          try {
            client.RegisterUserWithId(user_id, "First", "Last", name, "password", user_id, carrier);
            stored = true;
          } catch (const ServiceException& ex) {
            if (already_stored(ex.message)) {
              stored = true;
              break;
            }
            if (attempt + 1 == kAttempts) {
              gate.fail("RegisterUserWithId " + name + ": " + ex.message);
            }
          }
        }
        const std::size_t finished = done.fetch_add(1) + 1;
        if (finished % 500 == 0 || finished == users.size()) {
          std::lock_guard lock(print_mu);
          std::cout << "init  registered " << finished << "/" << users.size() << std::endl;
        }
      }
    } catch (const std::exception& ex) {
      gate.fail(ex.what());
    }
  });
  if (!gate.message.empty()) {
    std::cout << "FAIL  SocialGraph.Init\n      " << gate.message << std::endl;
    throw std::runtime_error(gate.message);
  }

  next.store(0);
  done.store(0);
  run_workers(kWorkers, [&](int) {
    try {
      Link link;
      auto client = dial<social_network::SocialGraphServiceClient>(link, social_graph_port);
      while (!gate.stop.load()) {
        const std::size_t index = next.fetch_add(1);
        if (index >= edges.size()) {
          return;
        }
        const Edge& edge = edges[index];
        bool stored = false;
        for (int attempt = 0; attempt < kAttempts && !stored; ++attempt) {
          try {
            client.Follow(edge.from, edge.from, edge.to, carrier);
            stored = true;
          } catch (const ServiceException& ex) {
            if (already_stored(ex.message)) {
              stored = true;
              break;
            }
            if (attempt + 1 == kAttempts) {
              gate.fail("Follow " + std::to_string(edge.from) + " " + std::to_string(edge.to) + ": " + ex.message);
            } else {
              std::this_thread::sleep_for(std::chrono::milliseconds(20 * (attempt + 1)));
            }
          }
        }
        const std::size_t finished = done.fetch_add(1) + 1;
        if (finished % 2000 == 0 || finished == edges.size()) {
          std::lock_guard lock(print_mu);
          std::cout << "init  followed " << finished << "/" << edges.size() << std::endl;
        }
      }
    } catch (const std::exception& ex) {
      gate.fail(ex.what());
    }
  });
  if (!gate.message.empty()) {
    std::cout << "FAIL  SocialGraph.Init\n      " << gate.message << std::endl;
    throw std::runtime_error(gate.message);
  }

  std::vector<int64_t> sources;
  sources.reserve(expected.size());
  for (const auto& [id, followees] : expected) {
    (void)followees;
    sources.push_back(id);
  }
  next.store(0);
  done.store(0);
  run_workers(kWorkers, [&](int) {
    try {
      Link link;
      auto client = dial<social_network::SocialGraphServiceClient>(link, social_graph_port);
      while (!gate.stop.load()) {
        const std::size_t index = next.fetch_add(1);
        if (index >= sources.size()) {
          return;
        }
        const int64_t user_id = sources[index];
        std::vector<int64_t> got;
        client.GetFollowees(got, user_id, user_id, carrier);
        std::sort(got.begin(), got.end());
        got.erase(std::unique(got.begin(), got.end()), got.end());
        const std::vector<int64_t>& want = expected[user_id];
        if (got != want) {
          gate.fail("user " + std::to_string(user_id) + " followees " + std::to_string(got.size()) +
                    " want " + std::to_string(want.size()));
          return;
        }
        const std::size_t finished = done.fetch_add(1) + 1;
        if (finished % 500 == 0 || finished == sources.size()) {
          std::lock_guard lock(print_mu);
          std::cout << "init  checked " << finished << "/" << sources.size() << std::endl;
        }
      }
    } catch (const std::exception& ex) {
      gate.fail(ex.what());
    }
  });
  if (!gate.message.empty()) {
    std::cout << "FAIL  SocialGraph.Init\n      " << gate.message << std::endl;
    throw std::runtime_error(gate.message);
  }

  std::cout << "pass  SocialGraph.Init users=" << users.size() << " edges=" << edges.size() << " graph="
            << basename_of(graph_path) << std::endl;
}
