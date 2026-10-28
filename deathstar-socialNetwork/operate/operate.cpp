// operate submits mercury-social.txt to a running Mercury process.
//
// Mercury is started separately. The run/ launchers cd into this tree so the
// services find config/service-config.json:
//
//   mercury <port> <cpu> [cpu...]
//
//   operate -mercury 127.0.0.1:8080 -cpu 2 test
//   operate -mercury 127.0.0.1:8080 -cpu 2 start
//   operate -mercury 127.0.0.1:8080 stop
//   operate -mercury 127.0.0.1:8080 -cpu 2 init
//   operate -mercury 127.0.0.1:8080 -cpu 2 -graph datasets/social-graph/socfb-Reed98/socfb-Reed98.edges init
//
// test starts every line, waits until that program is listening, calls every
// Thrift method on 127.0.0.1 and that service's port, then kills them.
// init does the same startup, loads the social graph, checks followees, then
// kills the daemons. MongoDB keeps the users and edges. -graph defaults to
// socfb-Reed98.edges. WriteHomeTimelineService does not listen; it is checked
// with POST /stat.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <linux/limits.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "graph.h"
#include "routes.h"

namespace {

[[noreturn]] void die(const std::string& message);

const std::string& benchmarks_root() {
  static const std::string root = [] {
    if (const char* env = std::getenv("DSB_ROOT")) {
      if (env[0] != '\0') {
        return std::string(env);
      }
    }
    char buf[PATH_MAX];
    const ssize_t n = ::readlink("/proc/self/exe", buf, sizeof buf - 1);
    if (n < 0) {
      die("readlink /proc/self/exe");
    }
    buf[n] = '\0';
    std::string dir(buf);
    while (true) {
      const auto slash = dir.find_last_of('/');
      if (slash == std::string::npos || slash == 0) {
        break;
      }
      dir.resize(slash);
      std::ifstream marker(dir + "/deathstar-hotelReservations/config.json");
      if (marker) {
        return dir;
      }
    }
    die(std::string("cannot find the benchmarks directory from ") + buf);
  }();
  return root;
}

std::string resolve_exec(const std::string& path) {
  if (!path.empty() && path[0] == '/') {
    return path;
  }
  return benchmarks_root() + "/" + path;
}

struct Tuple {
  std::string path;
  std::string hostname;
  std::string id;
};

struct Response {
  int status = 0;
  std::string body;
};

std::mutex g_mu;
std::vector<Tuple> g_started;
std::string g_base_host;
int g_base_port = 0;

[[noreturn]] void die(const std::string& message) {
  std::cerr << message << std::endl;
  std::exit(1);
}

std::string basename_of(const std::string& path) {
  const auto slash = path.find_last_of('/');
  return slash == std::string::npos ? path : path.substr(slash + 1);
}

std::string json_escape(const std::string& text) {
  std::string out;
  for (const char c : text) {
    if (c == '\\' || c == '"') {
      out.push_back('\\');
    }
    out.push_back(c);
  }
  return out;
}

std::string read_file(const std::string& path) {
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("cannot read " + path);
  }
  std::ostringstream buffer;
  buffer << in.rdbuf();
  return buffer.str();
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
  ++at;
  while (at < json.size() && std::isspace(static_cast<unsigned char>(json[at])) != 0) {
    ++at;
  }
  return std::stoi(json.substr(at));
}

std::string service_key(const std::string& binary) {
  std::string name = binary;
  const std::string suffix = "Service";
  if (name.size() > suffix.size() && name.compare(name.size() - suffix.size(), suffix.size(), suffix) == 0) {
    name.resize(name.size() - suffix.size());
  }
  std::string key;
  for (std::size_t i = 0; i < name.size(); ++i) {
    const unsigned char c = static_cast<unsigned char>(name[i]);
    if (std::isupper(c) != 0 && i > 0) {
      key.push_back('-');
    }
    key.push_back(static_cast<char>(std::tolower(c)));
  }
  return key + "-service";
}

std::string listen_address(const Tuple& line, const std::string& config) {
  const std::string base = basename_of(line.path);
  char* end = nullptr;
  const long port = std::strtol(base.c_str(), &end, 10);
  if (end != nullptr && *end == '\0' && port > 0) {
    return "127.0.0.1:" + base;
  }
  if (base == "mongod") {
    return "127.0.0.1:27017";
  }
  if (base == "redis-server") {
    return "127.0.0.1:6379";
  }
  if (base == "rabbitmq-server") {
    return "127.0.0.1:5672";
  }
  if (base == "WriteHomeTimelineService") {
    return {};
  }
  return "127.0.0.1:" + std::to_string(json_port(config, service_key(base)));
}

std::vector<Tuple> parse_file(const std::string& path) {
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("cannot read " + path);
  }
  std::vector<Tuple> lines;
  std::string raw;
  int number = 0;
  while (std::getline(in, raw)) {
    ++number;
    const auto hash = raw.find('#');
    if (hash != std::string::npos) {
      raw.resize(hash);
    }
    std::istringstream row(raw);
    std::string exec_path;
    std::string hostname;
    if (!(row >> exec_path >> hostname)) {
      continue;
    }
    std::string extra;
    if (row >> extra) {
      throw std::runtime_error(path + ":" + std::to_string(number) + ": want exec_path and hostname");
    }
    const std::string resolved = resolve_exec(exec_path);
    Tuple line{resolved, hostname, "social-" + basename_of(resolved)};
    for (const auto& existing : lines) {
      if (existing.id == line.id) {
        throw std::runtime_error("duplicate daemon id " + line.id);
      }
    }
    lines.push_back(std::move(line));
  }
  if (lines.empty()) {
    throw std::runtime_error(path + " has no programs");
  }
  return lines;
}

bool split_host_port(const std::string& address, std::string& host, int& port) {
  const auto colon = address.rfind(':');
  if (colon == std::string::npos || colon == 0) {
    return false;
  }
  host = address.substr(0, colon);
  try {
    port = std::stoi(address.substr(colon + 1));
  } catch (const std::exception&) {
    return false;
  }
  return port > 0 && port <= 65535;
}

int dial(const std::string& host, int port, int timeout_ms) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* result = nullptr;
  const std::string port_text = std::to_string(port);
  if (getaddrinfo(host.c_str(), port_text.c_str(), &hints, &result) != 0) {
    return -1;
  }
  int fd = -1;
  for (addrinfo* it = result; it != nullptr; it = it->ai_next) {
    fd = ::socket(it->ai_family, it->ai_socktype, it->ai_protocol);
    if (fd < 0) {
      continue;
    }
    const int flags = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    const int rc = ::connect(fd, it->ai_addr, it->ai_addrlen);
    if (rc == 0 || errno == EINPROGRESS) {
      pollfd wait{};
      wait.fd = fd;
      wait.events = POLLOUT;
      if (::poll(&wait, 1, timeout_ms) == 1 && (wait.revents & POLLOUT) != 0) {
        int error = 0;
        socklen_t len = sizeof(error);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) == 0 && error == 0) {
          ::fcntl(fd, F_SETFL, flags);
          break;
        }
      }
    }
    ::close(fd);
    fd = -1;
  }
  freeaddrinfo(result);
  return fd;
}

bool port_open(const std::string& address) {
  std::string host;
  int port = 0;
  if (!split_host_port(address, host, port)) {
    return false;
  }
  const int fd = dial(host, port, 200);
  if (fd < 0) {
    return false;
  }
  ::close(fd);
  return true;
}

void wait_until_listening(const std::string& address) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(90);
  while (std::chrono::steady_clock::now() < deadline) {
    if (port_open(address)) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  throw std::runtime_error("timed out waiting for " + address);
}

Response exchange(const std::string& host, int port, const std::string& request) {
  const int fd = dial(host, port, 3000);
  if (fd < 0) {
    throw std::runtime_error("cannot connect to " + host + ":" + std::to_string(port));
  }
  std::size_t sent = 0;
  while (sent < request.size()) {
    const ssize_t n = ::send(fd, request.data() + sent, request.size() - sent, MSG_NOSIGNAL);
    if (n < 0) {
      ::close(fd);
      throw std::runtime_error("send failed");
    }
    sent += static_cast<std::size_t>(n);
  }
  std::string raw;
  char buffer[4096];
  while (true) {
    const ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);
    if (n == 0) {
      break;
    }
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      ::close(fd);
      throw std::runtime_error("recv failed");
    }
    raw.append(buffer, static_cast<std::size_t>(n));
  }
  ::close(fd);

  Response response;
  const auto line_end = raw.find("\r\n");
  const std::string status_line = raw.substr(0, line_end);
  const auto code_at = status_line.find(' ');
  if (code_at != std::string::npos) {
    response.status = std::stoi(status_line.substr(code_at + 1));
  }
  const auto body_at = raw.find("\r\n\r\n");
  if (body_at != std::string::npos) {
    response.body = raw.substr(body_at + 4);
  }
  return response;
}

Response http(const std::string& method, const std::string& path, const std::string& body) {
  std::ostringstream request;
  request << method << " " << path << " HTTP/1.1\r\n"
          << "Host: " << g_base_host << ":" << g_base_port << "\r\n"
          << "Connection: close\r\n";
  if (!body.empty()) {
    request << "Content-Type: application/json\r\n"
            << "Content-Length: " << body.size() << "\r\n";
  }
  request << "\r\n" << body;
  return exchange(g_base_host, g_base_port, request.str());
}

void ping() {
  const auto response = http("GET", "/ping", "");
  if (response.status != 200 || response.body.find("pong") == std::string::npos) {
    throw std::runtime_error("mercury /ping failed: " + std::to_string(response.status) + " " + response.body);
  }
}

int response_pid(const std::string& body) {
  const auto key = body.find("\"pid\"");
  if (key == std::string::npos) {
    return 0;
  }
  const auto colon = body.find(':', key);
  if (colon == std::string::npos) {
    return 0;
  }
  return std::stoi(body.substr(colon + 1));
}

int start_daemon(const Tuple& line, int cpu) {
  const std::string body = std::string("{\"daemon_id\":\"") + json_escape(line.id) + "\",\"exec_path\":\"" +
                           json_escape(line.path) + "\",\"cpu\":" + std::to_string(cpu) + "}";
  const auto response = http("POST", "/start", body);
  const int pid = response_pid(response.body);
  if (response.status != 200 || pid <= 0) {
    throw std::runtime_error("POST /start " + line.id + ": " + std::to_string(response.status) + " " + response.body);
  }
  return pid;
}

void kill_daemon(const std::string& id) {
  const auto response = http("POST", "/kill", std::string("{\"daemon_id\":\"") + json_escape(id) + "\"}");
  if (response.status != 200) {
    std::cerr << "kill " << id << ": " << response.status << " " << response.body << std::endl;
    return;
  }
  std::cout << "killed " << id << std::endl;
}

void stop_started() {
  std::vector<Tuple> lines;
  {
    std::lock_guard lock(g_mu);
    lines = g_started;
    g_started.clear();
  }
  for (auto it = lines.rbegin(); it != lines.rend(); ++it) {
    try {
      kill_daemon(it->id);
    } catch (const std::exception& error) {
      std::cerr << "kill " << it->id << ": " << error.what() << std::endl;
    }
  }
}

void stop_all(const std::vector<Tuple>& lines) {
  for (auto it = lines.rbegin(); it != lines.rend(); ++it) {
    kill_daemon(it->id);
  }
}

bool still_running(const std::string& path) {
  const auto response = http("POST", "/stat", std::string("{\"exec_path\":\"") + json_escape(path) + "\"}");
  return response.status == 200 && response.body.find("true") != std::string::npos;
}

void wait_until_running(const Tuple& line) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (std::chrono::steady_clock::now() < deadline) {
    if (still_running(line.path)) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  throw std::runtime_error(line.id + " is not running");
}

void usage() {
  std::cerr << "usage: operate [-mercury host:port] [-cpu N] [-file path] [-config path] [-graph path] start|stop|test|init\n";
}

std::string resolve_graph(const std::string& graph) {
  auto readable = [](const std::string& path) {
    std::ifstream in(path);
    return static_cast<bool>(in);
  };
  if (readable(graph)) {
    return graph;
  }
  const std::string rooted = benchmarks_root() + "/deathstar-socialNetwork/" + graph;
  if (readable(rooted)) {
    return rooted;
  }
  throw std::runtime_error("cannot read " + graph);
}

}  // namespace

int main(int argc, char** argv) {
  std::string mercury = "127.0.0.1:8080";
  int cpu = 0;
  std::string file = benchmarks_root() + "/mercury-social.txt";
  std::string config_path = benchmarks_root() + "/deathstar-socialNetwork/config/service-config.json";
  std::string graph;
  std::vector<std::string> args;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto need = [&](const char* name) {
      if (i + 1 >= argc) {
        die(std::string(name) + " needs a value");
      }
      return std::string(argv[++i]);
    };
    if (arg == "-mercury") {
      mercury = need("-mercury");
    } else if (arg == "-cpu") {
      cpu = std::stoi(need("-cpu"));
    } else if (arg == "-file") {
      file = need("-file");
    } else if (arg == "-config") {
      config_path = need("-config");
    } else if (arg == "-graph") {
      graph = need("-graph");
    } else if (arg == "-h" || arg == "--help") {
      usage();
      return 2;
    } else {
      args.push_back(arg);
    }
  }
  std::string command = "test";
  if (!args.empty()) {
    command = args[0];
  }
  if (args.size() > 1 || (command != "start" && command != "stop" && command != "test" && command != "init")) {
    usage();
    return 2;
  }

  try {
    if (!split_host_port(mercury, g_base_host, g_base_port)) {
      die("mercury address must be host:port");
    }
    const auto lines = parse_file(file);
    const auto config = read_file(config_path);
    ping();

    if (command == "stop") {
      stop_all(lines);
      return 0;
    }
    if (cpu < 1) {
      die("-cpu is required: pass a daemon CPU from the mercury command line");
    }
    if (command == "init") {
      if (graph.empty()) {
        graph = benchmarks_root() + "/deathstar-socialNetwork/datasets/social-graph/socfb-Reed98/socfb-Reed98.edges";
      }
      graph = resolve_graph(graph);
    }

    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &set, nullptr);
    std::thread([] {
      sigset_t wait;
      sigemptyset(&wait);
      sigaddset(&wait, SIGINT);
      sigaddset(&wait, SIGTERM);
      int sig = 0;
      sigwait(&wait, &sig);
      stop_started();
      std::_Exit(1);
    }).detach();

    for (const auto& line : lines) {
      const std::string address = listen_address(line, config);
      if (!address.empty() && port_open(address)) {
        stop_started();
        die(line.id + ": " + address + " is already in use");
      }
      const int pid = start_daemon(line, cpu);
      std::cout << "started " << basename_of(line.path) << " daemon_id=" << line.id << " pid " << pid << std::endl;
      {
        std::lock_guard lock(g_mu);
        g_started.push_back(line);
      }
      if (address.empty()) {
        wait_until_running(line);
        std::cout << "pass  " << line.id << " running" << std::endl;
      } else {
        wait_until_listening(address);
        std::cout << "pass  " << address << " " << basename_of(line.path) << std::endl;
      }
    }

    if (command == "start") {
      std::lock_guard lock(g_mu);
      std::cout << "left " << g_started.size() << " daemons running" << std::endl;
      return 0;
    }
    const auto count = g_started.size();
    if (command == "init") {
      init_graph(config, graph);
      stop_started();
      std::cout << count << " listening, graph loaded" << std::endl;
      return 0;
    }
    test_routes(config);
    stop_started();
    std::cout << count << " listening, 0 failed" << std::endl;
  } catch (const std::exception& error) {
    stop_started();
    die(error.what());
  }
  return 0;
}
