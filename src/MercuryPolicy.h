#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_MERCURYPOLICY_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_MERCURYPOLICY_H_

// Degrade (Mercury's degrade system, Mercury-zeroctl SPEC.md "degrade"): when MERCURY_POLICY names a file, a
// service polls it every 200 ms. It holds {"disabled": ["follow", ...]}: a handler of a disabled route throws a
// ServiceException at once, so an overloaded system sheds optional work before it reaches any backend. The social
// network has no frontend here (the load generator calls the services over Thrift), so the services that serve the
// optional routes shed them, and only in that route's methods: compose-post's own calls into the same service go on.
// A missing or unreadable file disables nothing. Without MERCURY_POLICY nothing changes.

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>

#include "../gen-cpp/social_network_types.h"
#include "logger.h"

namespace social_network {

class MercuryPolicy {
 public:
  static MercuryPolicy &Get() {
    static MercuryPolicy policy;
    return policy;
  }

  // Throws a ServiceException when route is disabled.
  void Check(const std::string &route) {
    if (_path.empty()) {
      return;
    }
    std::lock_guard<std::mutex> lock(_mu);
    if (_disabled.count(route)) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
      se.message = "degraded: " + route + " is disabled under load";
      throw se;
    }
  }

 private:
  MercuryPolicy() {
    const char *path = std::getenv("MERCURY_POLICY");
    if (!path || !*path) {
      return;
    }
    _path = path;
    LOG(info) << "degrade: following " << _path;
    std::thread([this] {
      std::string last;
      for (;;) {
        std::set<std::string> set = Read();
        std::string now;
        for (const auto &r : set) {
          now += (now.empty() ? "" : ",") + r;
        }
        if (now != last) {
          LOG(info) << "degrade: disabled routes [" << now << "]";
          last = now;
        }
        {
          std::lock_guard<std::mutex> lock(_mu);
          _disabled.swap(set);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
      }
    }).detach();
  }

  // The quoted names inside "disabled": [...]; Mercury writes nothing else into the file.
  std::set<std::string> Read() const {
    std::set<std::string> out;
    std::ifstream in(_path);
    if (!in) {
      return out;
    }
    std::stringstream buf;
    buf << in.rdbuf();
    const std::string text = buf.str();
    auto at = text.find("\"disabled\"");
    if (at == std::string::npos || (at = text.find('[', at)) == std::string::npos) {
      return out;
    }
    const auto end = text.find(']', at);
    for (auto q = text.find('"', at); q != std::string::npos && q < end;) {
      const auto close = text.find('"', q + 1);
      if (close == std::string::npos || close > end) {
        break;
      }
      out.insert(text.substr(q + 1, close - q - 1));
      q = text.find('"', close + 1);
    }
    return out;
  }

  std::string _path;
  std::mutex _mu;
  std::set<std::string> _disabled;
};

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_SRC_MERCURYPOLICY_H_
