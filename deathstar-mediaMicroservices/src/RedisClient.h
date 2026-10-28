#ifndef MEDIA_MICROSERVICES_REDISCLIENT_H
#define MEDIA_MICROSERVICES_REDISCLIENT_H

#include <chrono>
#include <future>
#include <string>
#include <cpp_redis/cpp_redis>

#include "../gen-cpp/media_service_types.h"
#include "ClientPool.h"
#include "logger.h"
#include "GenericClient.h"

namespace media_service {

class RedisClient : public GenericClient {
 public:
  RedisClient(const std::string &addr, int port);
  RedisClient(const RedisClient &) = delete;
  RedisClient & operator=(const RedisClient &) = delete;
  RedisClient(RedisClient &&) = default;
  RedisClient & operator=(RedisClient &&) = default;

  ~RedisClient() override ;

  cpp_redis::client *GetClient() const;

  void Connect() override ;
  void Disconnect() override ;
  void KeepAlive() override ;
  void KeepAlive(int timeout_ms) override ;
  bool IsConnected() override ;

 private:
  cpp_redis::client * _client;
};

RedisClient::RedisClient(const std::string &addr, int port) {
  _addr = addr;
  _port = port;
  _client = new cpp_redis::client();
}

RedisClient::~RedisClient() {
  Disconnect();
  delete _client;
}

cpp_redis::client* RedisClient::GetClient() const {
  return _client;
}

void RedisClient::Connect() {
  if (!IsConnected()) {
    _client->connect(_addr, _port, [](const std::string& host, std::size_t port,
        cpp_redis::client::connect_state status) {
      if (status == cpp_redis::client::connect_state::dropped) {
        LOG(error) << "Failed to connect " << host << ":" << port;
      }
    });
  }
}

void RedisClient::Disconnect() {
  if (IsConnected()) {
    _client->disconnect();
  }
}

bool RedisClient::IsConnected() {
  return _client->is_connected();
}

void RedisClient::KeepAlive() {

}

void RedisClient::KeepAlive(int timeout_ms) {

}

// A checked-out client goes back to the pool on every exit, including exceptions.
class RedisCheckout {
 public:
  explicit RedisCheckout(ClientPool<RedisClient>* pool) : pool_(pool), client_(nullptr) {}
  RedisCheckout(const RedisCheckout&) = delete;
  RedisCheckout& operator=(const RedisCheckout&) = delete;
  ~RedisCheckout() { release(); }

  void reset(RedisClient* client) { release(); client_ = client; }
  RedisClient* client() const { return client_; }
  cpp_redis::client* redis() const { return client_->GetClient(); }
  // The reply did not arrive. Drop the connection so the next Pop opens a new one.
  void discard() {
    if (client_ == nullptr) {
      return;
    }
    client_->Disconnect();
    pool_->Remove(client_);
    client_ = nullptr;
  }

 private:
  void release() {
    if (client_ != nullptr) {
      pool_->Push(client_);
      client_ = nullptr;
    }
  }
  ClientPool<RedisClient>* pool_;
  RedisClient* client_;
};

// cpp_redis sync_commit(duration) returns either way and does not say which. The reply
// future is ready only when the wait actually finished.
inline constexpr std::chrono::milliseconds kRedisRoundTripTimeout{500};

[[noreturn]] inline void ThrowRedisTimeout(const char* message) {
  LOG(error) << message;
  ServiceException error;
  error.errorCode = ErrorCode::SE_REDIS_ERROR;
  error.message = message;
  throw error;
}

inline void DropTimedOut(RedisCheckout& held, std::future<cpp_redis::reply>& reply) {
  if (held.client() != nullptr) {
    held.client()->Disconnect();
    // Disconnect completes the pending reply on another thread before the client can be deleted.
    reply.wait_for(std::chrono::milliseconds(50));
  }
  held.discard();
}

inline cpp_redis::reply RedisSyncCommit(RedisCheckout& held, std::future<cpp_redis::reply>& reply) {
  held.redis()->sync_commit(kRedisRoundTripTimeout);
  if (reply.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
    DropTimedOut(held, reply);
    ThrowRedisTimeout("Redis sync_commit timed out");
  }
  return reply.get();
}

// commit() returns before the reply. Waiting here is what holds the checked-out client.
inline cpp_redis::reply RedisWait(RedisCheckout& held, std::future<cpp_redis::reply>& reply) {
  if (reply.wait_for(kRedisRoundTripTimeout) != std::future_status::ready) {
    DropTimedOut(held, reply);
    ThrowRedisTimeout("Redis reply timed out");
  }
  return reply.get();
}

} // mediua_service

#endif //MEDIA_MICROSERVICES_REDISCLIENT_H
