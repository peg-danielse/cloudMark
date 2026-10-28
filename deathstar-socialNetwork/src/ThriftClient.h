#ifndef SOCIAL_NETWORK_MICROSERVICES_THRIFTCLIENT_H
#define SOCIAL_NETWORK_MICROSERVICES_THRIFTCLIENT_H

#include <memory>
#include <string>
#include <thread>
#include <iostream>
#include <chrono>
#include <boost/log/trivial.hpp>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/THttpClient.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <nlohmann/json.hpp>
#include <cstdlib>
#include <sstream>
#include "logger.h"
#include "GenericClient.h"
#include "localhost.h"


namespace social_network {

using apache::thrift::protocol::TProtocol;
using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::transport::TFramedTransport;
using apache::thrift::transport::THttpClient;
using apache::thrift::transport::THttpTransport;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TSSLSocketFactory;
using apache::thrift::transport::TTransport;
using apache::thrift::transport::TTransportException;
using apache::thrift::TException;
using json = nlohmann::json;

inline std::string base64_encode(const std::string &input) {
  static const char table[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string out;
  out.reserve(((input.size() + 2) / 3) * 4);
  unsigned value = 0;
  int bits = -6;
  for (unsigned char c : input) {
    value = (value << 8) + c;
    bits += 8;
    while (bits >= 0) {
      out.push_back(table[(value >> bits) & 0x3f]);
      bits -= 6;
    }
  }
  if (bits > -6) {
    out.push_back(table[((value << 8) >> (bits + 8)) & 0x3f]);
  }
  while (out.size() % 4) {
    out.push_back('=');
  }
  return out;
}

inline std::string openfaas_basic_auth() {
  const char *user = std::getenv("OPENFAAS_GATEWAY_USER");
  if (user == nullptr || user[0] == '\0') {
    return "";
  }
  const char *password = std::getenv("OPENFAAS_GATEWAY_PASSWORD");
  return base64_encode(std::string(user) + ":" + (password ? password : ""));
}

inline bool thrift_client_via_gateway(const json &config_json) {
  if (!config_json.contains("openfaas") || !config_json["openfaas"].is_object()) {
    return false;
  }
  const json &openfaas = config_json["openfaas"];
  return openfaas.contains("gateway") && openfaas["gateway"].is_string() &&
         !openfaas["gateway"].get<std::string>().empty();
}

// THttpClient has no header hook in Thrift 0.12. The gateway requires Basic
// auth, so flush() is the same request with an Authorization line added.
class OpenFaaSHttpClient : public THttpClient {
 public:
  OpenFaaSHttpClient(const std::string &host, int port, const std::string &path,
                     const std::string &authorization)
      : THttpClient(host, port, path), authorization_(authorization), port_(port) {}

  void flush() override {
    uint8_t *buf = nullptr;
    uint32_t len = 0;
    writeBuffer_.getBuffer(&buf, &len);

    // One write, and close afterwards. The pool reuses this object, and the
    // gateway closes idle keep-alive sockets; the next send() then fails with
    // EPIPE before the gateway ever sees the request.
    std::ostringstream header;
    header << "POST " << path_ << " HTTP/1.1" << THttpTransport::CRLF
           << "Host: " << host_ << THttpTransport::CRLF
           << "Content-Type: application/x-thrift" << THttpTransport::CRLF
           << "Content-Length: " << len << THttpTransport::CRLF
           << "Accept: application/x-thrift" << THttpTransport::CRLF
           << "User-Agent: Thrift/0.12.0 (C++/THttpClient)" << THttpTransport::CRLF;
    if (!authorization_.empty()) {
      header << "Authorization: Basic " << authorization_ << THttpTransport::CRLF;
    }
    header << "Connection: close" << THttpTransport::CRLF << THttpTransport::CRLF;
    const std::string headers = header.str();
    std::string payload;
    payload.reserve(headers.size() + len);
    payload.append(headers);
    payload.append(reinterpret_cast<const char *>(buf), len);
    writeBuffer_.resetBuffer();
    readHeaders_ = true;

    auto send = [&]() {
      if (!transport_->isOpen()) {
        transport_->open();
      }
      transport_->write(reinterpret_cast<const uint8_t *>(payload.data()),
                        static_cast<uint32_t>(payload.size()));
      transport_->flush();
    };
    try {
      send();
    } catch (const TTransportException &) {
      try {
        transport_->close();
      } catch (const TException &) {
      }
      send();
    }
  }

 private:
  std::string authorization_;
  int port_;
};

template<class TThriftClient>
class ThriftClient : public GenericClient {
 public:
  ThriftClient(const std::string &addr, int port);
  ThriftClient(const std::string &addr, int port, int keepalive_ms, const json &config_json);

  ThriftClient(const ThriftClient &) = delete;
  ThriftClient &operator=(const ThriftClient &) = delete;
  ThriftClient(ThriftClient<TThriftClient> &&) = default;
  ThriftClient &operator=(ThriftClient &&) = default;

  ~ThriftClient() override;

  TThriftClient *GetClient() const;

  void Connect() override;
  void Disconnect() override;
  bool IsConnected() override;

 private:
  TThriftClient *_client;

  std::shared_ptr<TSocket> _socket;
  std::shared_ptr<TTransport> _transport;
  std::shared_ptr<TProtocol> _protocol;
  // Gateway HTTP sockets are not reused. A pooled client otherwise writes
  // into a connection the gateway has already closed.
  bool _fresh_connection{false};
};

template<class TThriftClient>
ThriftClient<TThriftClient>::ThriftClient(
    const std::string &, int port) {
  _addr = kDialHost;
  _port = port;
  _socket = std::shared_ptr<TSocket>(new TSocket(_addr, port));
  _socket->setKeepAlive(true);
  _transport = std::shared_ptr<TTransport>(new TFramedTransport(_socket));
  _protocol = std::shared_ptr<TProtocol>(new TBinaryProtocol(_transport));
  _client = new TThriftClient(_protocol);
  _connect_timestamp = 0;
  _keepalive_ms = 0;
}

template <class TThriftClient>
ThriftClient<TThriftClient>::ThriftClient(
    const std::string &, int port, int keepalive_ms, const json &config_json) {
  _addr = kDialHost;
  _port = port;
  bool ssl_enabled = config_json["ssl"]["enabled"];

  if (ssl_enabled) {
    std::string ca_path = config_json["ssl"]["caPath"];
    std::string ciphers = config_json["ssl"]["ciphers"];

    std::shared_ptr<TSSLSocketFactory> factory;
    factory = std::make_shared<TSSLSocketFactory>();
    factory->ciphers(ciphers);
    factory->loadTrustedCertificates(ca_path.c_str());

    // if (config_json["ssl"]["verifyClient"]) {
    //   std::string cert_path = config_json["ssl"]["clientCertPath"];
    //   std::string key_path = config_json["ssl"]["clientKeyPath"];
    //   factory->loadCertificate(cert_path.c_str());
    //   factory->loadPrivateKey(key_path.c_str());
    // }
    // Need verify server
    factory->authenticate(true);
    _socket = factory->createSocket(_addr, port);
  } else {
    _socket = std::shared_ptr<TSocket>(new TSocket(_addr, port));
  }
  _socket->setKeepAlive(true);
  _transport = std::shared_ptr<TTransport>(new TFramedTransport(_socket));
  _protocol = std::shared_ptr<TProtocol>(new TBinaryProtocol(_transport));
  _client = new TThriftClient(_protocol);
  _connect_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
  _keepalive_ms = keepalive_ms;
}

template<class TThriftClient>
ThriftClient<TThriftClient>::~ThriftClient() {
  Disconnect();
  delete _client;
}

template<class TThriftClient>
TThriftClient *ThriftClient<TThriftClient>::GetClient() const {
  return _client;
}

template<class TThriftClient>
bool ThriftClient<TThriftClient>::IsConnected() {
  return _transport->isOpen();
}

template<class TThriftClient>
void ThriftClient<TThriftClient>::Connect() {
  if (_fresh_connection && IsConnected()) {
    try {
      _transport->close();
    } catch (const TException &) {
    }
  }
  if (!IsConnected()) {
    try {
      _transport->open();
    } catch (TException &tx) {
      throw tx;
    }
  }
}

template<class TThriftClient>
void ThriftClient<TThriftClient>::Disconnect() {
  if (IsConnected()) {
    try {
      _transport->close();
    } catch (TException &tx) {
      throw tx;
    }
  }
}

} // namespace social_network


#endif //SOCIAL_NETWORK_MICROSERVICES_THRIFTCLIENT_H
