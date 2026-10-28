#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_UTILS_THRIFT_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_UTILS_THRIFT_H_

#include <memory>
#include <sstream>
#include <string>
#include <nlohmann/json.hpp>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/THttpServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TSSLServerSocket.h>
#include <thrift/transport/TTransport.h>

namespace social_network{
using json = nlohmann::json;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::transport::THttpServer;
using apache::thrift::transport::THttpTransport;
using apache::thrift::transport::TTransport;
using apache::thrift::transport::TServerSocket;
using apache::thrift::transport::TSSLServerSocket;
using apache::thrift::transport::TSSLSocketFactory;
using apache::thrift::transport::TTransportFactory;

// Present when service-config.json names an OpenFaaS gateway. Servers then
// speak Thrift over HTTP so of-watchdog can proxy the gateway to them.
inline bool openfaas_gateway_enabled(const json &config_json) {
  if (!config_json.contains("openfaas") || !config_json["openfaas"].is_object()) {
    return false;
  }
  const json &openfaas = config_json["openfaas"];
  return openfaas.contains("gateway") && openfaas["gateway"].is_string() &&
         !openfaas["gateway"].get<std::string>().empty();
}

// of-watchdog reuses HTTP connections. THttpServer answers one request and
// then closes, so Keep-Alive makes the next proxied call fail with EPIPE.
class OpenFaaSHttpServer : public THttpServer {
 public:
  explicit OpenFaaSHttpServer(std::shared_ptr<TTransport> transport)
      : THttpServer(transport) {}

  void flush() override {
    uint8_t *buf = nullptr;
    uint32_t len = 0;
    writeBuffer_.getBuffer(&buf, &len);
    std::ostringstream header;
    header << "HTTP/1.1 200 OK" << THttpTransport::CRLF
           << "Content-Type: application/x-thrift" << THttpTransport::CRLF
           << "Content-Length: " << len << THttpTransport::CRLF
           << "Connection: close" << THttpTransport::CRLF
           << THttpTransport::CRLF;
    const std::string headers = header.str();
    std::string payload;
    payload.reserve(headers.size() + len);
    payload.append(headers);
    payload.append(reinterpret_cast<const char *>(buf), len);
    transport_->write(reinterpret_cast<const uint8_t *>(payload.data()),
                      static_cast<uint32_t>(payload.size()));
    transport_->flush();
    writeBuffer_.resetBuffer();
    readHeaders_ = true;
  }
};

class OpenFaaSHttpServerTransportFactory : public TTransportFactory {
 public:
  std::shared_ptr<TTransport> getTransport(std::shared_ptr<TTransport> trans) override {
    return std::make_shared<OpenFaaSHttpServer>(trans);
  }
};

inline std::shared_ptr<TTransportFactory> get_transport_factory(const json &config_json) {
  if (openfaas_gateway_enabled(config_json)) {
    return std::make_shared<OpenFaaSHttpServerTransportFactory>();
  }
  return std::make_shared<TFramedTransportFactory>();
}

std::shared_ptr<TServerSocket> get_server_socket(const json &config_json, const std::string &address, int port) {
  bool ssl_enabled = config_json["ssl"]["enabled"];
  if (ssl_enabled) {
    std::string cert_path = config_json["ssl"]["serverCertPath"];
    std::string key_path = config_json["ssl"]["serverKeyPath"];
    std::string ca_path = config_json["ssl"]["caPath"];
    std::string ciphers = config_json["ssl"]["ciphers"];

    std::shared_ptr<TSSLSocketFactory> ssl_socket_factory;
    ssl_socket_factory = std::make_shared<TSSLSocketFactory>();
    ssl_socket_factory->loadCertificate(cert_path.c_str());
    ssl_socket_factory->loadPrivateKey(key_path.c_str());
    ssl_socket_factory->ciphers(ciphers);
    // if (config_json["ssl"]["verifyClient"]) {
    //   ssl_socket_factory->loadTrustedCertificates(ca_path.c_str());
    //   ssl_socket_factory->authenticate(true);
    // }
    return std::make_shared<TSSLServerSocket>(address, port, ssl_socket_factory);
  }
  return std::make_shared<TServerSocket>(address, port);
};

} //namespace social_network

#endif //SOCIAL_NETWORK_MICROSERVICES_SRC_UTILS_THRIFT_H_
