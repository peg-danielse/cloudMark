#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_URLSHORTENSERVICE_URLSHORTENHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_URLSHORTENSERVICE_URLSHORTENHANDLER_H_

#include <random>
#include <chrono>
#include <future>

#include <mongoc.h>
#include <libmemcached/memcached.h>
#include <libmemcached/util.h>
#include <bson/bson.h>

#include "../../gen-cpp/UrlShortenService.h"
#include "../../gen-cpp/social_network_types.h"
#include "../logger.h"
#include "../tracing.h"

#define HOSTNAME "http://short-url/"

namespace social_network {

class UrlShortenHandler : public UrlShortenServiceIf {
 public:
  UrlShortenHandler(memcached_pool_st *, mongoc_client_pool_t *, std::mutex *);
  ~UrlShortenHandler() override = default;

  void ComposeUrls(std::vector<Url> &, int64_t,
      const std::vector<std::string> &,
      const std::map<std::string, std::string> &) override;

  void GetExtendedUrls(std::vector<std::string> &, int64_t,
                       const std::vector<std::string> &,
                       const std::map<std::string, std::string> &) override ;

 private:
  memcached_pool_st *_memcached_client_pool;
  mongoc_client_pool_t *_mongodb_client_pool;
  static std::mt19937 _generator;
  std::uniform_int_distribution<int> _distribution;
  std::string _GenRandomStr(int length);
  std::mutex *_thread_lock;
};

std::mt19937 UrlShortenHandler::_generator = std::mt19937(std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count() % 0xffffffff);

UrlShortenHandler::UrlShortenHandler(
    memcached_pool_st *memcached_client_pool,
    mongoc_client_pool_t *mongodb_client_pool,
    std::mutex *thread_lock) {
  _memcached_client_pool = memcached_client_pool;
  _mongodb_client_pool = mongodb_client_pool;
  _thread_lock = thread_lock;
  _distribution = std::uniform_int_distribution<int>(0, 61);
}

std::string UrlShortenHandler::_GenRandomStr(int length) {
  const char char_map[] = "abcdefghijklmnopqrstuvwxyzABCDEF"
                    "GHIJKLMNOPQRSTUVWXYZ0123456789";
  std::string return_str;
  _thread_lock->lock();
  for (int i = 0; i < length; ++i) {
    return_str.append(1, char_map[_distribution(_generator)]);
  }
  _thread_lock->unlock();
  return return_str;
}
void UrlShortenHandler::ComposeUrls(
    std::vector<Url> &_return,
    int64_t req_id,
    const std::vector<std::string> &urls,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "compose_urls_server",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  std::vector<Url> target_urls;
  std::future<void> mongo_future;

  if (!urls.empty()) {
    for (auto &url : urls) {
      Url new_target_url;
      new_target_url.expanded_url = url;
      new_target_url.shortened_url = HOSTNAME +
          _GenRandomStr(10);
      target_urls.emplace_back(new_target_url);
    }

    mongo_future = std::async(
        std::launch::async, [&](){
          mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
              _mongodb_client_pool);
          if (!mongodb_client) {
            ServiceException se;
            se.errorCode = ErrorCode::SE_MONGODB_ERROR;
            se.message = "Failed to pop a client from MongoDB pool";
            throw se;
          }
          auto collection = mongoc_client_get_collection(
              mongodb_client, "url-shorten", "url-shorten");
          if (!collection) {
            ServiceException se;
            se.errorCode = ErrorCode::SE_MONGODB_ERROR;
            se.message = "Failed to create collection user from DB user";
            mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
            throw se;
          }

          auto mongo_span = opentracing::Tracer::Global()->StartSpan(
              "url_mongo_insert_client",
              { opentracing::ChildOf(&span->context()) });

          mongoc_bulk_operation_t *bulk;
          bson_t *doc;
          bson_error_t error;
          bson_t reply;
          bool ret;
          bulk = mongoc_collection_create_bulk_operation_with_opts(
              collection, nullptr);
          for (auto &url : target_urls) {
            doc = bson_new();
            BSON_APPEND_UTF8(doc, "shortened_url", url.shortened_url.c_str());
            BSON_APPEND_UTF8(doc, "expanded_url", url.expanded_url.c_str());
            mongoc_bulk_operation_insert (bulk, doc);
            bson_destroy(doc);
          }
          ret = mongoc_bulk_operation_execute (bulk, &reply, &error);
          if (!ret) {
            LOG(error) << "MongoDB error: "<< error.message;
            ServiceException se;
            se.errorCode = ErrorCode::SE_MONGODB_ERROR;
            se.message = "Failed to insert urls to MongoDB";
            bson_destroy (&reply);
            mongoc_bulk_operation_destroy(bulk);
            mongoc_collection_destroy(collection);
            mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
            throw se;
          }
          bson_destroy (&reply);
          mongoc_bulk_operation_destroy(bulk);
          mongoc_collection_destroy(collection);
          mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
          mongo_span->Finish();
        });

  }

  if (!urls.empty()) {
    try {
      mongo_future.get();
    } catch (...) {
      LOG(error) << "Failed to upload shortened urls from MongoDB";
      throw;
    }
  }

  _return = target_urls;
  span->Finish();

}

void UrlShortenHandler::GetExtendedUrls(
    std::vector<std::string> &_return,
    int64_t req_id,
    const std::vector<std::string> &shortened_urls,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "get_extended_urls_server",
      {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  _return.assign(shortened_urls.size(), "");
  if (shortened_urls.empty()) {
    span->Finish();
    return;
  }

  std::vector<char> resolved(shortened_urls.size(), 0);
  memcached_return_t memcached_rc;
  memcached_st *memcached_client =
      memcached_pool_pop(_memcached_client_pool, true, &memcached_rc);
  if (memcached_client) {
    for (std::size_t i = 0; i < shortened_urls.size(); ++i) {
      std::size_t value_length = 0;
      uint32_t flags = 0;
      memcached_return_t get_rc;
      char *value = memcached_get(
          memcached_client, shortened_urls[i].c_str(), shortened_urls[i].size(),
          &value_length, &flags, &get_rc);
      if (value != nullptr && get_rc == MEMCACHED_SUCCESS) {
        _return[i].assign(value, value_length);
        resolved[i] = 1;
      }
      free(value);
    }
    memcached_pool_push(_memcached_client_pool, memcached_client);
  }

  std::vector<std::size_t> missing;
  for (std::size_t i = 0; i < shortened_urls.size(); ++i) {
    if (!resolved[i]) {
      missing.push_back(i);
    }
  }
  if (missing.empty()) {
    span->Finish();
    return;
  }

  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(_mongodb_client_pool);
  if (!mongodb_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to pop a client from MongoDB pool";
    throw se;
  }
  auto collection = mongoc_client_get_collection(
      mongodb_client, "url-shorten", "url-shorten");
  if (!collection) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to create collection url-shorten from MongoDB";
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }

  bson_t *query = bson_new();
  bson_t query_child;
  bson_t query_list;
  const char *key;
  char buf[16];
  BSON_APPEND_DOCUMENT_BEGIN(query, "shortened_url", &query_child);
  BSON_APPEND_ARRAY_BEGIN(&query_child, "$in", &query_list);
  for (std::size_t i = 0; i < missing.size(); ++i) {
    bson_uint32_to_string(static_cast<uint32_t>(i), &key, buf, sizeof buf);
    BSON_APPEND_UTF8(&query_list, key, shortened_urls[missing[i]].c_str());
  }
  bson_append_array_end(&query_child, &query_list);
  bson_append_document_end(query, &query_child);

  std::map<std::string, std::string> found;
  mongoc_cursor_t *cursor =
      mongoc_collection_find_with_opts(collection, query, nullptr, nullptr);
  const bson_t *doc;
  while (mongoc_cursor_next(cursor, &doc)) {
    bson_iter_t shortened_iter;
    bson_iter_t expanded_iter;
    if (!bson_iter_init_find(&shortened_iter, doc, "shortened_url") ||
        !BSON_ITER_HOLDS_UTF8(&shortened_iter) ||
        !bson_iter_init_find(&expanded_iter, doc, "expanded_url") ||
        !BSON_ITER_HOLDS_UTF8(&expanded_iter)) {
      continue;
    }
    found.emplace(bson_iter_utf8(&shortened_iter, nullptr),
                  bson_iter_utf8(&expanded_iter, nullptr));
  }
  bson_error_t error;
  if (mongoc_cursor_error(cursor, &error)) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = error.message;
    bson_destroy(query);
    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }
  bson_destroy(query);
  mongoc_cursor_destroy(cursor);
  mongoc_collection_destroy(collection);
  mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);

  memcached_client =
      memcached_pool_pop(_memcached_client_pool, true, &memcached_rc);
  for (std::size_t index : missing) {
    auto it = found.find(shortened_urls[index]);
    if (it == found.end()) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
      se.message = "Cannot find shortened url of request " + std::to_string(req_id);
      if (memcached_client) {
        memcached_pool_push(_memcached_client_pool, memcached_client);
      }
      throw se;
    }
    _return[index] = it->second;
    if (memcached_client) {
      memcached_set(memcached_client, shortened_urls[index].c_str(),
                    shortened_urls[index].size(), it->second.c_str(),
                    it->second.size(), static_cast<time_t>(0),
                    static_cast<uint32_t>(0));
    }
  }
  if (memcached_client) {
    memcached_pool_push(_memcached_client_pool, memcached_client);
  }
  span->Finish();
}

}



#endif //SOCIAL_NETWORK_MICROSERVICES_SRC_URLSHORTENSERVICE_URLSHORTENHANDLER_H_
