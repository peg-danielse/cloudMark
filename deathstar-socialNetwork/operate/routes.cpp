#include "routes.h"

#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>

#include "ComposePostService.h"
#include "HomeTimelineService.h"
#include "MediaService.h"
#include "PostStorageService.h"
#include "SocialGraphService.h"
#include "TextService.h"
#include "UniqueIdService.h"
#include "UrlShortenService.h"
#include "UserMentionService.h"
#include "UserService.h"
#include "UserTimelineService.h"

namespace {

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::transport::TFramedTransport;
using apache::thrift::transport::TSocket;
using social_network::Creator;
using social_network::Media;
using social_network::Post;
using social_network::PostType;
using social_network::ServiceException;
using social_network::TextServiceReturn;
using social_network::Url;
using social_network::UserMention;

const char* kHost = "127.0.0.1";

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

struct Link {
  std::shared_ptr<TSocket> socket;
  std::shared_ptr<TFramedTransport> transport;
  std::shared_ptr<TBinaryProtocol> protocol;
};

template <class Client>
Client dial(Link& link, int port) {
  link.socket = std::make_shared<TSocket>(kHost, port);
  link.socket->setConnTimeout(3000);
  link.socket->setRecvTimeout(20000);
  link.socket->setSendTimeout(20000);
  link.transport = std::make_shared<TFramedTransport>(link.socket);
  link.protocol = std::make_shared<TBinaryProtocol>(link.transport);
  link.transport->open();
  return Client(link.protocol);
}

bool contains_id(const std::vector<int64_t>& ids, int64_t id) {
  for (const int64_t value : ids) {
    if (value == id) {
      return true;
    }
  }
  return false;
}

bool contains_text(const std::vector<Post>& posts, const std::string& text) {
  for (const Post& post : posts) {
    if (post.text == text) {
      return true;
    }
  }
  return false;
}

bool contains_post(const std::vector<Post>& posts, int64_t post_id) {
  for (const Post& post : posts) {
    if (post.post_id == post_id) {
      return true;
    }
  }
  return false;
}

std::string join_ids(const std::vector<int64_t>& ids) {
  std::ostringstream out;
  for (std::size_t i = 0; i < ids.size(); ++i) {
    if (i != 0) {
      out << ",";
    }
    out << ids[i];
  }
  return out.str();
}

}  // namespace

void test_routes(const std::string& config) {
  const int unique_id_port = json_port(config, "unique-id-service");
  const int media_port = json_port(config, "media-service");
  const int social_graph_port = json_port(config, "social-graph-service");
  const int post_storage_port = json_port(config, "post-storage-service");
  const int text_port = json_port(config, "text-service");
  const int user_port = json_port(config, "user-service");
  const int user_mention_port = json_port(config, "user-mention-service");
  const int user_timeline_port = json_port(config, "user-timeline-service");
  const int home_timeline_port = json_port(config, "home-timeline-service");
  const int url_shorten_port = json_port(config, "url-shorten-service");
  const int compose_port = json_port(config, "compose-post-service");

  const std::map<std::string, std::string> carrier;
  const int64_t stamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
  const int64_t alice_id = stamp * 10 + 1;
  const int64_t bob_id = stamp * 10 + 2;
  const int64_t graph_id = stamp * 10 + 3;
  const std::string alice = "a" + std::to_string(stamp);
  const std::string bob = "b" + std::to_string(stamp);
  const std::string carol = "c" + std::to_string(stamp);
  const std::string password = "secret";
  int64_t req = 1;

  int passed = 0;
  int failed = 0;
  auto pass = [&](const std::string& name) {
    ++passed;
    std::cout << "pass  " << name << std::endl;
  };
  auto fail = [&](const std::string& name, const std::string& detail) {
    ++failed;
    std::cout << "FAIL  " << name << "\n      " << detail << std::endl;
  };
  auto check = [&](const std::string& name, auto&& fn) {
    try {
      fn();
      pass(name);
    } catch (const ServiceException& ex) {
      fail(name, ex.message);
    } catch (const std::exception& ex) {
      fail(name, ex.what());
    }
  };

  Link unique_link;
  Link media_link;
  Link graph_link;
  Link post_link;
  Link text_link;
  Link user_link;
  Link mention_link;
  Link user_timeline_link;
  Link home_timeline_link;
  Link url_link;
  Link compose_link;

  social_network::UniqueIdServiceClient unique_id = dial<social_network::UniqueIdServiceClient>(unique_link, unique_id_port);
  social_network::MediaServiceClient media = dial<social_network::MediaServiceClient>(media_link, media_port);
  social_network::SocialGraphServiceClient graph = dial<social_network::SocialGraphServiceClient>(graph_link, social_graph_port);
  social_network::PostStorageServiceClient posts = dial<social_network::PostStorageServiceClient>(post_link, post_storage_port);
  social_network::TextServiceClient text = dial<social_network::TextServiceClient>(text_link, text_port);
  social_network::UserServiceClient users = dial<social_network::UserServiceClient>(user_link, user_port);
  social_network::UserMentionServiceClient mentions = dial<social_network::UserMentionServiceClient>(mention_link, user_mention_port);
  social_network::UserTimelineServiceClient user_timeline =
      dial<social_network::UserTimelineServiceClient>(user_timeline_link, user_timeline_port);
  social_network::HomeTimelineServiceClient home_timeline =
      dial<social_network::HomeTimelineServiceClient>(home_timeline_link, home_timeline_port);
  social_network::UrlShortenServiceClient urls = dial<social_network::UrlShortenServiceClient>(url_link, url_shorten_port);
  social_network::ComposePostServiceClient compose = dial<social_network::ComposePostServiceClient>(compose_link, compose_port);

  int64_t post_id = 0;
  check("UniqueId.ComposeUniqueId 127.0.0.1:" + std::to_string(unique_id_port), [&] {
    post_id = unique_id.ComposeUniqueId(req++, PostType::POST, carrier);
    if (post_id <= 0) {
      throw std::runtime_error("post id " + std::to_string(post_id));
    }
  });

  check("Media.ComposeMedia 127.0.0.1:" + std::to_string(media_port), [&] {
    std::vector<Media> created;
    media.ComposeMedia(created, req++, {"png"}, {7}, carrier);
    if (created.size() != 1 || created[0].media_id != 7 || created[0].media_type != "png") {
      throw std::runtime_error("media count " + std::to_string(created.size()));
    }
  });

  check("User.RegisterUserWithId 127.0.0.1:" + std::to_string(user_port), [&] {
    users.RegisterUserWithId(req++, "Ada", "Lovelace", alice, password, alice_id, carrier);
    users.RegisterUserWithId(req++, "Alan", "Turing", bob, password, bob_id, carrier);
  });

  int64_t carol_id = 0;
  check("User.RegisterUser 127.0.0.1:" + std::to_string(user_port), [&] {
    users.RegisterUser(req++, "Grace", "Hopper", carol, password, carrier);
  });
  check("User.GetUserId 127.0.0.1:" + std::to_string(user_port), [&] {
    carol_id = users.GetUserId(req++, carol, carrier);
    if (carol_id <= 0) {
      throw std::runtime_error("user id " + std::to_string(carol_id));
    }
  });
  check("User.Login 127.0.0.1:" + std::to_string(user_port), [&] {
    std::string token;
    users.Login(token, req++, alice, password, carrier);
    if (token.empty()) {
      throw std::runtime_error("empty login token");
    }
  });
  check("User.ComposeCreatorWithUserId 127.0.0.1:" + std::to_string(user_port), [&] {
    Creator creator;
    users.ComposeCreatorWithUserId(creator, req++, alice_id, alice, carrier);
    if (creator.user_id != alice_id || creator.username != alice) {
      throw std::runtime_error(creator.username);
    }
  });
  check("User.ComposeCreatorWithUsername 127.0.0.1:" + std::to_string(user_port), [&] {
    Creator creator;
    users.ComposeCreatorWithUsername(creator, req++, alice, carrier);
    if (creator.user_id != alice_id || creator.username != alice) {
      throw std::runtime_error("creator " + std::to_string(creator.user_id) + " " + creator.username);
    }
  });

  check("SocialGraph.InsertUser 127.0.0.1:" + std::to_string(social_graph_port), [&] {
    graph.InsertUser(req++, graph_id, carrier);
  });
  check("SocialGraph.Follow 127.0.0.1:" + std::to_string(social_graph_port), [&] {
    graph.Follow(req++, alice_id, bob_id, carrier);
  });
  check("SocialGraph.GetFollowers 127.0.0.1:" + std::to_string(social_graph_port), [&] {
    std::vector<int64_t> followers;
    graph.GetFollowers(followers, req++, bob_id, carrier);
    if (!contains_id(followers, alice_id)) {
      throw std::runtime_error("followers " + join_ids(followers));
    }
  });
  check("SocialGraph.GetFollowees 127.0.0.1:" + std::to_string(social_graph_port), [&] {
    std::vector<int64_t> followees;
    graph.GetFollowees(followees, req++, alice_id, carrier);
    if (!contains_id(followees, bob_id)) {
      throw std::runtime_error("followees " + join_ids(followees));
    }
  });
  check("SocialGraph.Unfollow 127.0.0.1:" + std::to_string(social_graph_port), [&] {
    graph.Unfollow(req++, alice_id, bob_id, carrier);
    std::vector<int64_t> followees;
    graph.GetFollowees(followees, req++, alice_id, carrier);
    if (contains_id(followees, bob_id)) {
      throw std::runtime_error("still following " + join_ids(followees));
    }
  });
  check("SocialGraph.FollowWithUsername 127.0.0.1:" + std::to_string(social_graph_port), [&] {
    graph.FollowWithUsername(req++, alice, bob, carrier);
    std::vector<int64_t> followees;
    graph.GetFollowees(followees, req++, alice_id, carrier);
    if (!contains_id(followees, bob_id)) {
      throw std::runtime_error("followees " + join_ids(followees));
    }
  });
  check("SocialGraph.UnfollowWithUsername 127.0.0.1:" + std::to_string(social_graph_port), [&] {
    graph.UnfollowWithUsername(req++, alice, bob, carrier);
    std::vector<int64_t> followees;
    graph.GetFollowees(followees, req++, alice_id, carrier);
    if (contains_id(followees, bob_id)) {
      throw std::runtime_error("still following " + join_ids(followees));
    }
  });

  check("UserMention.ComposeUserMentions 127.0.0.1:" + std::to_string(user_mention_port), [&] {
    std::vector<UserMention> found;
    mentions.ComposeUserMentions(found, req++, {alice}, carrier);
    if (found.size() != 1 || found[0].user_id != alice_id || found[0].username != alice) {
      throw std::runtime_error("mentions " + std::to_string(found.size()));
    }
  });

  std::string shortened;
  const std::string expanded = "https://example.com/" + alice;
  check("UrlShorten.ComposeUrls 127.0.0.1:" + std::to_string(url_shorten_port), [&] {
    std::vector<Url> created;
    urls.ComposeUrls(created, req++, {expanded}, carrier);
    if (created.size() != 1 || created[0].expanded_url != expanded ||
        created[0].shortened_url.compare(0, 17, "http://short-url/") != 0) {
      throw std::runtime_error(created.empty() ? "no url" : created[0].shortened_url);
    }
    shortened = created[0].shortened_url;
  });
  check("UrlShorten.GetExtendedUrls 127.0.0.1:" + std::to_string(url_shorten_port), [&] {
    std::vector<std::string> extended;
    urls.GetExtendedUrls(extended, req++, {shortened}, carrier);
    if (extended.size() != 1 || extended[0] != expanded) {
      throw std::runtime_error(extended.empty() ? "no url" : extended[0]);
    }
  });

  const std::string mention_text = "hello @" + alice;
  check("Text.ComposeText 127.0.0.1:" + std::to_string(text_port), [&] {
    TextServiceReturn composed;
    text.ComposeText(composed, req++, mention_text, carrier);
    if (composed.text != mention_text || composed.user_mentions.size() != 1 ||
        composed.user_mentions[0].user_id != alice_id) {
      throw std::runtime_error(composed.text);
    }
  });

  const std::string stored_text = "stored " + alice;
  const int64_t stored_at = stamp;
  check("PostStorage.StorePost 127.0.0.1:" + std::to_string(post_storage_port), [&] {
    if (post_id <= 0) {
      throw std::runtime_error("no post id");
    }
    Post post;
    post.post_id = post_id;
    post.creator.user_id = alice_id;
    post.creator.username = alice;
    post.req_id = req++;
    post.text = stored_text;
    post.timestamp = stored_at;
    post.post_type = PostType::POST;
    posts.StorePost(req++, post, carrier);
  });
  check("PostStorage.ReadPost 127.0.0.1:" + std::to_string(post_storage_port), [&] {
    Post post;
    posts.ReadPost(post, req++, post_id, carrier);
    if (post.post_id != post_id || post.text != stored_text || post.creator.user_id != alice_id) {
      throw std::runtime_error(post.text);
    }
  });
  check("PostStorage.ReadPosts 127.0.0.1:" + std::to_string(post_storage_port), [&] {
    std::vector<Post> read;
    posts.ReadPosts(read, req++, {post_id}, carrier);
    if (read.size() != 1 || read[0].post_id != post_id || read[0].text != stored_text) {
      throw std::runtime_error("posts " + std::to_string(read.size()));
    }
  });

  check("UserTimeline.WriteUserTimeline 127.0.0.1:" + std::to_string(user_timeline_port), [&] {
    user_timeline.WriteUserTimeline(req++, post_id, alice_id, stored_at, carrier);
  });
  check("UserTimeline.ReadUserTimeline 127.0.0.1:" + std::to_string(user_timeline_port), [&] {
    std::vector<Post> timeline;
    user_timeline.ReadUserTimeline(timeline, req++, alice_id, 0, 10, carrier);
    if (!contains_post(timeline, post_id)) {
      throw std::runtime_error("timeline posts " + std::to_string(timeline.size()));
    }
  });

  check("SocialGraph.Follow follower 127.0.0.1:" + std::to_string(social_graph_port), [&] {
    graph.Follow(req++, bob_id, alice_id, carrier);
  });
  check("HomeTimeline.WriteHomeTimeline 127.0.0.1:" + std::to_string(home_timeline_port), [&] {
    home_timeline.WriteHomeTimeline(req++, post_id, alice_id, stored_at, {}, carrier);
  });
  check("HomeTimeline.ReadHomeTimeline 127.0.0.1:" + std::to_string(home_timeline_port), [&] {
    std::vector<Post> timeline;
    home_timeline.ReadHomeTimeline(timeline, req++, bob_id, 0, 10, carrier);
    if (!contains_post(timeline, post_id)) {
      throw std::runtime_error("home posts " + std::to_string(timeline.size()));
    }
  });

  const std::string composed_text = "composed @" + bob;
  check("ComposePost.ComposePost 127.0.0.1:" + std::to_string(compose_port), [&] {
    compose.ComposePost(req++, alice, alice_id, composed_text, {11}, {"png"}, PostType::POST, carrier);
  });
  check("UserTimeline.ReadUserTimeline composed 127.0.0.1:" + std::to_string(user_timeline_port), [&] {
    std::vector<Post> timeline;
    user_timeline.ReadUserTimeline(timeline, req++, alice_id, 0, 10, carrier);
    if (!contains_text(timeline, composed_text)) {
      throw std::runtime_error("timeline posts " + std::to_string(timeline.size()));
    }
  });
  check("HomeTimeline.ReadHomeTimeline composed 127.0.0.1:" + std::to_string(home_timeline_port), [&] {
    std::vector<Post> timeline;
    home_timeline.ReadHomeTimeline(timeline, req++, bob_id, 0, 10, carrier);
    if (!contains_text(timeline, composed_text)) {
      throw std::runtime_error("home posts " + std::to_string(timeline.size()));
    }
  });

  std::cout << passed << " passed, " << failed << " failed" << std::endl;
  if (failed != 0) {
    throw std::runtime_error(std::to_string(failed) + " route checks failed");
  }
}
