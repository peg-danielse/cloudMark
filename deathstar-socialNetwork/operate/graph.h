#ifndef SOCIAL_NETWORK_OPERATE_GRAPH_H
#define SOCIAL_NETWORK_OPERATE_GRAPH_H

#include <string>

// init_graph registers every node in the edge file, then follows every edge.
// A sibling .nodes file supplies the node count. Users that are already
// registered, and edges that are already stored, are left in place.
// Prints one pass line and throws if a call fails or a followee list is short.
void init_graph(const std::string& config, const std::string& graph_path);

#endif
