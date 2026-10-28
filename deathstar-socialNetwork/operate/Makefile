CXX = g++
GEN = ../gen-cpp
OBJ = obj
CXXFLAGS = -std=c++20 -Wall -Wextra -Wno-unused-parameter -O2 -pthread -I$(GEN)
LDLIBS = -lthrift

LOCAL = operate.cpp routes.cpp graph.cpp
GEN_SRCS = social_network_types.cpp UniqueIdService.cpp TextService.cpp \
	UserService.cpp ComposePostService.cpp PostStorageService.cpp \
	HomeTimelineService.cpp UserTimelineService.cpp SocialGraphService.cpp \
	UserMentionService.cpp UrlShortenService.cpp MediaService.cpp

OBJS = $(addprefix $(OBJ)/, $(LOCAL:.cpp=.o) $(GEN_SRCS:.cpp=.o))

operate: $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(LDLIBS)

$(OBJ)/%.o: %.cpp graph.h routes.h | $(OBJ)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJ)/%.o: $(GEN)/%.cpp | $(OBJ)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJ):
	mkdir -p $(OBJ)

.PHONY: clean
clean:
	rm -f operate
	rm -rf $(OBJ)
