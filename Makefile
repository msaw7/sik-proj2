CXX := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -O2
LIBS := -lssl -lcrypto
TARGET := sikradio
SRCS := main.cpp err.cpp network_util.cpp
OBJS := $(SRCS:.cpp=.o)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(OBJS) $(LIBS)

main.o: main.cpp err.h network_util.h
	$(CXX) $(CXXFLAGS) -c main.cpp -o main.o

network_util.o: network_util.cpp network_util.h err.h
	$(CXX) $(CXXFLAGS) -c network_util.cpp -o network_util.o

err.o: err.cpp err.h
	$(CXX) $(CXXFLAGS) -c err.cpp -o err.o

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: clean