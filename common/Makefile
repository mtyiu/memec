CC=g++
CFLAGS=-std=c++11 -Wall -O2 -g
PLIO_SRC_ROOT=..
LIBS=-pthread
EXTERNAL_LIBS=-ltspread

OBJS= \
	config/config.o \
	config/global_config.o \
	config/server_addr.o \
	ds/bitmask_array.o \
	ds/chunk.o \
	ds/key_value.o \
	ds/latency.o \
	ds/load.o \
	ds/sockaddr_in.o \
	protocol/protocol.o \
	remap/remap_msg_handler.o \
	socket/socket.o \
	socket/epoll.o

all: $(OBJS)
	$(MAKE) -C coding

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $< $(EXTERNAL_LIBS)

clean:
	rm -f $(OBJS)
