CC=g++
CFLAGS=-std=c++11 -Wall -O2
MEMEC_SRC_ROOT=..
LIBS=-pthread
EXTERNAL_LIBS=-ltspread

OBJS= \
	config/config.o \
	config/global_config.o \
	config/server_addr.o \
	ds/bitmask_array.o \
	ds/chunk_pool.o \
	ds/instance_id_generator.o \
	ds/key_value.o \
	ds/latency.o \
	ds/redirection_list.o \
	ds/sockaddr_in.o \
	hash/cuckoo_hash.o \
	protocol/protocol.o \
	protocol/ack_protocol.o \
	protocol/address_protocol.o \
	protocol/batch_protocol.o \
	protocol/chunk_protocol.o \
	protocol/degraded_protocol.o \
	protocol/fault_protocol.o \
	protocol/heartbeat_protocol.o \
	protocol/load_protocol.o \
	protocol/normal_protocol.o \
	protocol/recovery_protocol.o \
	protocol/remap_protocol.o \
	protocol/seal_protocol.o \
	state_transit/state_transit_handler.o \
	state_transit/state_transit_state.o \
	socket/socket.o \
	socket/epoll.o

.PHONY: coding

all: $(OBJS) coding

coding:
	$(MAKE) USE_ISAL=$(USE_ISAL) -C coding

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $< $(EXTERNAL_LIBS)

%.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $< $(EXTERNAL_LIBS)

clean:
	rm -f $(OBJS)
	make -C coding clean
