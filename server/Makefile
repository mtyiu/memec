CC=g++
CFLAGS=-std=c++11 -Wall -O2
MEMEC_SRC_ROOT=..
LIBS=-pthread
BIN=$(MEMEC_SRC_ROOT)/bin/server

OBJS= \
	backup/backup.o \
	buffer/chunk_buffer.o \
	buffer/data_chunk_buffer.o \
	buffer/degraded_chunk_buffer.o \
	buffer/get_chunk_buffer.o \
	buffer/mixed_chunk_buffer.o \
	buffer/parity_chunk_buffer.o \
	buffer/remapped_buffer.o \
	config/server_config.o \
	ds/pending.o \
	ds/map.o \
	event/coding_event.o \
	event/coordinator_event.o \
	event/io_event.o \
	event/client_event.o \
	event/server_event.o \
	event/server_peer_event.o \
	state_transit/state_transit_handler.o \
	protocol/ack_protocol.o \
	protocol/degraded_protocol.o \
	protocol/heartbeat_protocol.o \
	protocol/normal_client_protocol.o \
	protocol/normal_server_protocol.o \
	protocol/recovery_protocol.o \
	protocol/register_protocol.o \
	protocol/remap_protocol.o \
	protocol/seal_protocol.o \
	socket/coordinator_socket.o \
	socket/client_socket.o \
	socket/server_socket.o \
	socket/server_peer_socket.o \
	storage/storage.o \
	storage/local_storage.o \
	worker/worker.o \
	worker/coordinator_worker.o \
	worker/degraded_worker.o \
	worker/client_worker.o \
	worker/recovery_worker.o \
	worker/remap_worker.o \
	worker/server_peer_req_worker.o \
	worker/server_peer_res_worker.o \
	worker/server_peer_worker.o \
	main/server.o \
	main/main.o

EXTERNAL_LIB= \
	$(wildcard $(MEMEC_SRC_ROOT)/common/coding/*.o) \
	$(MEMEC_SRC_ROOT)/common/config/config.o \
	$(MEMEC_SRC_ROOT)/common/config/global_config.o \
	$(MEMEC_SRC_ROOT)/common/config/server_addr.o \
	$(MEMEC_SRC_ROOT)/common/ds/bitmask_array.o \
	$(MEMEC_SRC_ROOT)/common/ds/chunk.o \
	$(MEMEC_SRC_ROOT)/common/ds/chunk_pool.o \
	$(MEMEC_SRC_ROOT)/common/ds/key_value.o \
	$(MEMEC_SRC_ROOT)/common/ds/load.o \
	$(MEMEC_SRC_ROOT)/common/ds/sockaddr_in.o \
	$(MEMEC_SRC_ROOT)/common/hash/cuckoo_hash.o \
	$(MEMEC_SRC_ROOT)/common/state_transit/state_transit_handler.o \
	$(MEMEC_SRC_ROOT)/common/protocol/protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/ack_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/address_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/batch_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/chunk_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/degraded_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/heartbeat_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/normal_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/recovery_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/remap_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/seal_protocol.o \
	$(MEMEC_SRC_ROOT)/common/socket/socket.o \
	$(MEMEC_SRC_ROOT)/common/socket/epoll.o \
	$(MEMEC_SRC_ROOT)/lib/death_handler/death_handler.o \
	$(MEMEC_SRC_ROOT)/lib/inih/ini.o

ifeq ($(USE_ISAL), 1)
	EXTERNAL_LIB+= $(MEMEC_SRC_ROOT)/lib/isa-l-2.14.0/lib/libisal.a
endif

LD_LIB= \
	-lrt \
	-ldl \
	-lspread

.PHONY: clean

$(BIN): $(OBJS) $(EXTERNAL_LIB)
	$(CC) $(CFLAGS) $(LIBS) -o $(BIN) $(OBJS) $(EXTERNAL_LIB) $(LD_LIB)

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

%.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(BIN)
