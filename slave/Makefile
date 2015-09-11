CC=g++
CFLAGS=-Wall -O2 -g
PLIO_SRC_ROOT=..
LIBS=-pthread
BIN=$(PLIO_SRC_ROOT)/bin/slave

OBJS= \
	buffer/chunk_buffer.o \
	buffer/data_chunk_buffer.o \
	buffer/dummy_data_chunk_buffer.o \
	buffer/parity_chunk_buffer.o \
	buffer/mixed_chunk_buffer.o \
	config/slave_config.o \
	ds/slave_load.o \
	event/coding_event.o \
	event/coordinator_event.o \
	event/io_event.o \
	event/master_event.o \
	event/slave_event.o \
	event/slave_peer_event.o \
	protocol/protocol.o \
	socket/coordinator_socket.o \
	socket/master_socket.o \
	socket/slave_socket.o \
	socket/slave_peer_socket.o \
	storage/storage.o \
	storage/local_storage.o \
	worker/worker.o \
	main/slave.o \
	main/main.o

EXTERNAL_LIB= \
	$(wildcard $(PLIO_SRC_ROOT)/common/coding/*.o) \
	$(PLIO_SRC_ROOT)/common/config/config.o \
	$(PLIO_SRC_ROOT)/common/config/global_config.o \
	$(PLIO_SRC_ROOT)/common/config/server_addr.o \
	$(PLIO_SRC_ROOT)/common/ds/bitmask_array.o \
	$(PLIO_SRC_ROOT)/common/ds/chunk.o \
	$(PLIO_SRC_ROOT)/common/ds/key_value.o \
	$(PLIO_SRC_ROOT)/common/ds/load.o \
	$(PLIO_SRC_ROOT)/common/protocol/protocol.o \
	$(PLIO_SRC_ROOT)/common/socket/socket.o \
	$(PLIO_SRC_ROOT)/common/socket/epoll.o \
	$(PLIO_SRC_ROOT)/lib/death_handler/death_handler.o \
	$(PLIO_SRC_ROOT)/lib/inih/ini.o \
	-lrt \
	-ldl

.PHONY: clean

$(BIN): $(OBJS) $(EXTERNAL_LIB)
	$(CC) $(CFLAGS) $(LIBS) -o $(BIN) $(OBJS) $(EXTERNAL_LIB)

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

%.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(BIN)
