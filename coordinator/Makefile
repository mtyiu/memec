CC=g++
CFLAGS=-std=c++11 -Wall -O2
PLIO_SRC_ROOT=..
LIBS=-pthread
BIN=$(PLIO_SRC_ROOT)/bin/coordinator

OBJS= \
	config/coordinator_config.o \
	ds/map.o \
	event/coordinator_event.o \
	event/master_event.o \
	event/slave_event.o \
	protocol/protocol.o \
	protocol/degraded_protocol.o \
	protocol/load_protocol.o \
	protocol/heartbeat_protocol.o \
	protocol/recovery_protocol.o \
	protocol/register_protocol.o \
	protocol/remap_protocol.o \
	remap/remap_msg_handler.o \
	remap/remap_worker.o \
	socket/coordinator_socket.o \
	socket/master_socket.o \
	socket/slave_socket.o \
	worker/worker.o \
	worker/degraded_worker.o \
	worker/master_worker.o \
	worker/recovery_worker.o \
	worker/remap_worker.o \
	worker/slave_worker.o \
	main/coordinator.o \
	main/main.o

EXTERNAL_LIB= \
	$(PLIO_SRC_ROOT)/common/config/config.o \
	$(PLIO_SRC_ROOT)/common/config/global_config.o \
	$(PLIO_SRC_ROOT)/common/config/server_addr.o \
	$(PLIO_SRC_ROOT)/common/ds/bitmask_array.o \
	$(PLIO_SRC_ROOT)/common/ds/instance_id_generator.o \
	$(PLIO_SRC_ROOT)/common/ds/key_value.o \
	$(PLIO_SRC_ROOT)/common/ds/latency.o \
	$(PLIO_SRC_ROOT)/common/ds/load.o \
	$(PLIO_SRC_ROOT)/common/ds/sockaddr_in.o \
	$(PLIO_SRC_ROOT)/common/protocol/protocol.o \
	$(PLIO_SRC_ROOT)/common/protocol/address_protocol.o \
	$(PLIO_SRC_ROOT)/common/protocol/batch_protocol.o \
	$(PLIO_SRC_ROOT)/common/protocol/chunk_protocol.o \
	$(PLIO_SRC_ROOT)/common/protocol/degraded_protocol.o \
	$(PLIO_SRC_ROOT)/common/protocol/heartbeat_protocol.o \
	$(PLIO_SRC_ROOT)/common/protocol/fault_protocol.o \
	$(PLIO_SRC_ROOT)/common/protocol/load_protocol.o \
	$(PLIO_SRC_ROOT)/common/protocol/normal_protocol.o \
	$(PLIO_SRC_ROOT)/common/protocol/recovery_protocol.o \
	$(PLIO_SRC_ROOT)/common/protocol/remap_protocol.o \
	$(PLIO_SRC_ROOT)/common/remap/remap_msg_handler.o \
	$(PLIO_SRC_ROOT)/common/socket/socket.o \
	$(PLIO_SRC_ROOT)/common/socket/epoll.o \
	$(PLIO_SRC_ROOT)/lib/death_handler/death_handler.o \
	$(PLIO_SRC_ROOT)/lib/inih/ini.o

LD_LIB= \
	-lrt \
	-ldl \
	-ltspread

.PHONY: clean

$(BIN): $(OBJS) $(EXTERNAL_LIB)
	$(CC) $(CFLAGS) $(LIBS) -o $(BIN) $(OBJS) $(EXTERNAL_LIB) $(LD_LIB)

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

%.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(BIN)
