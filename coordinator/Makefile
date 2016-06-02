CC=g++
CFLAGS=-std=c++11 -Wall -O2
MEMEC_SRC_ROOT=..
LIBS=-pthread
BIN=$(MEMEC_SRC_ROOT)/bin/coordinator

OBJS= \
	config/coordinator_config.o \
	ds/map.o \
	event/coordinator_event.o \
	event/client_event.o \
	event/server_event.o \
	protocol/protocol.o \
	state_transit/state_transit_handler.o \
	state_transit/state_transit_worker.o \
	socket/coordinator_socket.o \
	socket/client_socket.o \
	socket/server_socket.o \
	worker/worker.o \
	worker/degraded_worker.o \
	worker/client_worker.o \
	worker/recovery_worker.o \
	worker/remap_worker.o \
	worker/server_worker.o \
	main/coordinator.o \
	main/main.o

EXTERNAL_LIB= \
	$(MEMEC_SRC_ROOT)/common/config/config.o \
	$(MEMEC_SRC_ROOT)/common/config/global_config.o \
	$(MEMEC_SRC_ROOT)/common/config/server_addr.o \
	$(MEMEC_SRC_ROOT)/common/ds/bitmask_array.o \
	$(MEMEC_SRC_ROOT)/common/ds/instance_id_generator.o \
	$(MEMEC_SRC_ROOT)/common/ds/key_value.o \
	$(MEMEC_SRC_ROOT)/common/ds/latency.o \
	$(MEMEC_SRC_ROOT)/common/ds/load.o \
	$(MEMEC_SRC_ROOT)/common/ds/sockaddr_in.o \
	$(MEMEC_SRC_ROOT)/common/protocol/protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/address_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/batch_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/chunk_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/degraded_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/heartbeat_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/fault_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/load_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/normal_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/recovery_protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/remap_protocol.o \
	$(MEMEC_SRC_ROOT)/common/state_transit/state_transit_handler.o \
	$(MEMEC_SRC_ROOT)/common/state_transit/state_transit_state.o \
	$(MEMEC_SRC_ROOT)/common/socket/socket.o \
	$(MEMEC_SRC_ROOT)/common/socket/epoll.o \
	$(MEMEC_SRC_ROOT)/lib/death_handler/death_handler.o \
	$(MEMEC_SRC_ROOT)/lib/inih/ini.o

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
