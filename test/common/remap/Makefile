CC=g++
CFLAGS=-std=c++11 -Wall -O2
PLIO_SRC_ROOT=../../..
LIBS=-pthread

TARGETS= \
	coordinator_remap \
	client_remap

CLIENT_OBJS= \
	$(PLIO_SRC_ROOT)/client/config/master_config.o \
	$(PLIO_SRC_ROOT)/client/ds/pending.o \
	$(PLIO_SRC_ROOT)/client/event/application_event.o \
	$(PLIO_SRC_ROOT)/client/event/coordinator_event.o \
	$(PLIO_SRC_ROOT)/client/event/master_event.o \
	$(PLIO_SRC_ROOT)/client/event/slave_event.o \
	$(PLIO_SRC_ROOT)/client/protocol/protocol.o \
	$(PLIO_SRC_ROOT)/client/remap/remap_msg_handler.o \
	$(PLIO_SRC_ROOT)/client/remap/basic_remap_scheme.o \
	$(PLIO_SRC_ROOT)/client/socket/application_socket.o \
	$(PLIO_SRC_ROOT)/client/socket/coordinator_socket.o \
	$(PLIO_SRC_ROOT)/client/socket/master_socket.o \
	$(PLIO_SRC_ROOT)/client/socket/slave_socket.o \
	$(PLIO_SRC_ROOT)/client/worker/worker.o \
	$(PLIO_SRC_ROOT)/client/main/master.o

COOR_OBJS= \
	$(PLIO_SRC_ROOT)/coordinator/worker/worker.o \
	$(PLIO_SRC_ROOT)/coordinator/main/coordinator.o \
	$(PLIO_SRC_ROOT)/coordinator/config/coordinator_config.o \
	$(PLIO_SRC_ROOT)/coordinator/ds/map.o \
	$(PLIO_SRC_ROOT)/coordinator/event/coordinator_event.o \
	$(PLIO_SRC_ROOT)/coordinator/event/master_event.o \
	$(PLIO_SRC_ROOT)/coordinator/event/slave_event.o \
	$(PLIO_SRC_ROOT)/coordinator/protocol/protocol.o \
	$(PLIO_SRC_ROOT)/coordinator/socket/coordinator_socket.o \
	$(PLIO_SRC_ROOT)/coordinator/socket/master_socket.o \
	$(PLIO_SRC_ROOT)/coordinator/socket/slave_socket.o \
	$(PLIO_SRC_ROOT)/coordinator/remap/remap_msg_handler.o \
	$(PLIO_SRC_ROOT)/coordinator/remap/remap_worker.o

OBJS= \
	$(CLIENT_OBJS) \
	$(COOR_OBJS)

EXTERNAL_LIBS = \
	$(PLIO_SRC_ROOT)/common/config/config.o \
	$(PLIO_SRC_ROOT)/common/config/global_config.o \
	$(PLIO_SRC_ROOT)/common/config/server_addr.o \
	$(PLIO_SRC_ROOT)/common/ds/bitmask_array.o \
	$(PLIO_SRC_ROOT)/common/ds/key_value.o \
	$(PLIO_SRC_ROOT)/common/ds/latency.o \
	$(PLIO_SRC_ROOT)/common/ds/load.o \
	$(PLIO_SRC_ROOT)/common/ds/sockaddr_in.o \
	$(PLIO_SRC_ROOT)/common/protocol/protocol.o \
	$(PLIO_SRC_ROOT)/common/remap/remap_msg_handler.o \
	$(PLIO_SRC_ROOT)/common/socket/socket.o \
	$(PLIO_SRC_ROOT)/common/socket/epoll.o \
	$(PLIO_SRC_ROOT)/lib/death_handler/death_handler.o \
	$(PLIO_SRC_ROOT)/lib/inih/ini.o \
	-ldl \
	-lrt \
	-ltspread

all: $(OBJS) $(TARGETS)

#$(TARGETS): % : %.cc $(OBJS)
#	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $< $(OBJS) $(EXTERNAL_LIBS)

client_remap: % : %.cc $(CLIENT_OBJS)
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $< $(CLIENT_OBJS) $(EXTERNAL_LIBS)

coordinator_remap: % : %.cc $(CORD_OBJS)
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $< $(COOR_OBJS) $(EXTERNAL_LIBS)

$(OBJS): %.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(TARGETS)
