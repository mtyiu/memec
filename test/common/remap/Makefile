CC=g++
CFLAGS=-std=c++11 -Wall -O2
PLIO_SRC_ROOT=../../..
LIBS=-pthread

TARGETS= \
	coordinator_remap \
	master_remap

MSTR_OBJS= \
	$(PLIO_SRC_ROOT)/master/config/master_config.o \
	$(PLIO_SRC_ROOT)/master/ds/pending.o \
	$(PLIO_SRC_ROOT)/master/event/application_event.o \
	$(PLIO_SRC_ROOT)/master/event/coordinator_event.o \
	$(PLIO_SRC_ROOT)/master/event/master_event.o \
	$(PLIO_SRC_ROOT)/master/event/slave_event.o \
	$(PLIO_SRC_ROOT)/master/protocol/protocol.o \
	$(PLIO_SRC_ROOT)/master/remap/remap_msg_handler.o \
	$(PLIO_SRC_ROOT)/master/remap/basic_remap_scheme.o \
	$(PLIO_SRC_ROOT)/master/socket/application_socket.o \
	$(PLIO_SRC_ROOT)/master/socket/coordinator_socket.o \
	$(PLIO_SRC_ROOT)/master/socket/master_socket.o \
	$(PLIO_SRC_ROOT)/master/socket/slave_socket.o \
	$(PLIO_SRC_ROOT)/master/worker/worker.o \
	$(PLIO_SRC_ROOT)/coordinator/remap/remap_worker.o \
	$(PLIO_SRC_ROOT)/coordinator/remap/remap_msg_handler.o \
	$(PLIO_SRC_ROOT)/master/main/master.o 

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
	$(MSTR_OBJS) \
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

master_remap: % : %.cc $(MSTR_OBJS)
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $< $(MSTR_OBJS) $(EXTERNAL_LIBS)

coordinator_remap: % : %.cc $(CORD_OBJS)
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $< $(COOR_OBJS) $(EXTERNAL_LIBS)

$(OBJS): %.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(TARGETS)
