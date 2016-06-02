CC=g++
CFLAGS=-std=c++11 -Wall -O2
MEMEC_SRC_ROOT=../../..
LIBS=-pthread

TARGETS= \
	coordinator_remap \
	client_remap \
	coordinator_spread \
	client_spread

CLIENT_OBJS= \
	$(MEMEC_SRC_ROOT)/client/backup/*.o \
	$(MEMEC_SRC_ROOT)/client/config/client_config.o \
	$(MEMEC_SRC_ROOT)/client/ds/pending.o \
	$(MEMEC_SRC_ROOT)/client/event/application_event.o \
	$(MEMEC_SRC_ROOT)/client/event/coordinator_event.o \
	$(MEMEC_SRC_ROOT)/client/event/client_event.o \
	$(MEMEC_SRC_ROOT)/client/event/server_event.o \
	$(MEMEC_SRC_ROOT)/client/protocol/*.o \
	$(MEMEC_SRC_ROOT)/client/remap/remap_msg_handler.o \
	$(MEMEC_SRC_ROOT)/client/remap/basic_remap_scheme.o \
	$(MEMEC_SRC_ROOT)/client/socket/*.o \
	$(MEMEC_SRC_ROOT)/client/worker/*.o \
	$(MEMEC_SRC_ROOT)/client/main/client.o

COOR_OBJS= \
	$(MEMEC_SRC_ROOT)/coordinator/worker/*.o \
	$(MEMEC_SRC_ROOT)/coordinator/main/coordinator.o \
	$(MEMEC_SRC_ROOT)/coordinator/config/coordinator_config.o \
	$(MEMEC_SRC_ROOT)/coordinator/ds/map.o \
	$(MEMEC_SRC_ROOT)/coordinator/event/coordinator_event.o \
	$(MEMEC_SRC_ROOT)/coordinator/event/client_event.o \
	$(MEMEC_SRC_ROOT)/coordinator/event/server_event.o \
	$(MEMEC_SRC_ROOT)/coordinator/protocol/*.o \
	$(MEMEC_SRC_ROOT)/coordinator/socket/*.o \
	$(MEMEC_SRC_ROOT)/coordinator/remap/remap_msg_handler.o \
	$(MEMEC_SRC_ROOT)/coordinator/remap/remap_worker.o

EXTRA_OBJS= \
	simple_remap_msg_handler.o

OBJS= \
	$(CLIENT_OBJS) \
	$(COOR_OBJS) \
	$(EXTRA_OBJS)

EXTERNAL_LIBS = \
	$(MEMEC_SRC_ROOT)/common/config/config.o \
	$(MEMEC_SRC_ROOT)/common/config/global_config.o \
	$(MEMEC_SRC_ROOT)/common/config/server_addr.o \
	$(MEMEC_SRC_ROOT)/common/ds/bitmask_array.o \
	$(MEMEC_SRC_ROOT)/common/ds/instance_id_generator.o \
	$(MEMEC_SRC_ROOT)/common/ds/key_value.o \
	$(MEMEC_SRC_ROOT)/common/ds/latency.o \
	$(MEMEC_SRC_ROOT)/common/protocol/*.o \
	$(MEMEC_SRC_ROOT)/common/remap/remap_msg_handler.o \
	$(MEMEC_SRC_ROOT)/common/socket/*.o \
	$(MEMEC_SRC_ROOT)/lib/death_handler/death_handler.o \
	$(MEMEC_SRC_ROOT)/lib/inih/ini.o \
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

client_spread: % : %.cc $(CLIENT_OBJS)
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $< $(CLIENT_OBJS) $(EXTRA_OBJS) $(EXTERNAL_LIBS)

coordinator_spread:% : %.cc $(CORD_OBJS)
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $< $(COOR_OBJS) $(EXTRA_OBJS) $(EXTERNAL_LIBS)

$(OBJS): %.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(TARGETS)
