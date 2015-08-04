CC=g++
CFLAGS=-Wall -O2 -g
PLIO_SRC_ROOT=..
LIBS=-pthread -lrt
BIN=$(PLIO_SRC_ROOT)/bin/master

OBJS= \
	config/master_config.o \
	event/application_event.o \
	event/coordinator_event.o \
	event/master_event.o \
	event/slave_event.o \
	protocol/protocol.o \
	socket/application_socket.o \
	socket/coordinator_socket.o \
	socket/master_socket.o \
	socket/slave_socket.o \
	worker/worker.o \
	main/master.o \
	main/main.o

EXTERNAL_LIB= \
	$(PLIO_SRC_ROOT)/common/config/config.o \
	$(PLIO_SRC_ROOT)/common/config/global_config.o \
	$(PLIO_SRC_ROOT)/common/config/server_addr.o \
	$(PLIO_SRC_ROOT)/common/ds/bitmask_array.o \
	$(PLIO_SRC_ROOT)/common/ds/key_value.o \
	$(PLIO_SRC_ROOT)/common/protocol/protocol.o \
	$(PLIO_SRC_ROOT)/common/socket/socket.o \
	$(PLIO_SRC_ROOT)/common/socket/epoll.o \
	$(PLIO_SRC_ROOT)/lib/inih/ini.o

.PHONY: clean

$(BIN): $(OBJS) $(EXTERNAL_LIB)
	$(CC) $(CFLAGS) $(LIBS) -o $(BIN) $(OBJS) $(EXTERNAL_LIB)

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

%.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(BIN)
