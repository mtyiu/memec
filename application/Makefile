CC=g++
CFLAGS=-std=c++11 -Wall -O2
PLIO_SRC_ROOT=..
LIBS=-pthread
BIN=$(PLIO_SRC_ROOT)/bin/application

OBJS= \
	config/application_config.o \
	event/application_event.o \
	event/master_event.o \
	protocol/protocol.o \
	socket/master_socket.o \
	worker/worker.o \
	main/application.o \
	main/main.o

EXTERNAL_LIB= \
	$(PLIO_SRC_ROOT)/common/config/config.o \
	$(PLIO_SRC_ROOT)/common/config/server_addr.o \
	$(PLIO_SRC_ROOT)/common/ds/key_value.o \
	$(PLIO_SRC_ROOT)/common/protocol/protocol.o \
	$(PLIO_SRC_ROOT)/common/protocol/normal_protocol.o \
	$(PLIO_SRC_ROOT)/common/socket/socket.o \
	$(PLIO_SRC_ROOT)/common/socket/epoll.o \
	$(PLIO_SRC_ROOT)/lib/death_handler/death_handler.o \
	$(PLIO_SRC_ROOT)/lib/inih/ini.o

LD_LIB= \
	-lrt \
	-ldl

.PHONY: clean

$(BIN): $(OBJS) $(EXTERNAL_LIB)
	$(CC) $(CFLAGS) $(LIBS) -o $(BIN) $(OBJS) $(EXTERNAL_LIB) $(LD_LIB)

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

%.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(BIN)
