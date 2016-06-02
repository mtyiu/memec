CC=g++
CFLAGS=-std=c++11 -Wall -O2
MEMEC_SRC_ROOT=..
LIBS=-pthread
BIN=$(MEMEC_SRC_ROOT)/bin/application

OBJS= \
	config/application_config.o \
	event/client_event.o \
	protocol/protocol.o \
	socket/client_socket.o \
	worker/worker.o \
	main/application.o \
	main/main.o

EXTERNAL_LIB= \
	$(MEMEC_SRC_ROOT)/common/config/config.o \
	$(MEMEC_SRC_ROOT)/common/config/server_addr.o \
	$(MEMEC_SRC_ROOT)/common/ds/key_value.o \
	$(MEMEC_SRC_ROOT)/common/protocol/protocol.o \
	$(MEMEC_SRC_ROOT)/common/protocol/normal_protocol.o \
	$(MEMEC_SRC_ROOT)/common/socket/socket.o \
	$(MEMEC_SRC_ROOT)/common/socket/epoll.o \
	$(MEMEC_SRC_ROOT)/lib/death_handler/death_handler.o \
	$(MEMEC_SRC_ROOT)/lib/inih/ini.o

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
