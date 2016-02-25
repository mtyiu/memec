CC=g++
CFLAGS=-std=c++11 -Wall -O2
MEMEC_SRC_ROOT=../../..
LIBS=

OBJS= \
	global_config \
	server_addr

EXTERNAL_LIB=

all: $(OBJS) $(EXTERNAL_LIB)

global_config: global_config.cc $(MEMEC_SRC_ROOT)/common/config/config.o $(MEMEC_SRC_ROOT)/common/config/global_config.o $(MEMEC_SRC_ROOT)/common/config/server_addr.o $(MEMEC_SRC_ROOT)/lib/inih/ini.o
	$(CC) $(CFLAGS) $(LIBS) -o $@ $^

server_addr: server_addr.cc $(MEMEC_SRC_ROOT)/common/config/server_addr.o
	$(CC) $(CFLAGS) $(LIBS) -o $@ $^

clean:
	rm -f $(OBJS)
