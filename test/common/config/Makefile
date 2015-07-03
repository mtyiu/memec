CC=g++
CFLAGS=-Wall -O2 -g
PLIO_SRC_ROOT=../../..
LIBS=

OBJS= \
	server_addr

EXTERNAL_LIB=

all: $(OBJS) $(EXTERNAL_LIB)

server_addr: server_addr.cc $(PLIO_SRC_ROOT)/common/config/server_addr.o
	$(CC) $(CFLAGS) $(LIBS) -o $@ $^

clean:
	rm -f $(OBJS)
