CC=g++
CFLAGS=-std=c++11 -Wall -O2 -g
PLIO_SRC_ROOT=../../..
LIBS=

OBJS= \
	protocol

EXTERNAL_LIB=

all: $(OBJS) $(EXTERNAL_LIB)

protocol: protocol.cc $(PLIO_SRC_ROOT)/common/protocol/protocol.o
	$(CC) $(CFLAGS) $(LIBS) -o $@ $^

clean:
	rm -f $(OBJS)g
