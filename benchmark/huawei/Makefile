CC=g++
CFLAGS=-std=c++11 -Wall -O2
PLIO_SRC_ROOT=.
LIBS=-pthread
BIN=$(PLIO_SRC_ROOT)/bin/benchmark

OBJS= \
	protocol.o \
	memec.o \
	benchmark.o

LD_LIB= \
	-lrt

.PHONY: clean

$(BIN): $(OBJS)
	$(CC) $(CFLAGS) $(LIBS) -o $(BIN) $(OBJS) $(LD_LIB)

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

%.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(BIN)
