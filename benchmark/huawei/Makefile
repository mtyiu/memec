CC=g++
CFLAGS=-std=c++11 -Wall -O2
LIBS=-pthread
BIN=bin/benchmark bin/test

OBJS= \
	protocol.o \
	memec.o

LD_LIB= \
	-lrt

.PHONY: clean

all: bin/benchmark bin/window bin/test

# bin/benchmark: $(OBJS) memec_sync.o benchmark.cc
# 	$(CC) $(CFLAGS) $(LIBS) -o $@ benchmark.cc protocol.o memec_sync.o $(LD_LIB)

bin/benchmark: $(OBJS) benchmark.cc
		$(CC) $(CFLAGS) $(LIBS) -o $@ benchmark.cc $(OBJS) $(LD_LIB)

bin/window: $(OBJS) window.cc
	$(CC) $(CFLAGS) $(LIBS) -o $@ window.cc $(OBJS) $(LD_LIB)

bin/test: $(OBJS) test.cc
	$(CC) $(CFLAGS) $(LIBS) -o $@ test.cc $(OBJS) $(LD_LIB)

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

%.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(BIN)
