CC=g++
CFLAGS=-std=c++11 -Wall -O2 -g
LIBS=-pthread
BIN=bin/benchmark bin/test

OBJS= \
	protocol.o \
	memec.o

LD_LIB= \
	-lrt

.PHONY: clean

all: bin/case2 bin/test

bin/case2: $(OBJS) case2.cc
	$(CC) $(CFLAGS) $(LIBS) -o $@ case2.cc $(OBJS) $(LD_LIB)

bin/test: $(OBJS) test.cc
	$(CC) $(CFLAGS) $(LIBS) -o $@ test.cc $(OBJS) $(LD_LIB)

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

%.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(BIN)
