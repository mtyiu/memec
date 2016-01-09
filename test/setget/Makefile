CC=g++
CFLAGS=-std=c++11 -Wall -O2
LIBS=-pthread
BIN=test

OBJS= \
	protocol.o \
	memec.o

LD_LIB= \
	-lrt

.PHONY: clean

all: test

test: $(OBJS) test.cc
	$(CC) $(CFLAGS) $(LIBS) -o $@ test.cc $(OBJS) $(LD_LIB)

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

%.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

run:
	./test 255 4096 256 10000000 20 127.0.0.1 10091

hpc:
	./test 255 4096 256 10000000 20 137.189.88.46 9112

clean:
	rm -f $(OBJS) $(BIN)
