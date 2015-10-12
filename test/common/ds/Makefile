CC=g++
CFLAGS=-std=c++11 -Wall -O2 -g
PLIO_SRC_ROOT=../../..
LIBS=

OBJS= \
	bitmask_array \
	id_generator

EXTERNAL_LIB=

all: $(OBJS) $(EXTERNAL_LIB)

bitmask_array: bitmask_array.cc $(PLIO_SRC_ROOT)/common/ds/bitmask_array.o
	$(CC) $(CFLAGS) -Wno-unused-result $(LIBS) -o $@ $^

id_generator: id_generator.cc $(PLIO_SRC_ROOT)/common/ds/id_generator.hh
	$(CC) $(CFLAGS) -Wno-unused-result $(LIBS) -o $@ $^

clean:
	rm -f $(OBJS)
