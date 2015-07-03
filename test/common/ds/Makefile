CC=g++
CFLAGS=-Wall -O2 -g
PLIO_SRC_ROOT=../../..
LIBS=

OBJS= \
	bitmask_array

EXTERNAL_LIB=

all: $(OBJS) $(EXTERNAL_LIB)

bitmask_array: bitmask_array.cc $(PLIO_SRC_ROOT)/common/ds/bitmask_array.o
	$(CC) $(CFLAGS) -Wno-unused-result $(LIBS) -o $@ $^

clean:
	rm -f $(OBJS)
