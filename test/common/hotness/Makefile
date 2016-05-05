CC=g++
CFLAGS=-std=c++11 -Wall -O0 -g
MEMEC_SRC_ROOT=../../..
LIBS=

OBJS= \
	hotness_test

EXTERNAL_LIB= \
	../../../common/hotness/lru.o 

all: $(OBJS) $(EXTERNAL_LIB)

hotness_test: hotness_test.cc
	$(CC) $(CFLAGS) $(LIBS) -o $@ $^ $(EXTERNAL_LIB)

clean:
	rm -f $(OBJS)
