CC=g++
CFLAGS=-std=c++11 -Wall -O2
MEMEC_SRC_ROOT=../../..
LIBS=-pthread

OBJS= \
	chunk_pool

EXTERNAL_LIB= \
	$(MEMEC_SRC_ROOT)/server/buffer/chunk_pool.o

all: $(OBJS) $(EXTERNAL_LIB)

chunk_pool: chunk_pool.cc
	$(CC) $(CFLAGS) $(LIBS) $(EXTERNAL_LIB) -o $@ $^

clean:
	rm -f $(OBJS)
