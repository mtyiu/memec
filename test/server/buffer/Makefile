CC=g++
CFLAGS=-std=c++11 -Wall -O2
MEMEC_SRC_ROOT=../../..
LIBS=-pthread

OBJS= \
	chunk_pool

EXTERNAL_LIB=

all: $(OBJS) $(EXTERNAL_LIB)

chunk_pool: chunk_pool.cc $(MEMEC_SRC_ROOT)/server/buffer/chunk_pool.o
	$(CC) $(CFLAGS) $(LIBS) -o $@ $^

clean:
	rm -f $(OBJS)
