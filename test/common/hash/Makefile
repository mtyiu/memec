CC=g++
CFLAGS=-std=c++11 -Wall -O2
MEMEC_SRC_ROOT=../../..
LIBS=-pthread

OBJS= \
	cuckoo_hash \
	hash \
	Hash.class

EXTERNAL_LIB= \
	$(MEMEC_SRC_ROOT)/common/ds/key_value.o \
	$(MEMEC_SRC_ROOT)/common/hash/cuckoo_hash.o

all: $(OBJS) $(EXTERNAL_LIB)

cuckoo_hash: cuckoo_hash.cc
	$(CC) $(CFLAGS) $(LIBS) $(EXTERNAL_LIB) -o $@ $^

hash: hash.cc
	$(CC) $(CFLAGS) $(LIBS) -o $@ $^

Hash.class: Hash.java
	javac Hash.java

clean:
	rm -f $(OBJS)
