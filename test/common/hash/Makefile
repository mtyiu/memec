CC=g++
CFLAGS=-std=c++11 -Wall -O2
PLIO_SRC_ROOT=../../..
LIBS=

OBJS= \
	hash \
	Hash.class

EXTERNAL_LIB=

all: $(OBJS) $(EXTERNAL_LIB)

hash: hash.cc
	$(CC) $(CFLAGS) $(LIBS) -o $@ $^

Hash.class: Hash.java
	javac Hash.java

clean:
	rm -f $(OBJS)
