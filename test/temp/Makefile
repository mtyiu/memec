CC=g++
CFLAGS=-Wall -O2
PLIO_SRC_ROOT=../../..
LIBS=

OBJS= \
	permutation \
	stripe_list

EXTERNAL_LIB=

all: $(OBJS) $(EXTERNAL_LIB)

permutation: permutation.cc
	$(CC) $(CFLAGS) $(LIBS) -o $@ $^

stripe_list: stripe_list.cc
	$(CC) $(CFLAGS) $(LIBS) -o $@ $^

clean:
	rm -f $(OBJS)
