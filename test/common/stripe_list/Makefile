CC=g++
CFLAGS=-std=c++11 -Wall -O2 -g
PLIO_SRC_ROOT=../../..
LIBS=

OBJS= \
	analysis

EXTERNAL_LIB= \
	$(PLIO_SRC_ROOT)/common/ds/bitmask_array.o \
	-lm

all: $(OBJS) $(EXTERNAL_LIB)

analysis: analysis.cc $(PLIO_SRC_ROOT)/common/stripe_list/stripe_list.hh
	$(CC) $(CFLAGS) -Wno-unused-result $(LIBS) -o $@ analysis.cc $(EXTERNAL_LIB)

clean:
	rm -f $(OBJS)
