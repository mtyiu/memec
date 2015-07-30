GCC=gcc
CC=g++
CFLAGS=-Wall -O2 -g

PLIO_SRC_ROOT=..
LIBS=
INCLUDE=-I../../lib/jerasure/include -I../../lib/gf_complete/include
SRC_FILES=$(wildcard ../../lib/jerasure/src/*.c) $(wildcard ../../lib/gf_complete/src/*.c)

OBJS= \
	coding.o \
	raid5coding2.o \
	rdpcoding.o \
	cauchycoding.o \
	rscoding.o

COBJS= $(foreach d,$(SRC_FILES),$(patsubst %.c,%.o,$d))

all: $(COBJS) $(OBJS)

$(COBJS): %.o: %.c
	$(GCC) $(INCLUDE) -c -o $(@F) $<

$(OBJS): %.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(foreach d,$(SRC_FILES),$(patsubst %.c,%.o,$(notdir $d))) $(OBJS)
