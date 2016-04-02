GCC=gcc
CC=g++
CFLAGS=-std=c++11 -Wall -O2

MEMEC_SRC_ROOT=../..
LIBS=
JLIB_DIR=$(MEMEC_SRC_ROOT)/lib/jerasure
GFLIB_DIR=$(MEMEC_SRC_ROOT)/lib/gf_complete
INCLUDE=-I$(JLIB_DIR)/include -I$(GFLIB_DIR)/include
JSRC_FILES=$(wildcard $(JLIB_DIR)/src/*.c)
GFSRC_FILES=$(wildcard $(GFLIB_DIR)/src/*.c)

OBJS= \
	coding.o \
	raid0coding.o \
	raid1coding.o \
	raid5coding.o \
	rdpcoding.o \
	cauchycoding.o \
	rscoding.o \
	evenoddcoding.o

JOBJS= $(foreach d,$(JSRC_FILES),$(patsubst %.c,%.o,$(notdir $d)))
GFOBJS= $(foreach d,$(GFSRC_FILES),$(patsubst %.c,%.o,$(notdir $d)))

all: $(JOBJS) $(GFOBJS) $(OBJS)

$(JOBJS): %.o: $(JLIB_DIR)/src/%.c
	$(GCC) -O3 $(INCLUDE) -c -o $@ $<

$(GFOBJS): %.o: $(GFLIB_DIR)/src/%.c
	$(GCC) -O3 $(INCLUDE) -c -o $@ $<

$(OBJS): %.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(JOBJS) $(GFOBJS)
