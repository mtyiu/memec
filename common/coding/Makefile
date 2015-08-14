GCC=gcc
CC=g++
CFLAGS=-Wall -O2 -g

PLIO_SRC_ROOT=../..
LIBS=
JLIB_DIR=$(PLIO_SRC_ROOT)/lib/jerasure
GFLIB_DIR=$(PLIO_SRC_ROOT)/lib/gf_complete
INCLUDE=-I$(JLIB_DIR)/include -I$(GFLIB_DIR)/include
JSRC_FILES=$(wildcard $(JLIB_DIR)/src/*.c) 
GFSRC_FILES=$(wildcard $(GFLIB_DIR)/src/*.c)

OBJS= \
	coding.o \
	raid5coding.o \
	rdpcoding.o \
	cauchycoding.o \
	rscoding.o \
	evenoddcoding.o

JOBJS= $(foreach d,$(JSRC_FILES),$(patsubst %.c,%.o,$(notdir $d)))
GFOBJS= $(foreach d,$(GFSRC_FILES),$(patsubst %.c,%.o,$(notdir $d)))

all: $(JOBJS) $(GFOBJS) $(OBJS) 

$(JOBJS): %.o: $(JLIB_DIR)/src/%.c
	$(GCC) $(INCLUDE) -c -o $@ $<

$(GFOBJS): %.o: $(GFLIB_DIR)/src/%.c
	$(GCC) $(INCLUDE) -c -o $@ $<

$(OBJS): %.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(JOBJS) $(GFOBJS)
