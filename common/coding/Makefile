GCC=gcc
CC=g++
CFLAGS=-std=c++11 -Wall -O2
FLAGS=
MEMEC_SRC_ROOT=../..
LIBS=
INCLUDE=

JLIB_DIR=$(MEMEC_SRC_ROOT)/lib/jerasure
GFLIB_DIR=$(MEMEC_SRC_ROOT)/lib/gf_complete
JSRC_FILES=$(wildcard $(JLIB_DIR)/src/*.c)
GFSRC_FILES=$(wildcard $(GFLIB_DIR)/src/*.c)
ISAL_DIR=$(MEMEC_SRC_ROOT)/lib/isa-l-2.14.0

OBJS= \
	coding.o \
	raid0coding.o \
	raid1coding.o \
	raid5coding.o \
	rdpcoding.o \
	cauchycoding.o \
	rscoding.o \
	evenoddcoding.o

JOBJS=
GFOBJS=
ifeq ($(USE_ISAL), 1)
# use ISA-L
FLAGS+= -DUSE_ISAL
INCLUDE+= -I$(ISAL_DIR)/include/
else
# use Jerasure 
JOBJS+= $(foreach d,$(JSRC_FILES),$(patsubst %.c,%.o,$(notdir $d)))
GFOBJS+= $(foreach d,$(GFSRC_FILES),$(patsubst %.c,%.o,$(notdir $d)))
INCLUDE+=-I$(JLIB_DIR)/include -I$(GFLIB_DIR)/include
endif


all: $(JOBJS) $(GFOBJS) $(OBJS) 

$(JOBJS): %.o: $(JLIB_DIR)/src/%.c
	$(GCC) $(INCLUDE) -c -o $@ $<

$(GFOBJS): %.o: $(GFLIB_DIR)/src/%.c
	$(GCC) $(INCLUDE) -c -o $@ $<

$(OBJS): %.o: %.cc %.hh
	$(CC) $(CFLAGS) $(FLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(JOBJS) $(GFOBJS)
