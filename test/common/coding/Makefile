CC=g++
CFLAGS=-std=c++11 -Wall -O2
MEMEC_SRC_ROOT=../../..
LIBS=
INCLUDE=-I$(MEMEC_SRC_ROOT)/lib/jerasure/include -I$(MEMEC_SRC_ROOT)/lib/gf_complete/include
FLAGS=-DTEST_DELTA

TARGETS= \
	checker \
	coding \
	basic_op_performance \
	performance

OBJS= \
	$(wildcard $(MEMEC_SRC_ROOT)/common/config/*.o) \
	$(wildcard $(MEMEC_SRC_ROOT)/common/coding/*.o) \
	$(wildcard $(MEMEC_SRC_ROOT)/common/ds/*.o) \
	$(MEMEC_SRC_ROOT)/lib/inih/ini.o \
	-lrt

all: $(TARGETS)

jobjs:
	make -C $(MEMEC_SRC_ROOT)/common/coding

jobjs_clean:
	make -C $(MEMEC_SRC_ROOT)/common/coding clean

$(TARGETS): % : %.cc jobjs
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) $(FLAGS) -o $@ $< $(OBJS)

clean: jobjs_clean
	rm -f $(TARGETS)
