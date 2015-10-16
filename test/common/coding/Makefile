CC=g++
CFLAGS=-std=c++11 -Wall -O2 -g
PLIO_SRC_ROOT=../../..
LIBS=
INCLUDE=-I$(PLIO_SRC_ROOT)/lib/jerasure/include -I$(PLIO_SRC_ROOT)/lib/gf_complete/include
FLAGS=-DTEST_DELTA

TARGETS= \
	checker \
	coding \
	basic_op_performance \
	performance

OBJS= \
	$(wildcard $(PLIO_SRC_ROOT)/common/config/*.o) \
	$(wildcard $(PLIO_SRC_ROOT)/common/coding/*.o) \
	$(wildcard $(PLIO_SRC_ROOT)/common/ds/*.o) \
	$(PLIO_SRC_ROOT)/lib/inih/ini.o \
	-lrt

all: $(TARGETS)

jobjs:
	make -C $(PLIO_SRC_ROOT)/common/coding

jobjs_clean:
	make -C $(PLIO_SRC_ROOT)/common/coding clean

$(TARGETS): % : %.cc jobjs
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) $(FLAGS) -o $@ $< $(OBJS)

clean: jobjs_clean
	rm -f $(TARGETS)
