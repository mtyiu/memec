CC=g++
CFLAGS=-std=c++11 -Wall -O2
MEMEC_SRC_ROOT=../../..
LIBS=
EXTRA_LIB= -lpthread
INCLUDE=
FLAGS=-DTEST_DELTA
ifeq ($(USE_ISAL),1)
FLAGS+=-DUSE_ISAL
INCLUDE+=-I$(MEMEC_SRC_ROOT)/lib/isal-l-2.14.0/include
EXTRA_LIB+=$(MEMEC_SRC_ROOT)/lib/isa-l-2.14.0/lib/libisal.a
else
INCLUDE+=-I$(MEMEC_SRC_ROOT)/lib/jerasure/include -I$(MEMEC_SRC_ROOT)/lib/gf_complete/include
endif

TARGETS= \
	checker \
	coding \
	basic_op_performance \
	performance \
	batch_performance

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
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) $(FLAGS) -o $@ $< $(OBJS) $(EXTRA_LIB)

clean: jobjs_clean
	rm -f $(TARGETS)
