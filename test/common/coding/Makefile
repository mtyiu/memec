CC=g++
CFLAGS=-Wall -O2 -g
PLIO_SRC_ROOT=../../..
LIBS=

TARGETS= \
	coding

OBJS = \
	$(wildcard $(PLIO_SRC_ROOT)/common/config/*.o) \
	$(wildcard $(PLIO_SRC_ROOT)/common/coding/*.o) \
	$(wildcard $(PLIO_SRC_ROOT)/common/ds/*.o) \
	$(PLIO_SRC_ROOT)/lib/inih/ini.o

EXTERNAL_LIB=

all: $(TARGETS) $(EXTERNAL_LIB)

jobjs:
	make -C $(PLIO_SRC_ROOT)/common/coding

jobjs_clean:
	make -C $(PLIO_SRC_ROOT)/common/coding clean

$(TARGETS): % : %.cc jobjs
	$(CC) $(CFLAGS) $(LIBS) -o $@ $< $(OBJS)

clean: jobjs_clean
	rm -f $(TARGETS)
