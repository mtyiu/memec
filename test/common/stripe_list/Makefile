CC=g++
CFLAGS=-std=c++11 -Wall -O2 -g
PLIO_SRC_ROOT=../../..
LIBS=

OBJS= \
	analysis_rand \
	analysis_m_c

EXTERNAL_LIB= \
	$(PLIO_SRC_ROOT)/common/ds/bitmask_array.o \
	-lm

all: $(OBJS) $(EXTERNAL_LIB)

analysis_rand: analysis_rand.cc $(PLIO_SRC_ROOT)/common/stripe_list/stripe_list.hh
	$(CC) $(CFLAGS) -Wno-unused-result $(LIBS) -o $@ analysis_rand.cc $(EXTERNAL_LIB)

analysis_m_c: analysis_m_c.cc $(PLIO_SRC_ROOT)/common/stripe_list/stripe_list.hh
	$(CC) $(CFLAGS) -Wno-unused-result $(LIBS) -o $@ analysis_m_c.cc $(EXTERNAL_LIB)

clean:
	rm -f $(OBJS)
