CC=g++
CFLAGS=-Wall -O2 -g
PLIO_SRC_ROOT=../../..
LIBS=-pthread

TARGETS= \
	coordinator_remap \
	master_remap

OBJS= \
	$(PLIO_SRC_ROOT)/master/remap/remap_msg_handler.o \
	$(PLIO_SRC_ROOT)/coordinator/remap/remap_msg_handler.o 

EXTERNAL_LIBS = \
    $(PLIO_SRC_ROOT)/common/remap/remap_msg_handler.o \
	-ldl \
	-lrt \
	-ltspread

all: $(OBJS) $(TARGETS) 

$(TARGETS): % : %.cc $(OBJS)
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $< $(OBJS) $(EXTERNAL_LIBS)

$(OBJS): %.o: %.cc 
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean: 
	rm -f $(TARGETS)
