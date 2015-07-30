CC=g++
CFLAGS=-Wall -O2 -g
PLIO_SRC_ROOT=..
LIBS=-pthread

OBJS= \
	coding/coding.o \
	coding/raid5coding.o \
	config/config.o \
	config/global_config.o \
	config/server_addr.o \
	ds/bitmask_array.o \
	ds/chunk.o \
	ds/key_value.o \
	ds/stripe.o \
	protocol/protocol.o \
	socket/socket.o \
	socket/epoll.o

all: $(OBJS) 
	$(MAKE) -C coding 

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS)
