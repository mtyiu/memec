CC=g++
CFLAGS=-Wall -O2 -g
PLIO_SRC_ROOT=..
LIBS=-pthread

OBJS= \
	config/config.o \
	config/global_config.o \
	config/server_addr.o \
	socket/socket.o \
	socket/epoll.o \
	ds/bitmask_array.o

all: $(OBJS) 

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS)
