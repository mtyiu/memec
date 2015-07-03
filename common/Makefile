CC=g++
CFLAGS=-Wall -O2 -g
PLIO_SRC_ROOT=..
LIBS=-pthread

OBJS= \
	ds/bitmask_array.o

all: $(OBJS) 

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS)
