CC=g++
CFLAGS=-Wall -O2 -g
PLIO_SRC_ROOT=..
LIBS=-pthread
BIN=$(PLIO_SRC_ROOT)/bin/master

OBJS=

EXTERNAL_LIB=

all: $(OBJS) $(EXTERNAL_LIB)
	$(CC) $(CFLAGS) $(LIBS) -o $(BIN) $(OBJS) $(EXTERNAL_LIB)

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(BIN)
