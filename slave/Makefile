CC=g++
CFLAGS=-Wall -O2 -g
PLIO_SRC_ROOT=..
LIBS=-pthread
BIN=$(PLIO_SRC_ROOT)/bin/slave

OBJS= \
	main/main.o

EXTERNAL_LIB=

.PHONY: clean

$(BIN): $(OBJS) $(EXTERNAL_LIB)
	$(CC) $(CFLAGS) $(LIBS) -o $(BIN) $(OBJS) $(EXTERNAL_LIB)

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

%.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(BIN)
