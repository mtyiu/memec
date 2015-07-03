CC=g++
CFLAGS=-Wall -O2 -g
PLIO_SRC_ROOT=..
LIBS=-pthread
BIN=$(PLIO_SRC_ROOT)/bin/coordinator

OBJS= \
	config/coordinator_config.o \
	main/main.o

EXTERNAL_LIB= \
	$(PLIO_SRC_ROOT)/common/config/config.o \
	$(PLIO_SRC_ROOT)/lib/inih/ini.o

.PHONY: clean

$(BIN): $(OBJS) $(EXTERNAL_LIB)
	$(CC) $(CFLAGS) $(LIBS) -o $(BIN) $(OBJS) $(EXTERNAL_LIB)

%.o: %.cc %.hh
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

%.o: %.cc
	$(CC) $(CFLAGS) $(INCLUDE) $(LIBS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(BIN)
