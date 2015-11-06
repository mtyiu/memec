CC=g++
CFLAGS=-Wall -Wno-unused-result -O2

all: death_handler.o

death_handler.o: death_handler.cc death_handler.h
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f death_handler.o
