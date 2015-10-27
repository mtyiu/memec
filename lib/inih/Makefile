CC=g++
CFLAGS=-Wall -O2

all: ini.o

ini.o: ini.c ini.h
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f ini.o
