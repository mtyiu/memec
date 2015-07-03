#ifndef __BITMASK_ARRAY_HH__
#define __BITMASK_ARRAY_HH__

#include <cstdio>
#include <stdint.h>

class BitmaskArray {
private:
	size_t size;
	size_t count;
	size_t total;
	uint64_t *bitmasks;

public:
	BitmaskArray( size_t, size_t );
	~BitmaskArray();
	void set( size_t, size_t );
	void unset( size_t, size_t );
	bool check( size_t, size_t );
	void print( FILE *f = stdout );
	void printRaw( FILE *f = stdout );
	void clear( size_t );
};

#endif
