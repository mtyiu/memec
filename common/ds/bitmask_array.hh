#ifndef __COMMON_DS_BITMASK_ARRAY_HH__
#define __COMMON_DS_BITMASK_ARRAY_HH__

#include <cstdio>
#include <stdint.h>

class BitmaskArray {
private:
	size_t size;
	size_t count;
	size_t total;
	uint64_t *bitmasks;

public:
	BitmaskArray( size_t size, size_t count );
	~BitmaskArray();
	void set(   size_t entry, size_t bit );
	void unset( size_t entry, size_t bit );
	bool check( size_t entry, size_t bit );
	bool checkAllSet( size_t entry );
	void set(   size_t bit );
	void unset( size_t bit );
	bool check( size_t bit );
	void print( FILE *f = stdout );
	void printRaw( FILE *f = stdout );
	void clear( size_t entry );
};

#endif
