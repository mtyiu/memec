#ifndef __COMMON_DS_REDIRECTION_LIST_HH__
#define __COMMON_DS_REDIRECTION_LIST_HH__

#include <cstdio>
#include <stdint.h>

class RedirectionList {
private:
	void init( uint8_t count );

public:
	uint8_t *original;
	uint8_t *redirected;
	uint8_t count;

	RedirectionList();
	void free();
	void set( uint8_t *original, uint8_t *redirected, uint8_t count, bool copy = true );
	int find( uint8_t chunkId, bool &isParity );

	void print( FILE *f = stderr );
};

#endif
