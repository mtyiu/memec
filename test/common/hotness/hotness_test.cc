#include <stdio.h>
#include <string.h>
#include "hotness_test.hh"
#include "../../../common/hotness/hotness.hh"
#include "../../../common/hotness/lru.hh"

int main( int argc, char **argv ) {
	if ( argc < 2 ) {
		fprintf( stderr, "Usage: %s [lru]\n", argv[ 0 ] );
		return -1;
	}

	Hotness *hotness;
	LruHotness lru;
	if ( strcmp( argv[1], "lru" ) == 0 ) {
		hotness = &lru;
	} else {
		fprintf( stderr, "Unknown data structure" );
		return -1;
	}

	HotnessDataStructTest test( hotness );
	test.run();

	return 0;
}
