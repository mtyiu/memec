#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "../../../common/util/time.hh"

extern "C" {
#include "../../../lib/jerasure/include/galois.h"
}

#define CSIZE (4096)
#define ROUNDS (50000)
#define GB ( 1024 * 1024 * 1024 )

int main (void) {

	char* buf = (char*) malloc (sizeof(char) * CSIZE);
	char* buf2 = (char*) malloc (sizeof(char) * CSIZE);

	memset(buf, 4, CSIZE);
	memset(buf2, 5, CSIZE);

	galois_single_divide( 10, 2 , 8 );
	galois_single_divide( 10, 2 , 16 );
	galois_single_divide( 10, 2 , 32 );
	// test XOR speed
	struct timespec ts = start_timer();
	for ( int i = 0; i < ROUNDS; i++ ) {
		galois_region_xor(buf, buf2, CSIZE);
	}
	printf( " XOR: %.4lf GB/s\n", CSIZE * ROUNDS * 1.0 / GB / get_elapsed_time(ts));

	ts = start_timer();
	for ( int i = 0; i < ROUNDS; i++ ) {
		memcpy( buf , buf2, CSIZE );
	}
	printf( " memcpy: %.4lf GB/s\n", CSIZE * ROUNDS * 1.0 / GB / get_elapsed_time(ts));


	free(buf);
	free(buf2);

	return 0;
}
