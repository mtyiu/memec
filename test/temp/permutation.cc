#include <stdio.h>
#include <stdlib.h>

unsigned long choose( int n, int k ) {
	if ( ! k ) return 1;
	return( n * choose( n - 1, k - 1 ) / k );
}

int main( int argc, char **argv ) {
	int numServers, n, k, i, j;
	unsigned long tmp[ 2 ];
	struct timespec ts;

	if ( argc <= 3 )
		goto usage;

	numServers = atoi( argv[ 1 ] );
	n = atoi( argv[ 2 ] );
	k = atoi( argv[ 3 ] );

	printf( "Number of servers: %d\n"
	        "Number of data chunks and parity chunks in a stripe: %d\n"
	        "Number of parity chunks in a stripe: %d\n"
	        "Number of possible combinations: %lu\n",
	        numServers, n, k, choose( numServers, k ) * choose( numServers - k, n - k ) );

	return 0;

usage:
	fprintf( stderr, "Usage: %s [N] [n] [k]\n\n"
	                 "N: Number of servers\n"
	                 "n: Number of data chunks and parity chunks in a stripe\n"
	                 "k: Number of parity chunks in a stripe\n", argv[ 0 ] );
	return 1;
}
