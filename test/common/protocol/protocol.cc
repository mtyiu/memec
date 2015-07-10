#include <cstdio>
#include <cstdlib>
#include "../../../common/protocol/protocol.hh"

int main( int argc, char **argv ) {
	if ( argc <= 2 ) {
		fprintf( stderr, "Usage: %s [Key Size] [Value Size]\n", argv[ 0 ] );
		return 1;
	}
	int keySize = atoi( argv[ 1 ] );
	int valueSize = atoi( argv[ 2 ] );
	if ( keySize > 0 && valueSize > 0 ) {
		size_t ret = Protocol::getSuggestedBufferSize(
			( uint32_t ) keySize,
			( uint32_t ) valueSize
		);
		printf( "Suggested buffer size = %lu.\n", ret );
	} else {
		fprintf( stderr, "The input sizes should be positive." );
		return 1;
	}
	return 0;
}
