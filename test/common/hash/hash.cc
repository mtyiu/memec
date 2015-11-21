#include <cstdio>
#include <cstdlib>
#include <cstring>
#include "../../../common/hash/hash_func.hh"

int main( int argc, char **argv ) {
	if ( argc <= 1 ) {
		fprintf( stderr, "Usage: %s [input string]\n", argv[ 0 ] );
		return 1;
	}
	char *input = argv[ 1 ];
	printf( "%u\n", HashFunc::hash( input, ( unsigned int ) strlen( input ) ) );
	return 0;
}
