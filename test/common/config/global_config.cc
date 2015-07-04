#include <cstdio>
#include "../../../common/config/global_config.hh"

int main( int argc, char **argv ) {
	if ( argc <= 2 ) {
		fprintf( stderr, "Usage: %s [Path] [Filename]\n", argv[ 0 ] );
		return 1;
	}

	GlobalConfig config;
	if ( config.parse( argv[ 1 ], argv[ 2 ] ) ) {
		config.print();
	} else {
		fprintf( stderr, "Cannot read input file.\n" );
	}

	return 0;
}
