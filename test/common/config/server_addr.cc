#include <cstdio>
#include "../../../common/config/server_addr.hh"

int main( int argc, char **argv ) {
	if ( argc <= 2 ) {
		fprintf( stderr, "Usage: %s [Name] [Address]\n", argv[ 0 ] );
		return 1;
	}

	ServerAddr addr;
	if ( addr.parse( argv[ 1 ], argv[ 2 ] ) ) {
		addr.print();
	} else {
		fprintf( stderr, "Invalid arguments!\n" );
	}

	return 0;
}
