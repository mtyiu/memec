#include <stdio.h>
#include "../../../common/ds/id_generator.hh"

int main( int argc, char **argv ) {
	IDGenerator idGenerator;
	uint32_t num;

	printf( "Number of threads? ");
	fflush( stdout );
	scanf( "%u", &num );

	idGenerator.init( num );

	printf( "Thread ID? ");
	fflush( stdout );
	scanf( "%u", &num );

	while( 1 ) {
		printf( "\r%lu", idGenerator.nextVal( num ) );
	}
	return 0;
}
