#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../../common/ds/bitmask_array.hh"

int main( int argc, char **argv ) {
	int ret;
	size_t size, count, entry, bit;
	char op, buf[ 1024 ];
	bool run = true;
	BitmaskArray *bitmasks;

	printf( "Size of each entry? ");
	fflush( stdout );
	scanf( "%lu", &size );

	printf( "Number of entries? ");
	fflush( stdout );
	scanf( "%lu", &count );

	printf( "\nSize = %lu, count = %lu\n", size, count );
	bitmasks = new BitmaskArray( size, count );

	bitmasks->print();

	while( run ) {
		printf( "\ns[et]/u[nset]/c[lear]/e[xit] {entry} {bit}? " );
		fflush( stdout );
get_input:
		fgets( buf, 1024, stdin );
		sscanf( buf, "%c %lu %lu", &op, &entry, &bit );
		switch( op ) {
			case 's':
				bitmasks->set( entry, bit );
				break;
			case 'u':
				bitmasks->unset( entry, bit );
				break;
			case 'c':
				bitmasks->clear( entry );
				break;
			case 'e':
				run = false;
				break;
			default:
				goto get_input;
		}
		printf( "------------------------------------------------------------\n" );
		bitmasks->print();
		printf( "------------------------------------------------------------\n" );
	}
	delete bitmasks;
	return 0;
}