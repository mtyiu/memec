#include <cstdio>
#include <cstdlib>
#include <stdint.h>
#include "../../../common/coding/coding.hh"
#include "../../../common/coding/all_coding.hh"
#include "../../../common/coding/coding_params.hh"

int argv_ofs = 5;
uint32_t n = 0, k = 0, csize = 0;
Chunk **chunks = NULL;
char *buf = NULL, *pbuf = NULL;

Coding* handle;
CodingParams params;
CodingScheme scheme;

void usage( char* command ) {
	fprintf( stderr, "%s [n] [k] [chunk size] [raid5|cauchy|rdp|rs|evenodd] [k data files] [(n-k) parity files]\n", command );
}

bool parse_args( int argc, char** argv ) {
	if ( argc <  argv_ofs )
		return false;

	n = atoi( argv[1] );
	k = atoi( argv[2] );
	csize = atoi( argv[3] );

	if ( strcmp ( argv[4], "raid5" ) == 0 ) {
		params.setScheme ( CS_RAID5 );
		scheme  = CS_RAID5;
	} else if ( strcmp ( argv[4], "cauchy" ) == 0 ) {
		params.setScheme ( CS_CAUCHY );
		scheme  = CS_CAUCHY;
	} else if ( strcmp ( argv[4], "rdp" ) == 0 ) {
		params.setScheme ( CS_RDP );
		scheme  = CS_RDP;
	} else if ( strcmp ( argv[4], "rs" ) == 0 ) {
		params.setScheme ( CS_RS );
		scheme  = CS_RS;
	} else if ( strcmp ( argv[4], "evenodd" ) == 0 ) {
		params.setScheme ( CS_EVENODD );
		scheme  = CS_EVENODD;
	} else
		return false;

	params.setN( n );
	params.setK( k );
	params.setM( n - k );

	return ( ( uint32_t ) argc >= n + argv_ofs );
}

void print_chunk( Chunk *chunk, uint32_t id = 0 ) {
	char *prev = chunk->getData();
	char *data;
	int prevPos = 0;
	if (id != 0) {
		printf("chunk %d:\n", id);
	}
	for (uint32_t i = 1; i < csize; i++) {
		data = chunk->getData();
		if ( data[i] != *prev ) {
			printf("\t\t[%02x] from %8d to %8d\n", *prev, prevPos, i-1);
			prevPos = i;
			prev = &data[i];
		}
	}
	printf("\t\t[%02x] from %8d to %8d\n", *prev, prevPos, csize - 1);
}

bool read_chunks( char** argv ) {
	uint32_t i;
	FILE *infile = NULL;
	if ( n == 0 || k == 0 || csize == 0 || buf == NULL || chunks == NULL || pbuf == NULL )
		return false;

	for ( i = 0; i < n ; i++ ) {
		fprintf( stdout, "\tinput file [%s] as chunk %d (%s)\n", argv[ i + argv_ofs ], i, (i<k)?"DATA":"PARITY" );
		infile = fopen( argv[ i + argv_ofs ], "r" );
		if ( infile == NULL ) {
			fprintf( stderr, "Cannot open file [%s]!!\n", argv[ i + argv_ofs ] );
			return false;
		}
		if ( fread( buf + i * csize, 1, csize, infile ) < 1 ) {
			fprintf( stderr, "Cannot read file [%s]!!\n", argv[ i + argv_ofs ] );
			return false;
		}
		fclose( infile );
		//print_chunk( chunks[ i ], i + 1 );
	}
	return true;
}

bool verify_chunks() {
	uint32_t i;
	bool all_correct = true;
	Chunk c;
	if ( n == 0 || k == 0 || csize == 0 || buf == NULL || chunks == NULL || pbuf == NULL )
		return false;

	for ( i = 0; i < n - k ; i++ ) {
		if ( memcmp( buf + ( i + k ) * csize, pbuf + i * csize, csize ) == 0 )
			fprintf( stdout, "\tchunk %d [PARITY] correct\n", i + k );
		else {
			all_correct = false;
			c.setSize( csize );
			c.setData( buf + ( i + k ) * csize );
			fprintf( stdout, "\tchunk %d [PARITY] wrong..\n", i + k );
			fprintf( stdout, "\tinput " );
			print_chunk( &c, i + k );
			fprintf( stdout, "\texpected " );
			print_chunk( chunks[ i + k ], i + k );
		}
	}

	return all_correct;
}

int main( int argc, char** argv ) {
	if ( ! parse_args( argc, argv ) ) {
		usage( argv[0] );
		return -1;
	}

	handle = Coding::instantiate( scheme, params, csize );

	chunks = new Chunk*[ n ];
	buf = ( char* ) calloc ( sizeof( char ) * csize, n );
	pbuf = ( char* ) calloc ( sizeof( char ) * csize, n - k );
	for ( uint32_t i = 0 ; i < n ; i ++ ) {
		chunks[i] = new Chunk();
		chunks[i]->setData( ( i < k )? buf + ( i * csize ) : pbuf + ( i - k ) * csize );
		chunks[i]->setSize( csize );
	}

	fprintf( stdout, "Start checking chunks\n" );
	read_chunks( argv );
	for (uint32_t i = 0 ; i < n - k ; i ++ ) {
		handle->encode( chunks, chunks[ k + i ], i + 1);
	}
	fprintf( stdout, "Verify chunks\n");
	if ( verify_chunks() ) {
		fprintf( stdout, "Passed verification!\n" );
	} else {
		fprintf( stdout, "Failed verification...\n" );
	}

	return 0;
}
