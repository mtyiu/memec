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
	fprintf( stderr, "%s [n] [k] [chunk size] [raid1|raid5|cauchy|rdp|rs|evenodd] [k data files] [(n-k) parity files]\n", command );
}

bool parse_args( int argc, char** argv ) {
	if ( argc <  argv_ofs )
		return false;

	n = atoi( argv[1] );
	k = atoi( argv[2] );
	csize = atoi( argv[3] );

	if ( strcmp ( argv[4], "raid1" ) == 0 ) {
		params.setScheme ( CS_RAID1 );
		scheme  = CS_RAID1;
	} else if ( strcmp ( argv[4], "raid5" ) == 0 ) {
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
	char *prev = ChunkUtil::getData( chunk );
	char *data;
	int prevPos = 0;
	if (id != 0) {
		printf("chunk %d:\n", id);
	}
	for (uint32_t i = 1; i < csize; i++) {
		data = ChunkUtil::getData( chunk );
		if ( data[i] != *prev ) {
			printf("\t\t[%02x] from %8d to %8d\n", *prev, prevPos, i-1);
			prevPos = i;
			prev = &data[i];
		}
	}
	printf("\t\t[%02x] from %8d to %8d\n", *prev, prevPos, csize - 1);
}

void print_chunks( Chunk *c1, Chunk *c2, uint32_t id = 0 ) {
	char *d1 = ChunkUtil::getData( c1 ), *d2 = ChunkUtil::getData( c2 );
	int count = 0;
	if (id != 0) {
		printf("chunk %d:\n", id);
	}
	for (uint32_t i = 0; i < csize; i++) {
		if ( d1[i] != d2[i] ) {
			printf("\t\t[%4d] input: %02x vs. expected %02x\n", i, ( unsigned char ) d1[i], ( unsigned char ) d2[i]);
			count++;
		}
	}
	printf("\t\tNumber of mismatch: %d / %d bytes; correct %%: %4.2lf%%\n", count, csize, ( double ) ( csize - count ) / csize * 100.0);
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
	TempChunkPool tempChunkPool;
	Chunk *c = tempChunkPool.alloc();;
	if ( n == 0 || k == 0 || csize == 0 || buf == NULL || chunks == NULL || pbuf == NULL )
		return false;

	for ( i = 0; i < n - k ; i++ ) {
		if ( memcmp( buf + ( i + k ) * csize, pbuf + i * csize, csize ) == 0 )
			fprintf( stdout, "\tchunk %d [PARITY] correct\n", i + k );
		else {
			all_correct = false;
			ChunkUtil::setSize( c, csize );
			ChunkUtil::copy( c, 0, buf + ( i + k ) * csize, csize );
			fprintf( stdout, "\tchunk %d [PARITY] wrong..\n", i + k );
			/*
			fprintf( stdout, "\tinput " );
			print_chunk( &c, i + k );
			fprintf( stdout, "\texpected " );
			print_chunk( chunks[ i + k ], i + k );
			*/
			print_chunks( c, chunks[ i + k ], i + k );
		}
	}
	tempChunkPool.free( c );

	return all_correct;
}

int main( int argc, char** argv ) {
	if ( ! parse_args( argc, argv ) ) {
		usage( argv[0] );
		return -1;
	}

	handle = Coding::instantiate( scheme, params, csize );

	TempChunkPool tempChunkPool;
	ChunkUtil::init( csize, k );

	chunks = new Chunk*[ n ];
	for ( uint32_t i = 0 ; i < n ; i ++ ) {
		chunks[i] = tempChunkPool.alloc();
		ChunkUtil::setSize( chunks[ i ], csize );
	}

	fprintf( stdout, "Start checking chunks\n" );
	read_chunks( argv );
	for (uint32_t i = 0 ; i < n - k ; i ++ ) {
		handle->encode( chunks, chunks[ k + i ], i + 1);
	}
	fprintf( stdout, "Verify chunks\n");
	if ( verify_chunks() ) {
		fprintf( stdout, "Passed verification!\n" );
		return 0;
	} else {
		fprintf( stdout, "Failed verification...\n" );
		return 1;
	}
}
