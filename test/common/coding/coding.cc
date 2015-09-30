#include <cstdlib>
#include <vector>
#include "../../../common/config/global_config.hh"
#include "../../../common/ds/bitmask_array.hh"
#include "../../../common/coding/coding_scheme.hh"
#include "../../../common/coding/all_coding.hh"
#include "../../../common/coding/coding_params.hh"

#define CHUNK_SIZE (4096)
#define C_K (4)
#define C_M (2)
#define FAIL (0)
#define FAIL2 (1)
#define FAIL3 (3)
// range of data modified within a chunk
#define MODIFY_ST (1000)
#define MODIFY_ED (2000)

Coding* handle;
CodingParams params;
CodingScheme scheme;

void usage( char* argv ) {
	fprintf( stderr, "Usage: %s [raid5|cauchy|rdp|rs|evenodd]\n", argv);
}

void printChunk( Chunk *chunk, uint32_t id = 0 ) {
	char *prev = chunk->getData(), *data;
	int prevPos = 0;
	if (id != 0) {
		printf(" chunk %d:\n", id);
	}
	data = chunk->getData();
	for (uint32_t i = 1; i < CHUNK_SIZE; i++) {
		if ( data[i] != *prev ) {
			printf("[%x] from %d to %d\n", *prev, prevPos, i-1);
			prevPos = i;
			prev = &data[i];
		}
	}
	printf("[%x] from %d to %d\n", *prev, prevPos, CHUNK_SIZE - 1);
}

bool parseInput( char* arg ) {
	if ( strcmp ( arg, "raid5" ) == 0 ) {
		params.setScheme ( CS_RAID5 );
		scheme  = CS_RAID5;
	} else if ( strcmp ( arg, "cauchy" ) == 0 ) {
		params.setScheme ( CS_CAUCHY );
		scheme  = CS_CAUCHY;
	} else if ( strcmp ( arg, "rdp" ) == 0 ) {
		params.setScheme ( CS_RDP );
		scheme  = CS_RDP;
	} else if ( strcmp ( arg, "rs" ) == 0 ) {
		params.setScheme ( CS_RS );
		scheme  = CS_RS;
	} else if ( strcmp ( arg, "evenodd" ) == 0 ) {
		params.setScheme ( CS_EVENODD );
		scheme  = CS_EVENODD;
	} else
		return false;

	return true;
}

int main( int argc, char **argv ) {

	if ( argc < 2 ) {
		usage( argv[ 0 ] );
		return 0;
	}

	if ( parseInput( argv[ 1 ] ) == false ) {
		usage( argv[ 0 ]);
		return -1;
	}

	char* buf = ( char* ) malloc ( sizeof( char ) * CHUNK_SIZE * ( C_K + C_M ) );
	char* readbuf = ( char* ) malloc ( sizeof( char ) * CHUNK_SIZE * ( C_M ) );
	Chunk ** chunks = ( Chunk ** ) malloc ( sizeof( Chunk* ) * ( C_K + C_M ) );
	BitmaskArray bitmap ( 1, C_K + C_M );

	// set up the chunks
	for ( uint32_t idx = 0 ; idx < C_K * 2 ; idx ++ ) {
		memset( buf + idx * CHUNK_SIZE / 2, idx / 2 * 3 + 5 , CHUNK_SIZE / 2 );
		if ( idx % 2 == 0 ) {
			chunks[ idx / 2 ] = ( Chunk* ) malloc ( sizeof( Chunk ) );
			chunks[ idx / 2 ]->setData( buf + ( idx / 2 ) * CHUNK_SIZE );
			chunks[ idx / 2 ]->setSize( CHUNK_SIZE );

			// mark the chunk status
			bitmap.set ( idx / 2 , 0 );
		}
	}

	for ( uint32_t idx = C_K ; idx < C_K + C_M ; idx ++ ) {
		memset( buf + idx * CHUNK_SIZE, 0, CHUNK_SIZE );
		chunks[ idx ] = ( Chunk* ) malloc ( sizeof( Chunk ) );
		chunks[ idx ]->setData( buf + idx * CHUNK_SIZE );
		chunks[ idx ]->setSize( CHUNK_SIZE );
		bitmap.set ( idx  , 0 );
	}

	std::vector< uint32_t > failed;
	failed.push_back( FAIL );
	failed.push_back( FAIL2 );
	failed.push_back( FAIL3 );

	uint32_t m = 0;
	switch ( scheme ) {
		case CS_RAID5:
			m = 1;
			printf( ">> encode K: %d   M: 1   ", C_K );
			break;
		case CS_RDP:
			m = 2;
			printf( ">> encode K: %d   M: 2   ", C_K );
			break;
		case CS_CAUCHY:
			m = C_M;
			printf( ">> encode K: %d   M: %d   ", C_K, C_M );
			break;
		case CS_RS:
			m = C_M;
			printf( ">> encode K: %d   M: %d   ", C_K, C_M );
			break;
		case CS_EVENODD:
			m = 2;
			printf( ">> encode K: %d   M: 2   ", C_K );
			break;
		default:
			return -1;
	}
	params.setN( C_K + m );
	params.setK( C_K );
	params.setM( m );
	handle = Coding::instantiate( scheme, params, CHUNK_SIZE );
	for (uint32_t idx = 0 ; idx < m ; idx ++ ) {
		handle->encode( chunks, chunks[ C_K + idx ], idx + 1 );
	}
	printf( " done.\n");

#if defined(TEST_DELTA) && FAIL < C_K
	// always modifies the first data chunk to be lost to test if parities are correctly updated
	// use read buf to hold the data delta and the parity delta
	memset ( readbuf , 0, CHUNK_SIZE * C_M );
	Chunk* shadowParity = new Chunk();
	shadowParity->setData( readbuf + CHUNK_SIZE );
	// zero block
	char *zero = new char [ CHUNK_SIZE ];
	memset( zero, 0, CHUNK_SIZE );
	printf( "delta at chunk %u from %u to %u\n", failed[ 0 ], MODIFY_ST, MODIFY_ED );
	// pointers to the start of data delta and modified data
	char *odata = chunks[ failed[ 0 ] ]->getData() + MODIFY_ST;
	char *ndata = readbuf + MODIFY_ST;
	uint32_t len = MODIFY_ED - MODIFY_ST;
	// get data delta
	memcpy( ndata , odata, len );
	memset( odata, 1, len );
	Coding::bitwiseXOR( ndata, odata, ndata, len );
	// zero unmodified data chunks
	for ( uint32_t idx = 0 ; idx < C_K ; idx ++ ) {
		if ( idx == failed[ 0 ] )
			chunks[ failed[ 0 ] ]->setData( readbuf );
		else
			chunks[ idx ]->setData( zero );
	}
	// get parity delta
	for ( uint32_t idx = 0 ; idx < m ; idx ++ ) {
		memset(shadowParity->getData(), 0, CHUNK_SIZE);
		handle->encode( chunks, shadowParity, idx + 1, failed[ 0 ] * CHUNK_SIZE + MODIFY_ST, failed[ 0 ] * CHUNK_SIZE + MODIFY_ED );
		Coding::bitwiseXOR( chunks[ C_K + idx ], shadowParity, chunks[ C_K + idx ], CHUNK_SIZE );
	}
	// restore the data chunks
	for ( uint32_t idx = 0 ; idx < C_K ; idx ++ ) {
		chunks[ idx ]->setData( buf + idx * CHUNK_SIZE );
	}
	delete [] shadowParity;
	delete [] zero;
#endif

	memset ( readbuf , 0, CHUNK_SIZE * C_M );

	printf( ">> fail disk %d ... ", failed[ 0 ] );
	chunks[ failed[ 0 ] ]->setData( readbuf );
	bitmap.unset ( failed[ 0 ] , 0 );

	handle->decode ( chunks, &bitmap );

	if ( memcmp ( readbuf, buf + FAIL * CHUNK_SIZE, CHUNK_SIZE ) == 0 ) {
		fprintf( stdout, "Data recovered\n" );
	} else {
		fprintf( stdout, "FAILED to recover data!! O: [%x] R: [%x]\n", *(buf + FAIL * CHUNK_SIZE), *(readbuf) );
		printChunk( chunks[ failed[ 0 ] ], failed[ 0 ] );
		return -1;
	}

	// reset size to avoid change in size by key-value compaction
	for ( uint32_t idx = 0 ; idx < C_K ; idx ++ ) {
		chunks[ idx ]->setSize( CHUNK_SIZE );
	}

	// double failure
	if ( scheme != CS_RAID5 ) {
		memset ( readbuf , 0, CHUNK_SIZE * C_M );
		printf( ">> fail disk %d, ", failed[ 0 ]);
		bitmap.unset ( failed[ 0 ], 0 );
		printf( "%d ... ", failed[ 1 ]);
		bitmap.unset ( failed[ 1 ], 0 );
		chunks[ failed[ 1 ] ]->setData( readbuf + CHUNK_SIZE );

		handle->decode ( chunks, &bitmap );

		if ( memcmp ( readbuf, buf + failed[ 0 ] * CHUNK_SIZE, CHUNK_SIZE ) == 0 ) {
			fprintf( stdout, "Data %d recovered ... ", failed[ 0 ] );
		} else {
			fprintf( stdout, "\nFAILED to recover data!! [%x] \n", *(buf + failed[ 0 ] * CHUNK_SIZE));
			printChunk( chunks[ failed[ 0 ] ], failed[ 0 ] );
			return -1;
		}
		if ( memcmp ( readbuf + CHUNK_SIZE , buf + ( failed[ 1 ] ) * CHUNK_SIZE , CHUNK_SIZE ) == 0 ) {
			fprintf( stdout, "Data %d recovered\n", failed[ 1 ] );
		} else {
			fprintf( stdout, "FAILED to recover data!! [%x] \n", *(buf + failed[ 1 ] * CHUNK_SIZE));
			printChunk( chunks[ failed[ 1 ] ], failed[ 1 ] );
			return -1;
		}
	}

	// reset size to avoid change in size by key-value compaction
	for ( uint32_t idx = 0 ; idx < C_K ; idx ++ ) {
		chunks[ idx ]->setSize( CHUNK_SIZE );
	}

	// triple failure
	if ( scheme != CS_RAID5 && scheme != CS_RDP && scheme != CS_EVENODD && C_M > 2) {
		memset ( readbuf , 0, CHUNK_SIZE * C_M );
		printf( ">> fail disk %d, ", failed[ 0 ]);
		bitmap.unset ( failed[ 0 ], 0 );
		printf( "%d, ", failed[ 1 ]);
		bitmap.unset ( failed[ 1 ], 0 );
		printf( "%d ... ", failed[ 2 ]);
		bitmap.unset ( failed[ 2 ], 0 );
		chunks[ failed[ 2 ] ]->setData( readbuf + CHUNK_SIZE * 2 );

		handle->decode ( chunks, &bitmap );

		if ( memcmp ( readbuf, buf + failed[ 0 ] * CHUNK_SIZE, CHUNK_SIZE ) == 0 ) {
			fprintf( stdout, "Data %d recovered ... ", failed[ 0 ] );
		} else {
			fprintf( stdout, "\nFAILED to recover data!! [%x] \n", *(buf + failed[ 0 ] * CHUNK_SIZE));
			printChunk( chunks[ failed[ 0 ] ], failed[ 0 ] );
			return -1;
		}
		if ( memcmp ( readbuf + CHUNK_SIZE , buf + ( failed[ 1 ] ) * CHUNK_SIZE , CHUNK_SIZE ) == 0 ) {
			fprintf( stdout, "Data %d recovered ... ", failed[ 1 ] );
		} else {
			fprintf( stdout, "FAILED to recover data!! [%x] \n", *(buf + failed[ 1 ] * CHUNK_SIZE));
			printChunk( chunks[ failed[ 1 ] ], failed[ 1 ] );
			return -1;
		}
		if ( memcmp ( readbuf + CHUNK_SIZE * 2, buf + ( failed[ 2 ] ) * CHUNK_SIZE , CHUNK_SIZE ) == 0 ) {
			fprintf( stdout, "Data %d recovered\n", failed[ 2 ] );
		} else {
			fprintf( stdout, "FAILED to recover data!! [%x] \n", *(buf + failed[ 2 ] * CHUNK_SIZE));
			printChunk( chunks[ failed[ 2 ] ], failed[ 2 ] );
			return -1;
		}
	}

	// clean up
	free( buf );
	free( readbuf );
	for ( uint32_t idx = 0 ; idx < C_M + C_K ; idx ++ ) {
		chunks[ idx ]->setData( NULL );
		free(chunks[ idx ]);
	}
	free( chunks );

	return 0;
}
