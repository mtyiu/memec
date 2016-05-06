#include <cstdlib>
#include <vector>
#include "../../../common/config/global_config.hh"
#include "../../../common/ds/bitmask_array.hh"
#include "../../../common/coding/coding_scheme.hh"
#include "../../../common/coding/all_coding.hh"
#include "../../../common/coding/coding_params.hh"
#include "../../../common/util/time.hh"

#define CHUNK_SIZE (8192)
#define C_K (4)
#define C_M (2)
#define FAIL (0)
#define FAIL2 (1)
#define FAIL3 (3)
#define ROUNDS (5000)

Coding* handle;
CodingParams params;
CodingScheme scheme;


void usage( char* argv ) {
	fprintf( stderr, "Usage: %s [raid5|cauchy|rdp|rs|evenodd]\n", argv);
}

void report( double duration ) {
	printf( " %.4lf Kop/s\t", ROUNDS / 1000 / duration );
	printf( " %.4lf MB/s\n", C_K * CHUNK_SIZE / 1024.0 / 1024.0 * ROUNDS / duration );
}

void printChunk( Chunk *chunk, uint32_t id = 0 ) {
	char *prev = ChunkUtil::getData( chunk ), *data;
	int prevPos = 0;
	if (id != 0) {
		printf(" chunk %d:\n", id);
	}
	data = ChunkUtil::getData( chunk );
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

	TempChunkPool tempChunkPool;

	// set up the chunks
	for ( uint32_t idx = 0 ; idx < C_K * 2 ; idx ++ ) {
		memset( buf + idx * CHUNK_SIZE / 2, idx / 2 * 3 + 5 , CHUNK_SIZE / 2 );
		if ( idx % 2 == 0 ) {
			chunks[ idx / 2 ] = tempChunkPool.alloc();

			// mark the chunk status
			bitmap.set ( idx / 2 , 0 );
		}
	}

	for ( uint32_t idx = C_K ; idx < C_K + C_M ; idx ++ ) {
		chunks[ idx ] = tempChunkPool.alloc();
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
	params.setK( C_K );
	params.setN( C_K + m );
	params.setM( m );
	handle = Coding::instantiate( scheme, params, CHUNK_SIZE );

	struct timespec st = start_timer();
	for ( uint32_t round = 0 ; round < ROUNDS ; round ++ ) {
		for (uint32_t idx = 0 ; idx < m ; idx ++ ) {
			handle->encode( chunks, chunks[ C_K + idx ], idx + 1 );
		}
	}
	// report time per encoding operations
	report( get_elapsed_time( st ) / m );

	memset ( readbuf , 0, CHUNK_SIZE * C_M );

	printf( ">> fail disk %d ... ", failed[ 0 ] );
	ChunkUtil::copy( chunks[ failed[ 0 ] ], 0, readbuf, CHUNK_SIZE );
	bitmap.unset ( failed[ 0 ] , 0 );

	st = start_timer();
	for ( uint32_t round = 0 ; round < ROUNDS ; round ++ ) {
		handle->decode ( chunks, &bitmap );
	}
	report( get_elapsed_time( st ) );

	// double failure
	if ( scheme != CS_RAID5 ) {
		memset ( readbuf , 0, CHUNK_SIZE * C_M );
		printf( ">> fail disk %d, ", failed[ 0 ]);
		bitmap.unset ( failed[ 0 ], 0 );
		printf( "%d ... ", failed[ 1 ]);
		bitmap.unset ( failed[ 1 ], 0 );
		ChunkUtil::copy( chunks[ failed[ 1 ] ], 0, readbuf + CHUNK_SIZE, CHUNK_SIZE );

		st = start_timer();
		for ( uint32_t round = 0 ; round < ROUNDS ; round ++ ) {
			handle->decode ( chunks, &bitmap );
		}
		report( get_elapsed_time( st ) );
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
		ChunkUtil::copy( chunks[ failed[ 2 ] ], 0, readbuf + CHUNK_SIZE * 2, CHUNK_SIZE );
		st = start_timer();
		for ( uint32_t round = 0 ; round < ROUNDS ; round ++ ) {
			handle->decode ( chunks, &bitmap );
		}
		report( get_elapsed_time( st ) );

	}

	// clean up
	free( buf );
	free( readbuf );
	for ( uint32_t idx = 0 ; idx < C_M + C_K ; idx ++ ) {
		tempChunkPool.free( chunks[ idx ] );
	}
	free( chunks );

	return 0;
}
