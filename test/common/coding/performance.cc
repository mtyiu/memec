#include <cstdlib>
#include <vector>
#include "../../../common/config/global_config.hh"
#include "../../../common/ds/bitmask_array.hh"
#include "../../../common/coding/coding_scheme.hh"
#include "../../../common/coding/all_coding.hh"
#include "../../../common/coding/coding_params.hh"
#include "../../../common/util/time.hh"
#include "common.hh"

#define CHUNK_SIZE (8192)
#define C_K (4)
#define C_M (2)
#define FAIL (0)
#define FAIL2 (1)
#define FAIL3 (3)
#define ROUNDS (1000 * 1000)

void usage( char* argv ) {
	fprintf( stderr, "Usage: %s [raid5|cauchy|rdp|rs|evenodd]\n", argv);
}

int main( int argc, char **argv ) {

	if ( argc < 2 ) {
		usage( argv[ 0 ] );
		return 0;
	}

	if ( parseInput( &argv[ 1 ] ) == false ) {
		usage( argv[ 0 ]);
		return -1;
	}

	char* buf = ( char* ) malloc ( sizeof( char ) * CHUNK_SIZE * ( C_K + C_M ) );
	char* readbuf = ( char* ) malloc ( sizeof( char ) * CHUNK_SIZE * ( C_M ) );
	Chunk ** chunks = ( Chunk ** ) malloc ( sizeof( Chunk* ) * ( C_K + C_M ) );
	BitmaskArray bitmap ( 1, C_K + C_M );

    initChunkSize( CHUNK_SIZE );

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
	report( get_elapsed_time( st ) / m, ROUNDS, C_K );

	memset ( readbuf , 0, CHUNK_SIZE * C_M );

	printf( ">> fail disk %d ... ", failed[ 0 ] );
	ChunkUtil::copy( chunks[ failed[ 0 ] ], 0, readbuf, CHUNK_SIZE );
	bitmap.unset ( failed[ 0 ] , 0 );

	uint32_t round;
	st = start_timer();
	for ( round = 0 ; round < ROUNDS ; round ++ ) {
		if ( handle->decode ( chunks, &bitmap ) == false )
			break;
	}
	if ( round == ROUNDS )
		report( get_elapsed_time( st ), ROUNDS, C_K );
	else
		printf( ">> !! failed to decode\n" );

	// double failure
	if ( scheme != CS_RAID5 ) {
		memset ( readbuf , 0, CHUNK_SIZE * C_M );
		printf( ">> fail disk %d, ", failed[ 0 ]);
		bitmap.unset ( failed[ 0 ], 0 );
		printf( "%d ... ", failed[ 1 ]);
		bitmap.unset ( failed[ 1 ], 0 );
		ChunkUtil::copy( chunks[ failed[ 1 ] ], 0, readbuf + CHUNK_SIZE, CHUNK_SIZE );

		st = start_timer();
		for ( round = 0 ; round < ROUNDS ; round ++ ) {
			if ( handle->decode ( chunks, &bitmap ) == false )
				break;
		}
		if ( round == ROUNDS )
			report( get_elapsed_time( st ), ROUNDS, C_K );
		else
			printf( ">> !! failed to decode\n" );
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
		for ( round = 0 ; round < ROUNDS ; round ++ ) {
			handle->decode ( chunks, &bitmap );
		}
		if ( round == ROUNDS )
			report( get_elapsed_time( st ), ROUNDS, C_K );
		else
			printf( ">> !! failed to decode\n" );
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
