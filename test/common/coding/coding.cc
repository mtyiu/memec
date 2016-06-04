#include <algorithm>
#include <cstdlib>
#include <vector>
#include "../../../common/config/global_config.hh"
#include "../../../common/ds/bitmask_array.hh"
#include "../../../common/coding/coding_scheme.hh"
#include "../../../common/coding/all_coding.hh"
#include "../../../common/coding/coding_params.hh"

#define CHUNK_SIZE (4096)
#define C_K (8)
#define C_M (3)
#define FAIL (1)
#define FAIL2 (2)
#define FAIL3 (3)
// range of data modified within a chunk
#define MODIFY_ST (3012)
#define MODIFY_ED (4096)

Coding* handle;
CodingParams params;
CodingScheme scheme;

void usage( char* argv ) {
	fprintf( stderr, "Usage: %s [raid5|cauchy|rdp|rs|evenodd]\n", argv);
}

void printChunk( char *chunk, uint32_t id = 0 ) {
	char *prev = chunk;
	int prevPos = 0;
	if (id != 0) {
		printf(" chunk %d:\n", id);
	}
	for (uint32_t i = 1; i < CHUNK_SIZE; i++) {
		if ( chunk[i] != *prev ) {
			printf("[%x] from %d to %d\n", *prev, prevPos, i-1);
			prevPos = i;
			prev = &chunk[i];
		}
	}
	printf("[%x] from %d to %d\n", *prev, prevPos, CHUNK_SIZE - 1);
}

void printChunk( Chunk *chunk, uint32_t id = 0 ) {
	printChunk( ChunkUtil::getData( chunk ), id );
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

void zeroChunks( Chunk **chunks ) {
	for ( int i = 0; i < C_K + C_M; i++ ) {
		ChunkUtil::clear( chunks[ i ] );
	}
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

	char *buf = ( char* ) malloc ( sizeof( char ) * CHUNK_SIZE * ( C_K + C_M ) );
	Chunk **chunks = ( Chunk ** ) malloc ( sizeof( Chunk* ) * ( C_K + C_M ) );
	Chunk **readbuf = ( Chunk ** ) malloc ( sizeof( Chunk* ) * ( C_K + C_M ) );
	BitmaskArray bitmap ( 1, C_K + C_M );

	TempChunkPool tempChunkPool;
	ChunkUtil::init( CHUNK_SIZE, C_K );

	// set up the chunks
	for ( uint32_t idx = 0 ; idx < C_K * 2 ; idx ++ ) {
		if ( idx % 2 == 0 ) {
			chunks[ idx / 2 ] = tempChunkPool.alloc();
			readbuf[ idx / 2 ] = tempChunkPool.alloc();
			// mark the chunk status
			bitmap.set ( idx / 2 , 0 );
		}
		// write data into "buf" and "chunks"
		memset( buf + idx * CHUNK_SIZE / 2, idx / 2 * 3 + 5 , CHUNK_SIZE / 2 );
		ChunkUtil::copy( chunks[ idx / 2 ], ( idx % 2 ) * ( CHUNK_SIZE / 2 ), buf + idx * CHUNK_SIZE / 2, CHUNK_SIZE / 2 );
	}

	for ( uint32_t idx = C_K ; idx < C_K + C_M ; idx ++ ) {
		chunks[ idx ] = tempChunkPool.alloc();
		readbuf[ idx ] = tempChunkPool.alloc();
		bitmap.set ( idx  , 0 );
	}

	std::vector< uint32_t > failed;
	failed.push_back( FAIL );
	failed.push_back( FAIL2 );
	failed.push_back( FAIL3 );
	std::sort( failed.begin(), failed.end() );

	uint32_t m = 0;
	switch ( scheme ) {
		case CS_RAID5:
			m = 1;
			printf( ">> encode K: %d   M: 1   ", C_K );
			break;
		case CS_EVENODD:
			m = 2;
			printf( ">> encode K: %d   M: 2   ", C_K );
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

#if defined(TEST_DELTA) && FAIL < C_K && ( MODIFY_ED <= CHUNK_SIZE )
	// always modifies the first data chunk to be lost to test if parities are correctly updated
	// use read buf to hold the data delta and the parity delta
	
	zeroChunks( readbuf );
	for ( uint32_t i = 0; i < m; i++ ) {
		// zero block
		printf( "Update data at chunk %u from %u to %u\n", failed[ i ], MODIFY_ST, MODIFY_ED );
		// get pointers to where data is going to be updated 
		char *oldData = ChunkUtil::getData( chunks[ failed[ i ] ] ) + MODIFY_ST;
		char *newData = ChunkUtil::getData( readbuf[ failed[ i ] ] ) + MODIFY_ST;
		// get the delta len
		uint32_t len = MODIFY_ED - MODIFY_ST;
		// copy the old data out
		memcpy( newData , oldData, len );
		// update
		memset( oldData, i+1, len );
		// compute data delta
		Coding::bitwiseXOR( newData, oldData, newData, len );
	}
	// compute and apply parity delta
	for ( uint32_t idx = 0 ; idx < m ; idx ++ ) {
		handle->encode( readbuf, readbuf[ C_K + idx ], idx + 1, failed[ 0 ] * CHUNK_SIZE + MODIFY_ST, failed[ m - 1 ] * CHUNK_SIZE + MODIFY_ED );
		Coding::bitwiseXOR( chunks[ C_K + idx ], readbuf[ C_K + idx ], chunks[ C_K + idx ], CHUNK_SIZE );
	}
	zeroChunks( readbuf );
#endif

	printf( ">> fail disk %d ... ", failed[ 0 ] );
	for ( uint32_t idx = 0; idx < C_K + m; idx ++ ) {
		if ( idx == failed[ 0 ] )
			continue;
		ChunkUtil::copy( readbuf[ idx ], 0, ChunkUtil::getData( chunks[ idx ] ), CHUNK_SIZE );
	}
	bitmap.unset ( failed[ 0 ] , 0 );

	// do recovery
	handle->decode ( readbuf, &bitmap );

	// check results
	char *recoveredChunk =  ChunkUtil::getData( readbuf[ failed[ 0 ] ] );
	char *oldChunk =  ChunkUtil::getData( chunks[ failed[ 0 ] ] );
	if ( memcmp ( oldChunk , recoveredChunk, CHUNK_SIZE ) == 0 ) {
		fprintf( stdout, "Data recovered" );
	} else {
		fprintf( stdout, "FAILED to recover data!!\n correct:\n" );
		printChunk( chunks[ failed[ 0 ] ], failed[ 0 ] );
		fprintf( stdout, " vs recovered:\n" );
		printChunk( readbuf[ failed[ 0 ] ] , failed[ 0 ] );
		return -1;
	}
	fprintf( stdout, "\n" );

	// double failure
	if ( scheme != CS_RAID5 ) {
		zeroChunks( readbuf );
		printf( ">> fail disk %d, ", failed[ 0 ]);
		bitmap.unset ( failed[ 0 ], 0 );
		printf( "%d ... ", failed[ 1 ]);
		bitmap.unset ( failed[ 1 ], 0 );
		for ( uint32_t idx = 0; idx < C_K + m; idx ++ ) {
			if ( idx == failed[ 0 ] || idx == failed[ 1 ] )
				continue;
			ChunkUtil::copy( readbuf[ idx ], 0, ChunkUtil::getData( chunks[ idx ] ), CHUNK_SIZE );
		}

		// do recovery
		handle->decode ( readbuf, &bitmap );

		// check results
		for ( int i = 0; i < 2; i++ ) {
			recoveredChunk =  ChunkUtil::getData( readbuf[ failed[ i ] ] );
			oldChunk =  ChunkUtil::getData( chunks[ failed[ i ] ] );
			if ( memcmp ( recoveredChunk, oldChunk, CHUNK_SIZE ) == 0 ) {
				fprintf( stdout, ".. Data %d recovered ", failed[ i ] );
			} else {
				fprintf( stdout, "FAILED to recover data!!\n correct:\n" );
				printChunk( chunks[ failed[ i ] ], failed[ i ] );
				fprintf( stdout, " vs recovered:\n" );
				printChunk( readbuf[ failed[ i ] ] , failed[ i ] );
				return -1;
			}
		}
		fprintf( stdout, "\n" );
	}

	// triple failure
	if ( scheme != CS_RAID5 && scheme != CS_RDP && scheme != CS_EVENODD && m > 2 ) {
		zeroChunks( readbuf );
		printf( ">> fail disk %d, ", failed[ 0 ]);
		bitmap.unset ( failed[ 0 ], 0 );
		printf( "%d, ", failed[ 1 ]);
		bitmap.unset ( failed[ 1 ], 0 );
		printf( "%d ... ", failed[ 2 ]);
		bitmap.unset ( failed[ 2 ], 0 );
		for ( uint32_t idx = 0; idx < C_K + m; idx ++ ) {
			if ( idx == failed[ 0 ] || idx == failed[ 1 ] || idx == failed[ 2 ] )
				continue;
			ChunkUtil::copy( readbuf[ idx ], 0, ChunkUtil::getData( chunks[ idx ] ), CHUNK_SIZE );
		}

		// do recovery
		handle->decode ( readbuf, &bitmap );

		// check results
		for ( int i = 0; i < 3; i++ ) {
			recoveredChunk =  ChunkUtil::getData( readbuf[ failed[ i ] ] );
			oldChunk =  ChunkUtil::getData( chunks[ failed[ i ] ] );
			if ( memcmp ( recoveredChunk, oldChunk, CHUNK_SIZE ) == 0 ) {
				fprintf( stdout, ".. Data %d recovered ", failed[ i ] );
			} else {
				fprintf( stdout, "FAILED to recover data!!\n correct:\n" );
				printChunk( chunks[ failed[ i ] ], failed[ i ] );
				fprintf( stdout, " vs recovered:\n" );
				printChunk( readbuf[ failed[ i ] ] , failed[ i ] );
				return -1;
			}
		}
		fprintf( stdout, "\n" );
	}

	// clean up
	free( buf );
	for ( uint32_t idx = 0 ; idx < m + C_K ; idx ++ ) {
		tempChunkPool.free( chunks[ idx ]);
		tempChunkPool.free( readbuf[ idx ] );
	}
	free( chunks );
	free( readbuf );

	return 0;
}
