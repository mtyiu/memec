#ifndef __TEST_COMMON_CODING_COMMON_HH__
#define __TEST_COMMON_CODING_COMMON_HH__

#include "../../../common/ds/chunk_pool.hh"

Coding* handle;
CodingParams params;
CodingScheme scheme;

uint32_t batchSize;
uint32_t updateSize;

TempChunkPool tempChunkPool;

void report( double duration, uint32_t rounds, uint32_t k, uint32_t writeSize = 0 ) {
    uint32_t chunkSize = ChunkUtil::chunkSize;
    if ( writeSize == 0 )
        writeSize = k * chunkSize;
	printf( " %.4lf Kop/s\t", rounds / 1000 / duration );
	printf( " %.4lf MB/s\n", writeSize / 1024.0 / 1024.0 * rounds / duration );
}

void printChunk( Chunk *chunk, uint32_t id = 0 ) {
	char *prev = ChunkUtil::getData( chunk ), *data;
	int prevPos = 0;
    uint32_t chunkSize = ChunkUtil::chunkSize;

	if (id != 0) {
		printf(" chunk %d:\n", id);
	}

	data = ChunkUtil::getData( chunk );

	for (uint32_t i = 1; i < chunkSize ; i++) {
		if ( data[i] != *prev ) {
			printf("[%x] from %d to %d\n", *prev, prevPos, i-1);
			prevPos = i;
			prev = &data[i];
		}
	}
	printf("[%x] from %d to %d\n", *prev, prevPos, chunkSize - 1);
}

bool parseInput( char **arg, int argc = 1 ) {
	if ( strcmp ( arg[ 0 ], "raid5" ) == 0 ) {
		params.setScheme ( CS_RAID5 );
		scheme  = CS_RAID5;
	} else if ( strcmp ( arg[ 0 ], "cauchy" ) == 0 ) {
		params.setScheme ( CS_CAUCHY );
		scheme  = CS_CAUCHY;
	} else if ( strcmp ( arg[ 0 ], "rdp" ) == 0 ) {
		params.setScheme ( CS_RDP );
		scheme  = CS_RDP;
	} else if ( strcmp ( arg[ 0 ], "rs" ) == 0 ) {
		params.setScheme ( CS_RS );
		scheme  = CS_RS;
	} else if ( strcmp ( arg[ 0 ], "evenodd" ) == 0 ) {
		params.setScheme ( CS_EVENODD );
		scheme  = CS_EVENODD;
	} else {
        printf( "Coding scheme [%s] unknown\n", arg[ 0 ] );
		return false;
    }

    for( int i = 1; i < argc; i++ ) {
        switch( i ) {
            case 1:
                updateSize = atoi( arg[ i ] );
                break;
            case 2:
                if( updateSize == 0 )
                    continue;
                batchSize = atoi( arg[ i ] ) / updateSize;
                break;
            default:
                printf( "Too many arguments.\n" );
                return false;
        }
    }

	return true;
}

void initChunkSize( uint32_t chunkSize ) {
    ChunkUtil::chunkSize = chunkSize;
}

#endif
