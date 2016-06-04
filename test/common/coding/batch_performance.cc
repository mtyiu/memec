#include <vector>
#include "../../../common/util/time.hh"
#include "common.hh"

#define CHUNK_SIZE  (4096)
#define C_K         (8)
#define C_M         (2)
#define K           (1000)

#define ROUNDS      (1000 * 1000 * 10)
#define WORKER_NUM	(12)
//#define LATENCY

void usage( char **argv ) {
    printf( "Usage: %s [rdp|evenodd|rs|cauchy] [update size (in bytes)] [batch size (in bytes)] [workers] [rounds]\n", argv[ 0 ] );
}

void* run( void *argv ) {
	struct timespec tt = start_timer();

    // init update offsets
    srand( 12345 );
    std::vector<uint32_t> offsets;
    uint32_t offset, remain;
    for( uint32_t i = 0; i < rounds; i++ ) {
        // never cross chunks
        offset = ( rand() % ( CHUNK_SIZE * C_K - updateSize ) );
        if ( ( offset + updateSize ) / CHUNK_SIZE != offset / CHUNK_SIZE ) {
            remain = CHUNK_SIZE - offset % CHUNK_SIZE;
            // either move to next chunk or move to ( current chunk - updateSize )
            offset += ( remain > updateSize / 2 ) ? remain : remain - updateSize;
        }
        offsets.push_back( offset );
    }

    // init chunks
    char buf[ CHUNK_SIZE ];
    Chunk ** chunks = ( Chunk ** ) malloc( sizeof( Chunk * ) * ( C_K + C_M ) );
    for( uint32_t i = 0; i < C_K + C_M; i++ ) {
        memset( buf, rand() % sizeof( char ), CHUNK_SIZE );
        chunks[ i ] = tempChunkPool.alloc();
        ChunkUtil::copy( chunks[ i ], 0, buf, CHUNK_SIZE );
    }

#ifdef LATENCY
    std::vector<double> latencies;
    struct timespec lt;
#endif

    // run the benchmark
#ifndef LATENCY
	struct timespec st = start_timer();
#endif
    if ( batchSize > 1 ) {
		uint32_t minOffset, maxOffset;
        // batch computation
        for( uint32_t i = 0; i < rounds; i++ ) {
#ifdef LATENCY
            lt = start_timer();
#endif
            if ( (i+1) % batchSize == 0 ) {
                // apply "data delta" in batch
				minOffset = offsets[ i ];
				maxOffset = offsets[ i ] + updateSize;
                for( uint32_t j = i / batchSize * batchSize; j <= i; j++ ) {
                    offset = offsets[ j ];
                    ChunkUtil::copy( chunks[ offset / CHUNK_SIZE ], offset % CHUNK_SIZE, buf, updateSize );
					minOffset = ( offset < minOffset ) ? offset : minOffset;
					maxOffset = ( offset + updateSize < maxOffset ) ? maxOffset : offset + updateSize ;
                }
                // compute new parity
                handle->encode( chunks, chunks[ C_K + 1 ], 2, minOffset, maxOffset );
            }
#ifdef LATENCY
            latencies.push_back( get_elapsed_time( lt ) * K * K );
#endif
        }
    } else {
        // compute on every update
        for( uint32_t i = 0; i < rounds; i++ ) {
#ifdef LATENCY
            lt = start_timer();
#endif
            offset = offsets[ i ];
            ChunkUtil::copy( chunks[ offset / CHUNK_SIZE ], offset % CHUNK_SIZE, buf, updateSize );
            handle->encode( chunks, chunks[ C_K + 1 ], 2, offset, offset + updateSize );
#ifdef LATENCY
            latencies.push_back( get_elapsed_time( lt ) * K * K );
#endif
        }
    }

#ifdef LATENCY
    // report latencies
    for( uint32_t i = 0; i < latencies.size(); i++ ) {
        printf( "[%5d] %.4lf us\n", i, latencies[ i ] );
    }
#else
    // report throughput
	report( get_elapsed_time( st ), rounds, C_K, updateSize * rounds );
#endif

	for( uint32_t i = 0; i < C_K + C_M; i++ ) {
		tempChunkPool.free( chunks[ i ] );
	}
	free( chunks );

	printf( "thread ran for %.4lf ms\n", get_elapsed_time( tt ) * K );

	return 0;
}

int main( int argc, char **argv ) {
    if( argc < 6 ) {
        usage( argv );
        return -1;
    }

    // initiation
	workerNum = WORKER_NUM;
	rounds = ROUNDS;
    parseInput( &argv[ 1 ], argc - 1 );
	params.setK( C_K );
	params.setN( C_K + C_M );
	params.setM( C_M );
	handle = Coding::instantiate( scheme, params, CHUNK_SIZE );

    initChunkSize( CHUNK_SIZE );

    // report parameters
    printf( "%20s: %10s (n,k)=(%d,%d)\n"
            "%20s: %10d B\n"
            "%20s: %10d B\n"
            "%20s: %10d\n"
            "%20s: %10d\n"
            ,"Coding scheme", argv[ 1 ], ( C_K + C_M ), C_K,
            "Update size", updateSize,
            "Batch size", batchSize * updateSize,
			"Worker Num", workerNum,
			"Rounds", rounds
    );

	pthread_t *workers = ( pthread_t* ) malloc( sizeof( pthread_t ) * workerNum );
	pthread_attr_t attr;
	pthread_attr_init( &attr );
	void *ret;

	for( uint32_t i = 0; i < workerNum; i++ ) {
		pthread_create( &workers[ i ], &attr, run, 0 );
	}

	for( uint32_t i = 0; i < workerNum; i++ ) {
		pthread_join( workers[ i ], &ret );
	}

	free( workers );

    return 0;
}
