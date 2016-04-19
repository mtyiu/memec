#include <pthread.h>
#include <string.h>
#include <assert.h>
#include "../../../server/buffer/chunk_pool.hh"

uint32_t chunkSize;
uint32_t capacity;
uint32_t numTrials;
uint32_t numThreads;
ChunkPool chunkPool;
pthread_mutex_t lock;

void *run( void *argv ) {
	uint32_t listId = ( uint32_t ) ( rand() % 2000 );
	char **chunks = ( char ** ) malloc( sizeof( char * ) * numTrials );
	for ( uint32_t i = 0; i < numTrials; i++ ) {
		chunks[ i ] = chunkPool.alloc( listId, i, i );
		if ( chunks[ i ] )
			memset( chunks[ i ] + CHUNK_METADATA_SIZE, 255, chunkSize );
	}

	pthread_mutex_lock( &lock );
	printf( "* List ID = %u *\n", listId );
	for ( uint32_t i = 0; i < numTrials; i++ ) {
		if ( chunks[ i ] ) {
			// Check metadata
			uint32_t *metadata = ( uint32_t * ) chunks[ i ];
			printf( "#%u: [%u, %u; size = %u] 0x%p\n", i, metadata[ 0 ], metadata[ 1 ], metadata[ 2 ], chunks[ i ] );

			assert( listId == metadata[ 0 ] );

			// Check getChunk() correctness
			uint32_t offset = rand() % chunkSize;
			struct {
				uint32_t listId;
				uint32_t stripeId;
				uint32_t size;
				uint32_t offset;
				char *chunk;
			} result;
			result.chunk = chunkPool.getChunk( chunks[ i ] + offset, &result.listId, &result.stripeId, &result.size, &result.offset );

			assert( result.listId   == metadata[ 0 ] );
			assert( result.stripeId == metadata[ 1 ] );
			assert( result.size     == metadata[ 2 ] );
			assert( result.offset   == offset        );
			assert( result.chunk    == chunks[ i ]   );

			// for ( uint32_t j = 0; j < chunkSize; j++ )
			// 	printf( "%d ", *( chunks[ i ] + CHUNK_METADATA_SIZE + j ) );
			// printf( "\n" );
		} else {
			printf( "#%u: Cannot allocate memory\n", i );
		}
	}
	printf( "\n" );
	fflush( stdout );
	pthread_mutex_unlock( &lock );

	free( chunks );

	pthread_exit( 0 );
	return 0;
}

int main( int argc, char **argv ) {
	if ( argc != 5 ) {
		fprintf( stderr, "Usage: %s [chunk size] [capacity] [number of trials] [number of threads]\n", argv[ 0 ] );
		return 1;
	}

	chunkSize  = ( uint32_t ) atoi( argv[ 1 ] );
	capacity   = ( uint32_t ) atoi( argv[ 2 ] );
	numTrials  = ( uint32_t ) atoi( argv[ 3 ] );
	numThreads = ( uint32_t ) atoi( argv[ 4 ] );
	pthread_mutex_init( &lock, 0 );
	srand( time( 0 ) );

	chunkPool.init( chunkSize, capacity );

	chunkPool.print();
	printf( "\n" );

	pthread_t *tids = ( pthread_t * ) malloc( sizeof( pthread_t ) * numThreads );

	for ( uint32_t i = 0; i < numThreads; i++ )
		pthread_create( tids + i, 0, run, 0 );

	for ( uint32_t i = 0; i < numThreads; i++ )
		pthread_join( tids[ i ], 0 );

	free( tids );

	printf( "\n" );
	chunkPool.print();

	return 0;
}
