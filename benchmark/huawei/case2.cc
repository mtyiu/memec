#include <vector>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <arpa/inet.h>
#include <pthread.h>
#include "memec.hh"
#include "time.hh"

struct {
	// Configuration
	uint8_t keySize;
	uint32_t chunkSize;
	uint32_t batchSize;
	uint32_t dataSize;
	uint64_t totalSize;
	uint32_t addr;
	uint16_t port;
	uint32_t clientId;
	uint32_t numClients;
	uint32_t numThreads;
	// States
	uint64_t totalSizePerThread;
	uint32_t waiting;
	uint64_t sentBytes;
	struct timespec ts;
	pthread_mutex_t lock;
	pthread_cond_t waitCond, startCond;
} config;

char *getRandomString( size_t len, char *buf ) {
	static char alphabet[] = "0123456789abcdefghijklmnopqrstuvwxyz";
	static int count = strlen( alphabet );
	for ( size_t i = 0; i < len; i++ )
		buf[ i ] = alphabet[ rand() % count ];
	return buf;
}

char *getRandomLong( char *buf ) {
	*( ( uint32_t * )( buf ) ) = rand();
	*( ( uint32_t * )( buf + 4 ) ) = rand();
	return buf;
}

void runExperiment( MemEC *memec ) {
	uint8_t keySize = 8;
	uint32_t valueSize = config.dataSize - keySize;
	char *key, *value;
	uint64_t totalSize = 0;

	key = ( char * ) malloc( sizeof( char ) * keySize );
	value = ( char * ) malloc( sizeof( char ) * valueSize );
	getRandomString( valueSize, value );

	pthread_mutex_lock( &config.lock );
	config.waiting++;
	pthread_cond_signal( &config.waitCond );
	pthread_cond_wait( &config.startCond, &config.lock );
	pthread_mutex_unlock( &config.lock );

	while ( totalSize < config.totalSizePerThread ) {
		getRandomLong( key );
		memec->set( key, keySize, value, valueSize );
		totalSize += keySize + valueSize;
	}
	memec->flush();

	pthread_mutex_lock( &config.lock );
	config.sentBytes += totalSize;
	pthread_mutex_unlock( &config.lock );

	free( key );
	free( value );
}

void *run( void *argv ) {
	MemEC *memec = ( MemEC * ) argv;
	runExperiment( memec );
	pthread_exit( 0 );
	return 0;
}

void *stop( void *argv ) {
	MemEC *memec = ( MemEC * ) argv;
	memec->disconnect();
	pthread_exit( 0 );
	return 0;
}

int main( int argc, char **argv ) {
	if ( argc <= 10 ) {
		fprintf( stderr, "Usage: %s [Key size] [Chunk size] [Batch size] [Data size] [Total size] [Master IP] [Master port] [Client ID] [Total number of clients]\n", argv[ 0 ] );
		return 1;
	}
	struct sockaddr_in addr;

	config.keySize = atoi( argv[ 1 ] );
	config.chunkSize = atoi( argv[ 2 ] );
	config.batchSize = atoi( argv[ 3 ] );
	config.dataSize = atoi( argv[ 4 ] );
	config.totalSize = ( uint64_t ) atol( argv[ 5 ] );
	memset( &addr, 0, sizeof( addr ) );
	inet_pton( AF_INET, argv[ 6 ], &addr );
	config.addr = addr.sin_addr.s_addr;
	config.port = htons( atoi( argv[ 7 ] ) );
	config.clientId = ( uint32_t ) atol( argv[ 8 ] );
	config.numClients = ( uint32_t ) atol( argv[ 9 ] );
	config.numThreads = atoi( argv[ 10 ] );
	config.totalSizePerThread = config.totalSize / config.numThreads;
	config.waiting = 0;
	config.sentBytes = 0;
	pthread_mutex_init( &config.lock, 0 );
	pthread_cond_init( &config.waitCond, 0 );
	pthread_cond_init( &config.startCond, 0 );
	srand( time( 0 ) );

	int width = 20;
	char ipStr[ 16 ];
	inet_ntop( AF_INET, &addr, ipStr, sizeof( ipStr ) );
	printf(
		"---------- Configuration ----------\n"
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %lu\n"
		"%-*s : %s:%u\n"
		"%-*s : %u / %u\n"
		"%-*s : %u\n",
		width, "Key Size", config.keySize,
		width, "Chunk Size", config.chunkSize,
		width, "Batch Size", config.batchSize,
		width, "Data Size", config.dataSize,
		width, "Total Size", config.totalSize,
		width, "Master", ipStr, ntohs( config.port ),
		width, "Client ID", config.clientId, config.numClients,
		width, "Number of threads", config.numThreads
	);

	uint32_t fromId, toId;
	fromId = 4294967295 / config.numClients * config.clientId;
	toId = 4294967295 / config.numClients * ( config.clientId + 1 ) - 1;

	MemEC **memecs = new MemEC *[ config.numThreads ];
	pthread_t *tids = new pthread_t[ config.numThreads ];
	double elapsedTime;

	for ( uint32_t i = 0; i < config.numThreads; i++ ) {
		memecs[ i ] = new MemEC(
			config.keySize, config.chunkSize, config.batchSize,
			config.addr, config.port,
			fromId + ( ( toId - fromId ) / config.numThreads * i ),
			fromId + ( ( toId - fromId ) / config.numThreads * ( i + 1 ) - 1 )
		);
		memecs[ i ]->connect();
		pthread_create( tids + i, 0, run, ( void * ) memecs[ i ] );
	}

	pthread_mutex_lock( &config.lock );
	while ( config.waiting != config.numThreads )
		pthread_cond_wait( &config.waitCond, &config.lock );
	pthread_cond_broadcast( &config.startCond );
	config.ts = start_timer();
	pthread_mutex_unlock( &config.lock );

	for ( uint32_t i = 0; i < config.numThreads; i++ ) {
		pthread_join( tids[ i ], 0 );
	}
	elapsedTime = get_elapsed_time( config.ts );

	width = 20;
	printf(
		"\n---------- Statistics ----------\n"
		"%-*s : %8.3lf\n"
		"%-*s : %lu\n"
		"%-*s : %8.3lf\n"
		"%-*s : %8.3lf\n",
		width, "Elapsed time", elapsedTime,
		width, "Sent bytes", config.sentBytes,
		width, "Throughput (IOps)", ( double ) config.sentBytes / config.dataSize / elapsedTime,
		width, "Throughput (MBps)", ( double ) config.sentBytes / ( 1024 * 1024 ) / elapsedTime
	);

	printf( "\nCleaning up...\n" );
	for ( uint32_t i = 0; i < config.numThreads; i++ ) {
		pthread_create( tids + i, 0, stop, ( void * ) memecs[ i ] );
	}

	for ( uint32_t i = 0; i < config.numThreads; i++ ) {
		pthread_join( tids[ i ], 0 );
		delete memecs[ i ];
	}
	delete[] memecs;
	delete[] tids;

	return 0;
}
