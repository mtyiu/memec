#include <vector>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <arpa/inet.h>
#include <pthread.h>
#include <unistd.h>
#include "memec.hh"
#include "time.hh"

#define WAIT_ACKS

struct {
	// Configuration
	uint8_t keySize;
	uint32_t chunkSize;
	uint32_t batchSize;
	uint32_t dataSize;
	uint32_t numWindows;
	uint32_t windowRate;
	uint32_t itemsPerWindow;
	uint32_t addr;
	uint16_t port;
	uint32_t clientId;
	uint32_t numClients;
	uint32_t numThreads;
	// States for upload threads
	uint32_t numRunning;
	uint32_t waiting;
	uint64_t sentBytes;
	struct timespec ts;
	pthread_mutex_t lock;
	pthread_cond_t waitCond, startCond, hasItemsCond;
	// States for window threads
	uint32_t remaining;
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

void *window( void *argv ) {
	pthread_mutex_lock( &config.lock );
	config.waiting++;
	pthread_cond_signal( &config.waitCond );
	pthread_cond_wait( &config.startCond, &config.lock );
	pthread_mutex_unlock( &config.lock );

	struct timespec ts = start_timer();
	double elapsedTimeInUs;
	double windowRate = config.windowRate / 1.0e6;
	double period = 1.0e6 / config.windowRate;
	for ( uint32_t i = 0; i < config.itemsPerWindow; i++ ) {
		elapsedTimeInUs = get_elapsed_time_in_us( ts );
		if ( i > elapsedTimeInUs * windowRate ) { // too fast
			usleep( period * ( i - elapsedTimeInUs * windowRate ) );
		}

		pthread_mutex_lock( &config.lock );
		config.remaining++;
		pthread_cond_broadcast( &config.hasItemsCond );
		pthread_mutex_unlock( &config.lock );

		// if ( i % config.windowRate == 0 ) {
		// 	printf( "\r[%lf] %u / %u", get_elapsed_time( ts ), i, config.itemsPerWindow );
		// 	fflush( stdout );
		// }
	}
	// printf( "\r[%lf] %u / %u\n", get_elapsed_time( ts ), config.itemsPerWindow, config.itemsPerWindow );
	fflush( stdout );

	pthread_mutex_lock( &config.lock );
	config.numRunning--;
	pthread_mutex_unlock( &config.lock );

	pthread_exit( 0 );
	return 0;
}

void *upload( void *argv ) {
	MemEC *memec = ( MemEC * ) argv;

	uint8_t keySize = 8;
	uint32_t valueSize = config.dataSize - keySize;
	char *key = 0, *value;
	uint64_t totalSize = 0;
	bool isEmpty = false;

	key = new char[ keySize ];
	value = new char[ valueSize ];
	getRandomString( valueSize, value );

	pthread_mutex_lock( &config.lock );
	config.waiting++;
	pthread_cond_signal( &config.waitCond );
	pthread_cond_wait( &config.startCond, &config.lock );
	pthread_mutex_unlock( &config.lock );

	while ( 1 ) {
		pthread_mutex_lock( &config.lock );
		while ( config.remaining == 0 ) {
			if ( ! config.numRunning ) {
				pthread_cond_broadcast( &config.hasItemsCond );
				isEmpty = true;
				break;
			}
			pthread_cond_wait( &config.hasItemsCond, &config.lock );
		}
		if ( ! isEmpty )
			config.remaining--;
		pthread_mutex_unlock( &config.lock );

		if ( ! isEmpty ) {
			getRandomLong( key );
			memec->set( key, keySize, value, valueSize );
			totalSize += keySize + valueSize;
		} else {
			break;
		}
	}
	memec->flush();

	pthread_mutex_lock( &config.lock );
	config.sentBytes += totalSize;
	pthread_mutex_unlock( &config.lock );

	delete[] key;
	delete[] value;

#ifdef WAIT_ACKS
	memec->disconnect();
#endif

	pthread_exit( 0 );
	return 0;
}

#ifndef WAIT_ACKS
void *stop( void *argv ) {
	MemEC *memec = ( MemEC * ) argv;
	memec->disconnect();
	pthread_exit( 0 );
	return 0;
}
#endif

int main( int argc, char **argv ) {
	if ( argc <= 12 ) {
		fprintf( stderr, "Usage: %s [Key size] [Chunk size] [Batch size] [Data size] [Number of windows] [Window rate] [Items per window] [Master IP] [Master port] [Client ID] [Total number of clients] [Number of threads]\n", argv[ 0 ] );
		return 1;
	}
	struct sockaddr_in addr;

	config.keySize = atoi( argv[ 1 ] );
	config.chunkSize = atoi( argv[ 2 ] );
	config.batchSize = atoi( argv[ 3 ] );
	config.dataSize = atoi( argv[ 4 ] );
	config.numWindows = atoi( argv[ 5 ] );
	config.windowRate = atoi( argv[ 6 ] );
	config.itemsPerWindow = atoi( argv[ 7 ] );
	memset( &addr, 0, sizeof( addr ) );
	inet_pton( AF_INET, argv[ 8 ], &( addr.sin_addr ) );
	config.addr = addr.sin_addr.s_addr;
	config.port = htons( atoi( argv[ 9 ] ) );
	config.clientId = ( uint32_t ) atol( argv[ 10 ] );
	config.numClients = ( uint32_t ) atol( argv[ 11 ] );
	config.numThreads = atoi( argv[ 12 ] );
	config.numRunning = config.numWindows;
	config.waiting = 0;
	config.sentBytes = 0;
	config.remaining = 0;
	pthread_mutex_init( &config.lock, 0 );
	pthread_cond_init( &config.waitCond, 0 );
	pthread_cond_init( &config.startCond, 0 );
	srand( time( 0 ) );

	int width = 20;
	char ipStr[ 16 ];
	inet_ntop( AF_INET, &( addr.sin_addr ), ipStr, sizeof( ipStr ) );
	printf(
		"---------- Configuration ----------\n"
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %s:%u\n"
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %u\n",
		width, "Key Size", config.keySize,
		width, "Chunk Size", config.chunkSize,
		width, "Batch Size", config.batchSize,
		width, "Data Size", config.dataSize,
		width, "Number of windows", config.numWindows,
		width, "Window rate", config.windowRate,
		width, "Items per window", config.itemsPerWindow,
		width, "Master", ipStr, ntohs( config.port ),
		width, "Client ID", config.clientId,
		width, "Number of clients", config.numClients,
		width, "Number of threads", config.numThreads
	);

	uint32_t fromId, toId;
	fromId = 4294967295 / config.numClients * config.clientId;
	toId = 4294967295 / config.numClients * ( config.clientId + 1 ) - 1;

	MemEC **memecs = new MemEC *[ config.numThreads ];
	pthread_t *tids = new pthread_t[ config.numThreads + config.numWindows ];
	double elapsedTime;

	// -------------------- Upload --------------------
	for ( uint32_t i = 0; i < config.numWindows; i++ ) {
		pthread_create( tids + i, 0, window, 0 );
	}

	for ( uint32_t i = 0; i < config.numThreads; i++ ) {
		memecs[ i ] = new MemEC(
			config.keySize, config.chunkSize, config.batchSize,
			config.addr, config.port,
			fromId + ( ( toId - fromId ) / config.numThreads * i ),
			fromId + ( ( toId - fromId ) / config.numThreads * ( i + 1 ) - 1 )
		);
		memecs[ i ]->connect();
		pthread_create( tids + i + config.numWindows, 0, upload, ( void * ) memecs[ i ] );
	}

	pthread_mutex_lock( &config.lock );
	while ( config.waiting != config.numThreads + config.numWindows )
		pthread_cond_wait( &config.waitCond, &config.lock );
	pthread_cond_broadcast( &config.startCond );
	config.ts = start_timer();
	pthread_mutex_unlock( &config.lock );

	for ( uint32_t i = 0; i < config.numThreads + config.numWindows; i++ ) {
		pthread_join( tids[ i ], 0 );
	}
	elapsedTime = get_elapsed_time( config.ts );

	width = 20;
	printf(
		"\n---------- Statistics (upload) ----------\n"
		"%-*s : %.3lf\n"
		"%-*s : %lu\n"
		"%-*s : %.3lf\n"
		"%-*s : %.3lf\n",
		width, "Elapsed time (s)", elapsedTime,
		width, "Sent bytes", config.sentBytes,
		width, "Throughput (IOps)", ( double ) config.sentBytes / config.dataSize / elapsedTime,
		width, "Throughput (MBps)", ( double ) config.sentBytes / ( 1024 * 1024 ) / elapsedTime
	);

	printf( "\nCleaning up...\n" );
	for ( uint32_t i = 0; i < config.numThreads; i++ ) {
#ifndef WAIT_ACKS
		pthread_create( tids + i + config.numWindows, 0, stop, ( void * ) memecs[ i ] );
	}

	for ( uint32_t i = 0; i < config.numThreads; i++ ) {
		pthread_join( tids[ i + config.numWindows ], 0 );
#endif
		delete memecs[ i ];
	}

	delete[] memecs;
	delete[] tids;

	return 0;
}
