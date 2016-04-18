#include <unordered_map>
#include "../../../common/ds/key.hh"
#include "../../../common/ds/key_value.hh"
#include "../../../common/hash/cuckoo_hash.hh"
#include "../../../common/hash/hash_func.hh"

// #define DUMP_ALL_DEBUG_MESSAGES

char *generateRandomString( size_t len, char *buf ) {
	static char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	static int count = strlen( alphabet );
	for ( size_t i = 0; i < len; i++ )
		buf[ i ] = alphabet[ rand() % count ];
	return buf;
}

int main( int argc, char **argv ) {
	if ( argc != 5 ) {
		fprintf( stderr, "Usage: %s [Object count] [Number of trials] [Key size] [Value size]\n", argv[ 0 ] );
		return 1;
	}

	struct {
		int count;
		int numTrials;
		uint8_t keySize;
		uint32_t valueSize;
		uint32_t objectSize;
	} config;
	struct {
		int success;
		int fail;
	} stat;

	// Parse arguments
	config.count = atoi( argv[ 1 ] );
	config.numTrials = atoi( argv[ 2 ] );
	config.keySize = ( uint8_t ) atoi( argv[ 3 ] );
	config.valueSize = ( uint32_t ) atoi( argv[ 4 ] );
	config.objectSize = config.keySize + config.valueSize + KEY_VALUE_METADATA_SIZE;

	printf(
		"Number of objects : %d\n"
		"Number of trials  : %d\n"
		"Key size          : %u bytes\n"
		"Value size        : %u bytes\n"
		"Object size       : %u bytes\n",
		config.count, config.numTrials, config.keySize, config.valueSize, config.objectSize
	);

	CuckooHash cuckooHash;
	std::unordered_map<Key, char *> map;
	Key k;
	char **buf, *key, *value;

	// Allocate memory
	key = ( char * ) malloc( sizeof( char ) * config.keySize );
	value = ( char * ) malloc( sizeof( char ) * config.valueSize );
	buf = ( char ** ) malloc( sizeof( char * ) * config.count );
	for ( int i = 0; i < config.count; i++ )
		buf[ i ] = ( char * ) malloc( sizeof( char ) * config.objectSize );

	// Generate objects
	srand( time( 0 ) );
#ifdef DUMP_ALL_DEBUG_MESSAGES
	printf( "\n---------- Object list ----------\n" );
#endif
	for ( int i = 0; i < config.count; i++ ) {
		generateRandomString( config.keySize, key );
		generateRandomString( config.valueSize, value );

		KeyValue::serialize( buf[ i ], key, config.keySize, value, config.valueSize );

		cuckooHash.insert( buf[ i ] );

		k.set( config.keySize, buf[ i ] + KEY_VALUE_METADATA_SIZE );
		map[ k ] = buf[ i ];

#ifdef DUMP_ALL_DEBUG_MESSAGES
		printf( "[%5d] %.*s : %.*s (0x%p)\n", i, config.keySize, key, config.valueSize, value, buf[ i ] );
#endif
	}

	free( key );
	free( value );

#ifdef DUMP_ALL_DEBUG_MESSAGES
	printf( "\n---------- Trials ----------\n" );
#endif
	stat.success = 0;
	stat.fail = 0;
	for ( int i = 0; i < config.numTrials; i++ ) {
		int index = rand() % config.count;
		char *p1, *p2;

		key = buf[ index ] + KEY_VALUE_METADATA_SIZE;
		k.set( config.keySize, key );

		p1 = cuckooHash.find( key, config.keySize );
		p2 = map[ k ];

		if ( p1 == p2 )
			stat.success++;
		else
			stat.fail++;

#ifdef DUMP_ALL_DEBUG_MESSAGES
		printf( "#%5d: Key = %.*s...[%s]\n", i, config.keySize, buf[ index ] + KEY_VALUE_METADATA_SIZE, p1 == p2 ? "OK" : "Failed" );
#else
		if ( i % 1000000 == 0 ) {
			printf( "\rProgress: %d / %d", i, config.numTrials );
			fflush( stdout );
		}
#endif
	}

	printf(
		"\n---------- Statistics ----------\n"
		"Success : %d / %d\n"
		"Failed  : %d / %d\n",
		stat.success, config.numTrials,
		stat.fail, config.numTrials
	);

	// Release memory
	for ( int i = 0; i < config.count; i++ )
		free( buf[ i ] );
	free( buf );

	return 0;
}
