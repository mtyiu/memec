#include <vector>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <arpa/inet.h>
#include "memec.hh"

struct {
	uint8_t keySize;
	uint32_t chunkSize;
	uint32_t batchSize;
	uint32_t addr;
	uint16_t port;
	uint32_t fromId;
	uint32_t toId;
} config;

char *getRandomString( size_t len ) {
	static char alphabet[] = "0123456789abcdefghijklmnopqrstuvwxyz";
	static int count = strlen( alphabet );
	char *ret = ( char * ) malloc( sizeof( char ) * len );
	for ( size_t i = 0; i < len; i++ )
		ret[ i ] = alphabet[ rand() % count ];
	return ret;
}

void interactive( MemEC &memec ) {
	char buf[ 4096 ];
	char *key, *value, *valueUpdate;
	uint8_t keySize;
	uint32_t valueSize, valueUpdateSize, valueUpdateOffset;
	int numTokens;
	char *tokens[ 4 ] = { 0, 0, 0, 0 };
	bool isValid = true, ret;

	printf( "> " );
	fflush( stdout );
	while ( fgets( buf, sizeof( buf ), stdin ) != 0 ) {
		buf[ strlen( buf ) - 1 ] = 0;
		for ( numTokens = 0; numTokens < 4; numTokens++ ) {
			tokens[ numTokens ] = strtok( numTokens == 0 ? buf : 0, " " );
			if ( tokens[ numTokens ] == 0 )
				break;
		}

		if ( tokens[ 1 ] ) {
			key = tokens[ 1 ];
			keySize = strlen( key );
		} else {
			key = 0;
			keySize = 0;
		}

		value = 0;
		valueUpdate = 0;
		valueSize = 0;
		valueUpdateSize = 0;
		valueUpdateOffset = 0;

		if ( numTokens == 1 ) {
			if ( strcasecmp( tokens[ 0 ], "exit" ) == 0 ) {
				printf( "Bye.\n" );
				break;
			} else if ( strcasecmp( tokens[ 0 ], "pending" ) == 0 ) {
				memec.printPending();
				goto next;
			}
		} else if ( ! numTokens ) {
			goto next;
		}

		isValid = true;
		ret = false;
		switch( numTokens ) {
			case 0:
				break;
			case 1:
				isValid = false;
				break;
			case 2:
				if ( strcasecmp( tokens[ 0 ], "get" ) == 0 ) {
					ret = memec.get( key, keySize, value, valueSize );
					if ( ret )
						printf( "Key: %.*s; value: %.*s.\n", keySize, key, valueSize, value );
				} else if ( strcasecmp( tokens[ 0 ], "delete" ) == 0 ) {
					ret = memec.del( key, keySize );
				} else {
					isValid = false;
				}
				break;
			case 3:
				if ( strcasecmp( tokens[ 0 ], "set" ) == 0 ) {
					value = tokens[ 2 ];
					valueSize = strlen( value );
					ret = memec.set( key, keySize, value, valueSize );
				} else {
					isValid = false;
				}
				break;
			case 4:
				if ( strcasecmp( tokens[ 0 ], "update" ) == 0 ) {
					valueUpdate = tokens[ 2 ];
					valueUpdateSize = strlen( valueUpdate );
					valueUpdateOffset = atoi( tokens[ 3 ] );
					ret = memec.update( key, keySize, valueUpdate, valueUpdateSize, valueUpdateOffset );
				} else {
					isValid = false;
				}
				break;
		}

		if ( isValid ) {
			printf( "%s.\n", ret ? "Success" : "Fail" );
		} else {
			printf( "Invalid command!\n" );
		}

next:
		printf( "> " );
		fflush( stdout );
	}
}

void batch( MemEC &memec, size_t count, uint8_t keySize = 12, uint32_t valueSize = 500 ) {
	std::vector<char *> keys;
	std::vector<char *> values;
	char *key, *value;
	for ( size_t i = 0; i < count; i++ ) {
		key = getRandomString( keySize );
		value = getRandomString( valueSize );
		keys.push_back( key );
		values.push_back( value );
		if ( ! memec.set( key, keySize, value, valueSize ) )
			break;
	}
	memec.flush();
}

void randomWorkload( MemEC &memec, size_t count, uint8_t keySize = 12, uint32_t valueSize = 500 ) {
	std::vector<char *> keys;
	std::vector<char *> values;
	char *key, *value, *valueUpdate;
	uint32_t valueUpdateSize, valueUpdateOffset, tmpValueSize;
	uint32_t stat[ 2 ][ 4 ] = { { 0, 0, 0, 0 }, { 0, 0, 0, 0 } };
	int action, index;

	for ( size_t i = 0; i < count; i++ ) {
		action = keys.size() == 0 ? 0 : rand() % 4;
		switch( action ) {
			case 0:
				key = getRandomString( keySize );
				value = getRandomString( valueSize );
				keys.push_back( key );
				values.push_back( value );
				memec.set( key, keySize, value, valueSize );
				stat[ 0 ][ 0 ]++;
				stat[ 1 ][ 0 ]++;
				break;
			case 1:
				index = rand() % keys.size();
				key = keys[ index ];
				memec.get( key, keySize, value, tmpValueSize );
				if ( tmpValueSize != valueSize || strncmp( value, values[ index ], valueSize ) != 0 )
					fprintf( stderr, "[GET] Value mismatch: %.*s vs. %.*s\n", tmpValueSize, value, valueSize, values[ index ] );
				else
					stat[ 1 ][ 1 ]++;

				stat[ 0 ][ 1 ]++;
				break;
			case 2:
				index = rand() % keys.size();
				key = keys[ index ];
				valueUpdateSize = rand() % valueSize;
				valueUpdateOffset = valueSize == valueUpdateSize ? 0 : rand() % ( valueSize - valueUpdateSize );
				valueUpdate = getRandomString( valueUpdateSize );
				memec.update( key, keySize, valueUpdate, valueUpdateSize, valueUpdateOffset );
				memcpy( values[ index ] + valueUpdateOffset, valueUpdate, valueUpdateSize );
				free( valueUpdate );

				// Verify that the update is successful
				memec.get( key, keySize, value, tmpValueSize );
				if ( tmpValueSize != valueSize || strncmp( value, values[ index ], valueSize ) != 0 )
					fprintf( stderr, "[UPDATE] Value mismatch: %.*s vs. %.*s\n", valueSize, value, valueSize, values[ index ] );
				else
					stat[ 1 ][ 2 ]++;

				stat[ 0 ][ 2 ]++;
				break;
			case 3:
				index = rand() % keys.size();
				key = keys[ index ];
				memec.del( key, keySize );

				// Verify that the delete is successful
				memec.get( key, keySize, value, tmpValueSize );
				if ( tmpValueSize != 0 )
					fprintf( stderr, "[DELETE] The key %.*s cannot be deleted.\n", keySize, key );
				else
					stat[ 1 ][ 3 ]++;

				free( keys[ index ] );
				free( values[ index ] );
				keys.erase( keys.begin() + index );
				values.erase( values.begin() + index );

				stat[ 0 ][ 3 ]++;
				break;
			default:
				break;
		}
	}
	memec.flush();

	// Release allocated memory
	for ( size_t i = 0, len = keys.size(); i < len; i++ ) {
		free( keys[ i ] );
		free( values[ i ] );
	}

	int width = 25;
	printf(
		"---------- Report ----------\n"
		"%*s : %u / %u\n"
		"%*s : %u / %u\n"
		"%*s : %u / %u\n"
		"%*s : %u / %u\n",
		width, "Number of SET requests", stat[ 0 ][ 0 ], stat[ 1 ][ 0 ],
		width, "Number of GET requests", stat[ 0 ][ 1 ], stat[ 1 ][ 1 ],
		width, "Number of UPDATE requests", stat[ 0 ][ 2 ], stat[ 1 ][ 2 ],
		width, "Number of DELETE requests", stat[ 0 ][ 3 ], stat[ 1 ][ 3 ]
	);
}

int main( int argc, char **argv ) {
	if ( argc <= 7 ) {
		fprintf( stderr, "Usage: %s [Key size] [Chunk size] [Batch size] [Master IP] [Master port] [From ID] [To ID]\n", argv[ 0 ] );
		return 1;
	}
	struct sockaddr_in addr;

	config.keySize = atoi( argv[ 1 ] );
	config.chunkSize = atoi( argv[ 2 ] );
	config.batchSize = atoi( argv[ 3 ] );
	memset( &addr, 0, sizeof( addr ) );
	inet_pton( AF_INET, argv[ 4 ], &( addr.sin_addr ) );
	config.addr = addr.sin_addr.s_addr;
	config.port = htons( atoi( argv[ 5 ] ) );
	config.fromId = ( uint32_t ) atol( argv[ 6 ] );
	config.toId = ( uint32_t ) atol( argv[ 7 ] );

	int width = 10;
	char ipStr[ 16 ];
	inet_ntop( AF_INET, &( addr.sin_addr ), ipStr, sizeof( ipStr ) );
	printf(
		"---------- Configuration ----------\n"
		"%*s : %u\n"
		"%*s : %u\n"
		"%*s : %u\n"
		"%*s : %s:%u\n"
		"%*s : %u - %u\n",
		width, "Key Size", config.keySize,
		width, "Chunk Size", config.chunkSize,
		width, "Batch Size", config.batchSize,
		width, "Master", ipStr, ntohs( config.port ),
		width, "ID Range", config.fromId, config.toId
	);

	MemEC memec(
		config.keySize, config.chunkSize, config.batchSize,
		config.addr, config.port,
		config.fromId, config.toId
	);

	if ( memec.connect() )
		printf( "Connected.\n" );

	srand( time( 0 ) );
	// batch( memec, 500000 );
	// randomWorkload( memec, 50000 );
	interactive( memec );

	memec.disconnect();

	return 0;
}
