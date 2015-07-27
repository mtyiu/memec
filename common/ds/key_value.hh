#ifndef __COMMON_DS_KEY_VALUE_HH__
#define __COMMON_DS_KEY_VALUE_HH__

#include <stdint.h>
#include <arpa/inet.h>

class KeyValue {
public:
	char *data;

	char *serialize( char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
		return KeyValue::serialize( this->data, key, keySize, value, valueSize );
	}

	static char *serialize( char *data, char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
		data[ 0 ] = ( char * ) keySize;
		
		valueSize = htonl( valueSize );
		data[ 1 ] = ( valueSize >> 24 ) & 0xFF;
		data[ 2 ] = ( valueSize >> 16 ) & 0xFF;
		data[ 3 ] = ( valueSize >> 8 ) & 0xFF;
		valueSize = ntohl( valueSize );

		memcpy( data + 4, key, keySize );
		memcpy( data + 4 + keySize, value, valueSize );

		return data;
	}

	char *deserialize( char &*key, uint8_t &keySize, char &*value, uint32_t &valueSize ) {
		return KeyValue::deserialize( this->data, key, keySize, value, valueSize );
	}

	static char *deserialize( char *data, char &*key, uint8_t &keySize, char &*value, uint32_t &valueSize ) {
		keySize = *( ( uint8_t * ) data[ 0 ] );

		valueSize = 0;
		valueSize |= data[ 1 ] << 24;
		valueSize |= data[ 2 ] << 16;
		valueSize |= data[ 3 ] << 8;
		valueSize = ntohl( valueSize );

		key = data + 4;

		value = data + 4 + keySize;

		return data;
	}
};

#endif
