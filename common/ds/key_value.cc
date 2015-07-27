#include <cstring>
#include "key_value.hh"

char *KeyValue::serialize( char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
	return KeyValue::serialize( this->data, key, keySize, value, valueSize );
}

char *KeyValue::serialize( char *data, char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
	data[ 0 ] = ( char ) keySize;
	
	valueSize = htonl( valueSize );
	data[ 1 ] = ( valueSize >> 24 ) & 0xFF;
	data[ 2 ] = ( valueSize >> 16 ) & 0xFF;
	data[ 3 ] = ( valueSize >> 8 ) & 0xFF;
	valueSize = ntohl( valueSize );

	memcpy( data + 4, key, keySize );
	memcpy( data + 4 + keySize, value, valueSize );

	return data;
}

char *KeyValue::deserialize( char *&key, uint8_t &keySize, char *&value, uint32_t &valueSize ) {
	return KeyValue::deserialize( this->data, key, keySize, value, valueSize );
}

char *KeyValue::deserialize( char *data, char *&key, uint8_t &keySize, char *&value, uint32_t &valueSize ) {
	keySize = ( uint8_t ) data[ 0 ];

	valueSize = 0;
	valueSize |= data[ 1 ] << 24;
	valueSize |= data[ 2 ] << 16;
	valueSize |= data[ 3 ] << 8;
	valueSize = ntohl( valueSize );

	key = data + 4;

	value = data + 4 + keySize;

	return data;
}

bool compare( const KeyValue *v1, const KeyValue *v2 ) {
	uint8_t keySize1 = ( uint8_t ) v1[ 0 ];
	uint8_t keySize2 = ( uint8_t ) v2[ 0 ];

	if ( keySize1 == keySize2 ) {
		return strncmp( v1 + 4, v2 + 4, keySize1 ) == 0;
	}
	return false;
}
