#include <cstring>
#include "key_value.hh"

Key KeyValue::key() {
	Key key;
	key.size = ( uint8_t ) this->data[ 0 ];
	key.data = this->data + KEY_VALUE_METADATA_SIZE;
	return key;
}

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

	memcpy( data + KEY_VALUE_METADATA_SIZE, key, keySize );
	memcpy( data + KEY_VALUE_METADATA_SIZE + keySize, value, valueSize );

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

	key = data + KEY_VALUE_METADATA_SIZE;

	value = data + KEY_VALUE_METADATA_SIZE + keySize;

	return data;
}

bool compare( const KeyValue *v1, const KeyValue *v2 ) {
	uint8_t keySize1 = ( uint8_t ) v1->data[ 0 ];
	uint8_t keySize2 = ( uint8_t ) v2->data[ 0 ];

	if ( keySize1 == keySize2 ) {
		return strncmp(
			v1->data + KEY_VALUE_METADATA_SIZE,
			v2->data + KEY_VALUE_METADATA_SIZE,
			keySize1
		) == 0;
	}
	return false;
}
