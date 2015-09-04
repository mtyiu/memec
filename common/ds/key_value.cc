#include <cstdlib>
#include <cstring>
#include "key_value.hh"

Key KeyValue::key() {
	Key key;
	key.set( ( uint8_t ) this->data[ 0 ], this->data + KEY_VALUE_METADATA_SIZE, 0 );
	return key;
}

void KeyValue::dup( char *key, uint8_t keySize, char *value, uint32_t valueSize, void *ptr ) {
	this->data = ( char * ) malloc( sizeof( char ) * ( keySize + valueSize ) + KEY_VALUE_METADATA_SIZE );
	this->ptr = ptr;
	this->serialize( key, keySize, value, valueSize );
}

void KeyValue::set( char *data, void *ptr ) {
	this->data = data;
	this->ptr = ptr;
}

void KeyValue::clear() {
	this->data = 0;
	this->ptr = 0;
}

void KeyValue::free() {
	if ( this->data )
		::free( this->data );
	this->data = 0;
}

void KeyValue::setSize( uint8_t keySize, uint32_t valueSize ) {
	data[ 0 ] = ( char ) keySize;

	valueSize = htonl( valueSize );
	unsigned char *tmp = ( unsigned char * ) &valueSize;
	data[ 1 ] = tmp[ 1 ];
	data[ 2 ] = tmp[ 2 ];
	data[ 3 ] = tmp[ 3 ];
	valueSize = ntohl( valueSize );
}

char *KeyValue::serialize( char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
	return KeyValue::serialize( this->data, key, keySize, value, valueSize );
}

char *KeyValue::serialize( char *data, char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
	data[ 0 ] = ( char ) keySize;

	valueSize = htonl( valueSize );
	unsigned char *tmp = ( unsigned char * ) &valueSize;
	data[ 1 ] = tmp[ 1 ];
	data[ 2 ] = tmp[ 2 ];
	data[ 3 ] = tmp[ 3 ];
	valueSize = ntohl( valueSize );

	memcpy( data + KEY_VALUE_METADATA_SIZE, key, keySize );
	memcpy( data + KEY_VALUE_METADATA_SIZE + keySize, value, valueSize );

	return data;
}

char *KeyValue::deserialize( char *&key, uint8_t &keySize, char *&value, uint32_t &valueSize ) const {
	return KeyValue::deserialize( this->data, key, keySize, value, valueSize );
}

char *KeyValue::deserialize( char *data, char *&key, uint8_t &keySize, char *&value, uint32_t &valueSize ) {
	keySize = ( uint8_t ) data[ 0 ];

	valueSize = 0;
	unsigned char *tmp = ( unsigned char * ) &valueSize;
	tmp[ 1 ] = data[ 1 ];
	tmp[ 2 ] = data[ 2 ];
	tmp[ 3 ] = data[ 3 ];
	valueSize = ntohl( valueSize );

	key = data + KEY_VALUE_METADATA_SIZE;

	value = data + KEY_VALUE_METADATA_SIZE + keySize;

	return data;
}

bool KeyValue::operator<( const KeyValue &kv ) const {
	char *key[ 2 ], *value[ 2 ];
	uint8_t keySize[ 2 ];
	uint32_t valueSize[ 2 ];
	int ret;

	this->deserialize( key[ 0 ], keySize[ 0 ], value[ 0 ], valueSize[ 0 ] );
	this->deserialize( key[ 1 ], keySize[ 1 ], value[ 1 ], valueSize[ 1 ] );

	if ( keySize[ 0 ] < keySize[ 1 ] )
		return true;
	if ( keySize[ 0 ] > keySize[ 1 ] )
		return false;

	ret = strncmp( key[ 0 ], key[ 1 ], keySize[ 0 ] );
	if ( ret < 0 )
		return true;
	if ( ret > 0 )
		return false;

	if ( this->ptr < kv.ptr )
		return true;
	if ( this->ptr > kv.ptr )
		return false;

	if ( valueSize[ 0 ] < valueSize[ 1 ] )
		return true;
	if ( valueSize[ 0 ] > valueSize[ 1 ] )
		return false;

	return strncmp( value[ 0 ], value[ 1 ], valueSize[ 0 ] ) < 0;
}

bool KeyValue::compare( const KeyValue *v1, const KeyValue *v2 ) {
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
