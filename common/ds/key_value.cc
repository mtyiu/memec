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

uint32_t KeyValue::getChunkUpdateOffset( uint32_t chunkOffset, uint8_t keySize, uint32_t valueUpdateOffset ) {
	return ( chunkOffset + KEY_VALUE_METADATA_SIZE + keySize + valueUpdateOffset );
}
