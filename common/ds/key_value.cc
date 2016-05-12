#include <cstdlib>
#include <cstring>
#include "key_value.hh"

uint32_t LargeObjectUtil::chunkSize;

Key KeyValue::key( bool enableSplit ) {
	Key key;
	if ( enableSplit ) {
		char *valueStr;
		uint32_t valueSize, splitOffset;
		this->deserialize( key.data, key.size, valueStr, valueSize, splitOffset );
	} else {
		key.set( ( uint8_t ) this->data[ 0 ], this->data + KEY_VALUE_METADATA_SIZE, 0 );
	}
	return key;
}

void KeyValue::dup( char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t splitOffset, void *ptr ) {
	uint32_t splitSize;
	bool isLarge = LargeObjectUtil::isLarge( keySize, valueSize, 0, &splitSize );
	if ( isLarge ) {
		if ( splitOffset + splitSize > valueSize )
			splitSize = valueSize - splitOffset;
	} else {
		splitSize = valueSize;
	}

	this->data = ( char * ) malloc( sizeof( char ) * ( keySize + splitSize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ) + KEY_VALUE_METADATA_SIZE ) );
	this->ptr = ptr;
	this->serialize( key, keySize, value, valueSize, splitOffset );
}

void KeyValue::_dup( char *key, uint8_t keySize, char *value, uint32_t valueSize, void *ptr ) {
	this->data = ( char * ) malloc( sizeof( char ) * ( keySize + valueSize + KEY_VALUE_METADATA_SIZE ) );
	this->ptr = ptr;
	this->_serialize( key, keySize, value, valueSize );
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
	return KeyValue::setSize( this->data, keySize, valueSize );
}

void KeyValue::setSize( char *data, uint8_t keySize, uint32_t valueSize ) {
	data[ 0 ] = ( char ) keySize;

	valueSize = htonl( valueSize );
	unsigned char *tmp = ( unsigned char * ) &valueSize;
	data[ 1 ] = tmp[ 1 ];
	data[ 2 ] = tmp[ 2 ];
	data[ 3 ] = tmp[ 3 ];
	valueSize = ntohl( valueSize );
}

uint32_t KeyValue::getSize( uint8_t *keySizePtr, uint32_t *valueSizePtr ) const {
	return KeyValue::getSize( this->data, keySizePtr, valueSizePtr );
}

uint32_t KeyValue::getSize( char *data, uint8_t *keySizePtr, uint32_t *valueSizePtr ) {
	char *keyStr, *valueStr;
	uint8_t keySize;
	uint32_t valueSize, splitOffset, splitSize, ret;
	KeyValue::deserialize( data, keyStr, keySize, valueStr, valueSize, splitOffset );

	bool isLarge = LargeObjectUtil::isLarge( keySize, valueSize, 0, &splitSize );
	if ( isLarge ) {
		if ( splitOffset + splitSize > valueSize )
			splitSize = valueSize - splitOffset;
		ret = KEY_VALUE_METADATA_SIZE + keySize + SPLIT_OFFSET_SIZE + splitSize;
	} else {
		ret = KEY_VALUE_METADATA_SIZE + keySize + valueSize;
	}

	if ( keySizePtr ) *keySizePtr = keySize;
	if ( valueSizePtr ) *valueSizePtr = valueSize;

	return ret;
}

char *KeyValue::serialize( char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t splitOffset ) {
	return KeyValue::serialize( this->data, key, keySize, value, valueSize, splitOffset );
}

char *KeyValue::serialize( char *data, char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t splitOffset ) {
	char *ret = data;
	data[ 0 ] = ( char ) keySize;

	valueSize = htonl( valueSize );
	unsigned char *tmp = ( unsigned char * ) &valueSize;
	data[ 1 ] = tmp[ 1 ];
	data[ 2 ] = tmp[ 2 ];
	data[ 3 ] = tmp[ 3 ];
	valueSize = ntohl( valueSize );

	data += KEY_VALUE_METADATA_SIZE;

	if ( key )
		memcpy( data, key, keySize );
	data += keySize;

	uint32_t splitSize;
	bool isLarge = LargeObjectUtil::isLarge( keySize, valueSize, 0, &splitSize );
	if ( isLarge ) {
		splitOffset = htonl( splitOffset );
		tmp = ( unsigned char * ) &splitOffset;
		data[ 0 ] = tmp[ 1 ];
		data[ 1 ] = tmp[ 2 ];
		data[ 2 ] = tmp[ 3 ];
		splitOffset = ntohl( splitOffset );

		data += SPLIT_OFFSET_SIZE;

		if ( splitOffset + splitSize > valueSize )
			splitSize = valueSize - splitOffset;
	} else {
		splitSize = valueSize;
	}

	if ( value )
		memcpy( data, value, splitSize );
	fprintf(
		stderr, "~~ %u %c (%d) | %c (%d) | %c (%d) | %c (%d) | %c (%d) | %c (%d)\n",
		splitSize,
		data[ splitSize - 6 ], data[ splitSize - 6 ],
		data[ splitSize - 5 ], data[ splitSize - 5 ],
		data[ splitSize - 4 ], data[ splitSize - 4 ],
		data[ splitSize - 3 ], data[ splitSize - 3 ],
		data[ splitSize - 2 ], data[ splitSize - 2 ],
		data[ splitSize - 1 ], data[ splitSize - 1 ]
	);
	return ret;
}

char *KeyValue::deserialize( char *&key, uint8_t &keySize, char *&value, uint32_t &valueSize, uint32_t &splitOffset ) const {
	return KeyValue::deserialize( this->data, key, keySize, value, valueSize, splitOffset );
}

char *KeyValue::deserialize( char *data, char *&key, uint8_t &keySize, char *&value, uint32_t &valueSize, uint32_t &splitOffset ) {
	char *ret = data;
	keySize = ( uint8_t ) data[ 0 ];

	valueSize = 0;
	unsigned char *tmp = ( unsigned char * ) &valueSize;
	tmp[ 1 ] = data[ 1 ];
	tmp[ 2 ] = data[ 2 ];
	tmp[ 3 ] = data[ 3 ];
	valueSize = ntohl( valueSize );

	data += KEY_VALUE_METADATA_SIZE;
	key = data;
	data += keySize;

	uint32_t splitSize;
	bool isLarge = LargeObjectUtil::isLarge( keySize, valueSize, 0, &splitSize );
	if ( isLarge ) {
		splitOffset = 0;
		tmp = ( unsigned char * ) &splitOffset;
		tmp[ 1 ] = data[ 0 ];
		tmp[ 2 ] = data[ 1 ];
		tmp[ 3 ] = data[ 2 ];
		splitOffset = ntohl( splitOffset );

		data += SPLIT_OFFSET_SIZE;
	} else {
		splitOffset = 0;
	}

	value = data;

	return ret;
}

char *KeyValue::_serialize( char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
	return KeyValue::_serialize( this->data, key, keySize, value, valueSize );
}

char *KeyValue::_serialize( char *data, char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
	data[ 0 ] = ( char ) keySize;

	valueSize = htonl( valueSize );
	unsigned char *tmp = ( unsigned char * ) &valueSize;
	data[ 1 ] = tmp[ 1 ];
	data[ 2 ] = tmp[ 2 ];
	data[ 3 ] = tmp[ 3 ];
	valueSize = ntohl( valueSize );

	if ( key )
		memcpy( data + KEY_VALUE_METADATA_SIZE, key, keySize );
	if ( value )
		memcpy( data + KEY_VALUE_METADATA_SIZE + keySize, value, valueSize );

	return data;
}

char *KeyValue::_deserialize( char *&key, uint8_t &keySize, char *&value, uint32_t &valueSize ) const {
	return KeyValue::_deserialize( this->data, key, keySize, value, valueSize );
}

char *KeyValue::_deserialize( char *data, char *&key, uint8_t &keySize, char *&value, uint32_t &valueSize ) {
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

void KeyValue::print( FILE *f ) {
	uint8_t keySize;
	uint32_t valueSize, splitOffset;
	char *key, *value;

	this->deserialize( key, keySize, value, valueSize, splitOffset );

	fprintf(
		f, "Key: %.*s; Value: %.*s (split offset = %u)\n",
		keySize, key, valueSize, value, splitOffset
	);
}

////////////////////////////////////////////////////////////////////////////////

void LargeObjectUtil::init( uint32_t chunkSize ) {
	LargeObjectUtil::chunkSize = chunkSize;
}

bool LargeObjectUtil::isLarge( uint8_t keySize, uint32_t valueSize, uint32_t *numOfSplitPtr, uint32_t *splitSizePtr ) {
	uint32_t totalSize = KEY_VALUE_METADATA_SIZE + keySize + valueSize;
	uint32_t numOfSplit, splitSize;

	if ( numOfSplitPtr ) *numOfSplitPtr = 1;
	if ( splitSizePtr ) *splitSizePtr = valueSize;

	if ( LargeObjectUtil::chunkSize < totalSize ) {
		splitSize = LargeObjectUtil::chunkSize - KEY_VALUE_METADATA_SIZE - keySize;
		numOfSplit = valueSize / splitSize;
		if ( valueSize % splitSize ) numOfSplit++;

		if ( numOfSplitPtr ) *numOfSplitPtr = numOfSplit;
		if ( splitSizePtr ) *splitSizePtr = splitSize;

		return true;
	} else {
		// No need to split
		return false;
	}
}

uint32_t LargeObjectUtil::getValueOffsetAtSplit( uint8_t keySize, uint32_t valueSize, uint32_t index ) {
	uint32_t totalSize = KEY_VALUE_METADATA_SIZE + keySize + valueSize;
	uint32_t splitSize;

	if ( LargeObjectUtil::chunkSize < totalSize ) {
		splitSize = LargeObjectUtil::chunkSize - KEY_VALUE_METADATA_SIZE - keySize;
		return ( index * splitSize );
	} else {
		// No need to split
		return 0;
	}
}

uint32_t LargeObjectUtil::getSplitIndex( uint8_t keySize, uint32_t valueSize, uint32_t offset, bool &isLarge ) {
	uint32_t totalSize = KEY_VALUE_METADATA_SIZE + keySize + valueSize;
	uint32_t splitSize;

	if ( LargeObjectUtil::chunkSize < totalSize ) {
		isLarge = true;
		splitSize = LargeObjectUtil::chunkSize - KEY_VALUE_METADATA_SIZE - keySize;
		return ( offset / splitSize );
	} else {
		// No need to split
		isLarge = false;
		return 0;
	}
}

void LargeObjectUtil::writeSplitOffset( char *buf, uint32_t splitOffset ) {
	splitOffset = htonl( splitOffset );
	unsigned char *tmp = ( unsigned char * ) &splitOffset;
	buf[ 0 ] = tmp[ 1 ];
	buf[ 1 ] = tmp[ 2 ];
	buf[ 2 ] = tmp[ 3 ];
	splitOffset = ntohl( splitOffset );
}

uint32_t LargeObjectUtil::readSplitOffset( char *buf ) {
	uint32_t splitOffset = 0;
	unsigned char *tmp = ( unsigned char * ) &splitOffset;
	tmp[ 1 ] = buf[ 0 ];
	tmp[ 2 ] = buf[ 1 ];
	tmp[ 3 ] = buf[ 2 ];
	splitOffset = ntohl( splitOffset );
	return splitOffset;
}
