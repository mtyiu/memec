#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/types.h>
#include <signal.h>
#include <pthread.h>
#include "protocol.hh"

#define PROTO_KEY_VALUE_SIZE	4 // 1 byte for key size, 3 bytes for value size

size_t Protocol::generateHeader( uint8_t magic, uint8_t opcode, uint32_t length, uint32_t id, char *buf ) {
	size_t bytes = 0;

	buf[ 0 ] = ( ( magic & 0x07 ) | ( PROTO_MAGIC_FROM_APPLICATION & 0x18 ) | ( PROTO_MAGIC_TO_MASTER & 0x60 ) );
	buf[ 1 ] = opcode & 0xFF;
	bytes = 2;

	*( ( uint32_t * )( buf + bytes ) ) = htonl( length );
	bytes += 4;

	*( ( uint32_t * )( buf + bytes ) ) = htonl( id );
	bytes += 4;

	return bytes;
}

bool Protocol::parseHeader( uint8_t &magic, uint8_t &from, uint8_t &to, uint8_t &opcode, uint32_t &length, uint32_t &id, char *buf, size_t size ) {
	if ( size < PROTO_HEADER_SIZE )
		return false;

	magic = buf[ 0 ] & 0x07;
	from = buf[ 0 ] & 0x18;
	to = buf[ 0 ] & 0x60;
	opcode = buf[ 1 ] & 0xFF;
	length = ntohl( *( ( uint32_t * )( buf + 2 ) ) );
	id = ntohl( *( ( uint32_t * )( buf + 6 ) ) );

	switch( magic ) {
		case PROTO_MAGIC_REQUEST:
		case PROTO_MAGIC_RESPONSE_SUCCESS:
		case PROTO_MAGIC_RESPONSE_FAILURE:
			break;
		default:
			fprintf( stderr, "Error #1: (magic, from, to, opcode, length, id) = (%x, %x, %x, %x, %u, %u)\n", magic, from, to, opcode, length, id );
			return false;
	}

	if ( from != PROTO_MAGIC_FROM_MASTER ) {
		fprintf( stderr, "Error #2: (magic, from, to, opcode, length, id) = (%x, %x, %x, %x, %u, %u)\n", magic, from, to, opcode, length, id );
		return false;
	}

	if ( to != PROTO_MAGIC_TO_APPLICATION ) {
		fprintf( stderr, "Error #3: (magic, from, to, opcode, length, id) = (%x, %x, %x, %x, %u, %u)\n", magic, from, to, opcode, length, id );
		return false;
	}

	switch( opcode ) {
		case PROTO_OPCODE_REGISTER:
		case PROTO_OPCODE_GET:
		case PROTO_OPCODE_SET:
		case PROTO_OPCODE_UPDATE:
		case PROTO_OPCODE_DELETE:
			break;
		default:
			fprintf( stderr, "Error #4: (magic, from, to, opcode, length, id) = (%x, %x, %x, %x, %u, %u)\n", magic, from, to, opcode, length, id );
			return false;
	}

	return true;
}

bool Protocol::parseHeader( struct ProtocolHeader &header, char *buf, size_t size ) {
	return this->parseHeader(
		header.magic,
		header.from,
		header.to,
		header.opcode,
		header.length,
		header.id,
		buf, size
	);
}

size_t Protocol::generateKeyHeader( uint8_t magic, uint8_t opcode, uint32_t id, uint8_t keySize, char *key, char *buf ) {
	size_t bytes = this->generateHeader( magic, opcode, PROTO_KEY_SIZE + keySize, id, buf );
	buf += PROTO_HEADER_SIZE;

	buf[ 0 ] = keySize;

	buf += PROTO_KEY_SIZE;
	memmove( buf, key, keySize );

	bytes += PROTO_KEY_SIZE + keySize;

	return bytes;
}

bool Protocol::parseKeyHeader( size_t offset, uint8_t &keySize, char *&key, char *buf, size_t size ) {
	if ( size - offset < PROTO_KEY_SIZE )
		return false;

	char *ptr = buf + offset;
	keySize = ( uint8_t ) ptr[ 0 ];

	if ( size - offset < ( size_t ) PROTO_KEY_SIZE + keySize )
		return false;

	key = ptr + PROTO_KEY_SIZE;

	return true;
}

bool Protocol::parseKeyHeader( struct KeyHeader &header, char *buf, size_t size, size_t offset ) {
	return this->parseKeyHeader(
		offset,
		header.keySize,
		header.key,
		buf, size
	);
}

size_t Protocol::generateKeyValueHeader( uint8_t magic, uint8_t opcode, uint32_t id, uint8_t keySize, char *key, uint32_t valueSize, char *value, char *buf ) {
	size_t bytes = this->generateHeader( magic, opcode, PROTO_KEY_VALUE_SIZE + keySize + valueSize, id, buf );
	buf += PROTO_HEADER_SIZE;

	buf[ 0 ] = keySize;

	valueSize = htonl( valueSize );
	unsigned char *tmp = ( unsigned char * ) &valueSize;
	buf[ 1 ] = tmp[ 1 ];
	buf[ 2 ] = tmp[ 2 ];
	buf[ 3 ] = tmp[ 3 ];
	valueSize = ntohl( valueSize );

	buf += PROTO_KEY_VALUE_SIZE;
	memmove( buf, key, keySize );
	buf += keySize;
	if ( valueSize )
		memmove( buf, value, valueSize );
	bytes += PROTO_KEY_VALUE_SIZE + keySize + valueSize;

	return bytes;
}

bool Protocol::parseKeyValueHeader( size_t offset, uint8_t &keySize, char *&key, uint32_t &valueSize, char *&value, char *buf, size_t size ) {
	if ( size - offset < PROTO_KEY_VALUE_SIZE )
		return false;

	char *ptr = buf + offset;
	unsigned char *tmp;
	keySize = ( uint8_t ) ptr[ 0 ];
	valueSize = 0;
	tmp = ( unsigned char * ) &valueSize;
	tmp[ 1 ] = ptr[ 1 ];
	tmp[ 2 ] = ptr[ 2 ];
	tmp[ 3 ] = ptr[ 3 ];
	valueSize = ntohl( valueSize );

	if ( size - offset < PROTO_KEY_VALUE_SIZE + keySize + valueSize )
		return false;

	key = ptr + PROTO_KEY_VALUE_SIZE;
	value = valueSize ? key + keySize : 0;

	return true;
}

bool Protocol::parseKeyValueHeader( struct KeyValueHeader &header, char *buf, size_t size, size_t offset ) {
	return this->parseKeyValueHeader(
		offset,
		header.keySize,
		header.key,
		header.valueSize,
		header.value,
		buf, size
	);
}

size_t Protocol::generateKeyValueUpdateHeader( uint8_t magic, uint8_t opcode, uint32_t id, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate, char *buf ) {
	size_t bytes = this->generateHeader(
		magic, opcode,
		PROTO_KEY_VALUE_UPDATE_SIZE + keySize + ( valueUpdate ? valueUpdateSize : 0 ),
		id, buf
	);
	buf += PROTO_HEADER_SIZE;

	buf[ 0 ] = keySize;

	unsigned char *tmp;
	valueUpdateSize = htonl( valueUpdateSize );
	valueUpdateOffset = htonl( valueUpdateOffset );
	tmp = ( unsigned char * ) &valueUpdateSize;
	buf[ 1 ] = tmp[ 1 ];
	buf[ 2 ] = tmp[ 2 ];
	buf[ 3 ] = tmp[ 3 ];
	tmp = ( unsigned char * ) &valueUpdateOffset;
	buf[ 4 ] = tmp[ 1 ];
	buf[ 5 ] = tmp[ 2 ];
	buf[ 6 ] = tmp[ 3 ];
	valueUpdateSize = ntohl( valueUpdateSize );
	valueUpdateOffset = ntohl( valueUpdateOffset );

	buf += PROTO_KEY_VALUE_UPDATE_SIZE;
	memmove( buf, key, keySize );
	buf += keySize;
	if ( valueUpdateSize && valueUpdate ) {
		memmove( buf, valueUpdate, valueUpdateSize );
		bytes += PROTO_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize;
	} else {
		bytes += PROTO_KEY_VALUE_UPDATE_SIZE + keySize;
	}

	return bytes;
}

bool Protocol::parseKeyValueUpdateHeader( size_t offset, uint8_t &keySize, char *&key, uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize, char *buf, size_t size ) {
	if ( size - offset < PROTO_KEY_VALUE_UPDATE_SIZE )
		return false;

	char *ptr = buf + offset;
	unsigned char *tmp;
	keySize = ( uint8_t ) ptr[ 0 ];

	valueUpdateSize = 0;
	tmp = ( unsigned char * ) &valueUpdateSize;
	tmp[ 1 ] = ptr[ 1 ];
	tmp[ 2 ] = ptr[ 2 ];
	tmp[ 3 ] = ptr[ 3 ];
	valueUpdateSize = ntohl( valueUpdateSize );

	valueUpdateOffset = 0;
	tmp = ( unsigned char * ) &valueUpdateOffset;
	tmp[ 1 ] = ptr[ 4 ];
	tmp[ 2 ] = ptr[ 5 ];
	tmp[ 3 ] = ptr[ 6 ];
	valueUpdateOffset = ntohl( valueUpdateOffset );

	key = ptr + PROTO_KEY_VALUE_UPDATE_SIZE;

	return true;
}

bool Protocol::parseKeyValueUpdateHeader( struct KeyValueUpdateHeader &header, char *buf, size_t size, size_t offset ) {
	header.valueUpdate = 0;
	return this->parseKeyValueUpdateHeader(
		offset,
		header.keySize,
		header.key,
		header.valueUpdateOffset,
		header.valueUpdateSize,
		buf, size
	);
}
