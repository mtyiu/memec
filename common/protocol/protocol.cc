#include <cstdlib>
#include "protocol.hh"
#include "../util/debug.hh"

#define PROTO_BUF_MIN_SIZE		65536
#define PROTO_KEY_VALUE_SIZE	4 // 1 byte for key size, 3 bytes for value size

size_t Protocol::generateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t length ) {
	size_t bytes = 0;

	this->buffer.data[ 0 ] = ( ( magic & 0x07 ) | ( this->from & 0x18 ) | ( to & 0x60 ) );
	this->buffer.data[ 1 ] = opcode & 0xFF;
	this->buffer.data[ 2 ] = 0;
	this->buffer.data[ 3 ] = 0;
	bytes += 4;

	*( ( uint32_t * )( this->buffer.data + bytes ) ) = htonl( length );
	bytes += 4;

	return bytes;
}

size_t Protocol::generateKeyHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint8_t keySize, char *key ) {
	char *buf = this->buffer.data + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, keySize );

	buf[ 0 ] = keySize;

	buf += 1;
	memcpy( buf, key, keySize );

	bytes += 1 + keySize;

	return bytes;
}

size_t Protocol::generateKeyValueHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint8_t keySize, char *key, uint32_t valueSize, char *value ) {
	char *buf = this->buffer.data + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, keySize + valueSize );

	buf[ 0 ] = keySize;

	valueSize = htonl( valueSize );
	printf( "%u\n", valueSize );
	buf[ 1 ] = ( valueSize >> 24 ) & 0xFF;
	buf[ 2 ] = ( valueSize >> 16 ) & 0xFF;
	buf[ 3 ] = ( valueSize >> 8 ) & 0xFF;
	valueSize = ntohl( valueSize );

	printf( "%u: %u %u %u\n", valueSize, buf[ 1 ], buf[ 2 ], buf[ 3 ] );

	buf += 4;
	memcpy( buf, key, keySize );
	buf += keySize;
	memcpy( buf, value, valueSize );

	bytes += 4 + keySize + valueSize;

	return bytes;
}

bool Protocol::parseHeader( uint8_t &magic, uint8_t &from, uint8_t &to, uint8_t &opcode, uint32_t &length, char *buf, size_t size ) {
	if ( size < 8 )
		return false;

	magic = buf[ 0 ] & 0x07;
	from = buf[ 0 ] & 0x18;
	to = buf[ 0 ] & 0x60;
	opcode = buf[ 1 ] & 0xFF;
	length = ntohl( *( ( uint32_t * )( buf + 4 ) ) );

	switch( magic ) {
		case PROTO_MAGIC_HEARTBEAT:
		case PROTO_MAGIC_REQUEST:
		case PROTO_MAGIC_RESPONSE_SUCCESS:
		case PROTO_MAGIC_RESPONSE_FAILURE:
			break;
		default:
			return false;
	}

	switch( from ) {
		case PROTO_MAGIC_FROM_APPLICATION:
		case PROTO_MAGIC_FROM_COORDINATOR:
		case PROTO_MAGIC_FROM_MASTER:
		case PROTO_MAGIC_FROM_SLAVE:
			break;
		default:
			return false;
	}

	if ( to != this->to )
		return false;

	switch( opcode ) {
		case PROTO_OPCODE_REGISTER:
		case PROTO_OPCODE_GET_CONFIG:
		case PROTO_OPCODE_GET:
		case PROTO_OPCODE_SET:
		case PROTO_OPCODE_REPLACE:
		case PROTO_OPCODE_UPDATE:
		case PROTO_OPCODE_DELETE:
		case PROTO_OPCODE_FLUSH:
			break;
		default:
			return false;
	}

	return true;
}

bool Protocol::parseKeyHeader( size_t offset, uint8_t &keySize, char *&key, char *buf, size_t size ) {
	if ( size < PROTO_KEY_SIZE )
		return false;

	char *ptr = buf + offset;
	keySize = ( uint8_t ) ptr[ 0 ];

	if ( size < ( size_t ) PROTO_KEY_SIZE + keySize )
		return false;

	key = ptr + PROTO_KEY_SIZE;

	return true;
}

bool Protocol::parseKeyValueHeader( size_t offset, uint8_t &keySize, char *&key, uint32_t &valueSize, char *&value, char *buf, size_t size ) {
	if ( size < PROTO_KEY_VALUE_SIZE )
		return false;

	char *ptr = buf + offset;
	keySize = ( uint8_t ) ptr[ 0 ];
	valueSize = 0;
	valueSize |= ptr[ 1 ] << 24;
	valueSize |= ptr[ 2 ] << 16;
	valueSize |= ptr[ 3 ] << 8;
	valueSize = ntohl( valueSize );

	if ( size < PROTO_KEY_VALUE_SIZE + keySize + valueSize )
		return false;

	key = ptr + PROTO_KEY_VALUE_SIZE;
	value = key + keySize;

	return true;
}

Protocol::Protocol( Role role ) {
	this->buffer.size = 0;
	this->buffer.data = 0;
	switch( role ) {
		case ROLE_APPLICATION:
			this->from = PROTO_MAGIC_FROM_APPLICATION;
			this->to = PROTO_MAGIC_TO_APPLICATION;
			break;
		case ROLE_COORDINATOR:
			this->from = PROTO_MAGIC_FROM_COORDINATOR;
			this->to = PROTO_MAGIC_TO_COORDINATOR;
			break;
		case ROLE_MASTER:
			this->from = PROTO_MAGIC_FROM_MASTER;
			this->to = PROTO_MAGIC_TO_MASTER;
			break;
		case ROLE_SLAVE:
			this->from = PROTO_MAGIC_FROM_SLAVE;
			this->to = PROTO_MAGIC_TO_SLAVE;
			break;
		default:
			__ERROR__( "Protocol", "Protocol", "Unknown server role." );
	}
}

bool Protocol::init( size_t size ) {
	this->buffer.size = size;
	this->buffer.data = ( char * ) ::malloc( size );
	if ( ! this->buffer.data ) {
		__ERROR__( "Protocol", "init", "Cannot allocate memory." );
		return false;
	}
	return true;
}

void Protocol::free() {
	if ( ! this->buffer.data )
		return;
	::free( this->buffer.data );
	this->buffer.size = 0;
	this->buffer.data = 0;
}

bool Protocol::parseHeader( struct ProtocolHeader &header, char *buf, size_t size ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.data;
		size = this->buffer.size;
	}
	return this->parseHeader(
		header.magic,
		header.from,
		header.to,
		header.opcode,
		header.length,
		buf, size
	);
}

bool Protocol::parseKeyHeader( struct KeyHeader &header, size_t offset, char *buf, size_t size ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.data;
		size = this->buffer.size;
	}
	return this->parseKeyHeader(
		offset,
		header.keySize,
		header.key,
		buf, size
	);
}

bool Protocol::parseKeyValueHeader( struct KeyValueHeader &header, size_t offset, char *buf, size_t size ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.data;
		size = this->buffer.size;
	}
	return this->parseKeyValueHeader(
		offset,
		header.keySize,
		header.key,
		header.valueSize,
		header.value,
		buf, size
	);
}

size_t Protocol::getSuggestedBufferSize( uint32_t keySize, uint32_t chunkSize ) {
	size_t ret = (
		PROTO_HEADER_SIZE +
		PROTO_KEY_VALUE_SIZE +
		keySize +
		chunkSize
	);
	// Set ret = ceil( ret / 4096 ) * 4096
	if ( ret & 4095 ) { // 0xFFF
		ret >>= 12;
		ret += 1;
		ret <<= 12;
	}
	// Set ret = ret * 2
	ret <<= 1;
	if ( ret < PROTO_BUF_MIN_SIZE )
		ret = PROTO_BUF_MIN_SIZE;
	return ret;
}
