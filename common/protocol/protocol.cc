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

Protocol::Protocol( Role role ) {
	this->buffer.size = 0;
	this->buffer.data = 0;
	switch( role ) {
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

bool Protocol::parseHeader( char *buf, size_t size, uint8_t &magic, uint8_t &from, uint8_t &opcode, uint32_t &length ) {
	if ( size < 8 )
		return false;

	uint8_t to;
	
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
			break;
		default:
			return false;
	}

	return true;
}

bool Protocol::parseHeader( char *buf, size_t size, struct ProtocolHeader &header ) {
	return this->parseHeader(
		buf, size,
		header.magic,
		header.from,
		header.opcode,
		header.length
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
