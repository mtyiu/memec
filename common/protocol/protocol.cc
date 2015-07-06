#include <cstdlib>
#include "protocol.hh"
#include "../util/debug.hh"

size_t Protocol::generateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t length ) {
	size_t bytes = 0;

	this->buffer.data[ 0 ] = ( ( magic & 0x0F ) | ( this->from & 0x30 ) | ( to & 0xC0 ) );
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

bool Protocol::init( size_t size, char *data ) {
	this->buffer.size = size;
	this->buffer.data = data;
	return true;
}

bool Protocol::parseHeader( char *buf, size_t size, uint8_t &magic, uint8_t &from, uint8_t &opcode, uint32_t &length ) {
	if ( size < 8 )
		return false;

	uint8_t to;
	
	magic = buf[ 0 ] & 0x0F;
	from = buf[ 0 ] & 0x30;
	to = buf[ 0 ] & 0xC0;
	opcode = buf[ 1 ] & 0xFF;
	length = ntohl( *( ( uint32_t * )( buf + 4 ) ) );

	switch( magic ) {
		case PROTO_MAGIC_REQUEST:
		case PROTO_MAGIC_RESPONSE_SUCCESS:
		case PROTO_MAGIC_RESPONSE_FAILURE:
		case PROTO_MAGIC_HEARTBEAT:
			break;
		default:
			return false;
	}

	switch( from ) {
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
