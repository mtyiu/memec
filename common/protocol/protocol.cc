#include <cstdlib>
#include "protocol.hh"
#include "../util/debug.hh"

#define PROTO_BUF_MIN_SIZE		65536
#define PROTO_KEY_VALUE_SIZE	4 // 1 byte for key size, 3 bytes for value size

size_t Protocol::generateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t length ) {
	size_t bytes = 0;

	this->buffer.send[ 0 ] = ( ( magic & 0x07 ) | ( this->from & 0x18 ) | ( to & 0x60 ) );
	this->buffer.send[ 1 ] = opcode & 0xFF;
	this->buffer.send[ 2 ] = 0;
	this->buffer.send[ 3 ] = 0;
	bytes += 4;

	*( ( uint32_t * )( this->buffer.send + bytes ) ) = htonl( length );
	bytes += 4;

	return bytes;
}

size_t Protocol::generateKeyHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint8_t keySize, char *key ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_KEY_SIZE + keySize );

	buf[ 0 ] = keySize;

	buf += 1;
	memmove( buf, key, keySize );

	bytes += 1 + keySize;

	return bytes;
}

size_t Protocol::generateKeyValueHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint8_t keySize, char *key, uint32_t valueSize, char *value ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_KEY_VALUE_SIZE + keySize + valueSize );

	buf[ 0 ] = keySize;

	valueSize = htonl( valueSize );
	buf[ 1 ] = ( valueSize >> 24 ) & 0xFF;
	buf[ 2 ] = ( valueSize >> 16 ) & 0xFF;
	buf[ 3 ] = ( valueSize >> 8 ) & 0xFF;
	valueSize = ntohl( valueSize );

	buf += PROTO_KEY_VALUE_SIZE;
	memmove( buf, key, keySize );
	buf += keySize;
	if ( valueSize )
		memmove( buf, value, valueSize );

	bytes += PROTO_KEY_VALUE_SIZE + keySize + valueSize;

	return bytes;
}

size_t Protocol::generateKeyValueUpdateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize );

	buf[ 0 ] = keySize;

	valueUpdateSize = htonl( valueUpdateSize );
	buf[ 1 ] = ( valueUpdateSize >> 24 ) & 0xFF;
	buf[ 2 ] = ( valueUpdateSize >> 16 ) & 0xFF;
	buf[ 3 ] = ( valueUpdateSize >> 8 ) & 0xFF;
	valueUpdateSize = ntohl( valueUpdateSize );

	valueUpdateOffset = htonl( valueUpdateOffset );
	buf[ 4 ] = ( valueUpdateOffset >> 24 ) & 0xFF;
	buf[ 5 ] = ( valueUpdateOffset >> 16 ) & 0xFF;
	buf[ 6 ] = ( valueUpdateOffset >> 8 ) & 0xFF;
	valueUpdateOffset = ntohl( valueUpdateOffset );

	buf += PROTO_KEY_VALUE_UPDATE_SIZE;
	memmove( buf, key, keySize );
	buf += keySize;
	if ( valueUpdateSize )
		memmove( buf, valueUpdate, valueUpdateSize );

	bytes += PROTO_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize;

	return bytes;
}

size_t Protocol::generateHeartbeatMessage( uint8_t magic, uint8_t to, uint8_t opcode, struct HeartbeatHeader &header, std::map<Key, Metadata> &ops, size_t &count ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	std::map<Key, Metadata>::iterator it;
	size_t bytes = PROTO_HEADER_SIZE;
	count = 0;

	*( ( uint32_t * )( buf      ) ) = htonl( header.get );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( header.set );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( header.update );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( header.del );
	buf += PROTO_HEARTBEAT_SIZE;
	bytes += PROTO_HEARTBEAT_SIZE;

	for ( it = ops.begin(); it != ops.end(); it++ ) {
		const Key &key = it->first;
		const Metadata &metadata = it->second;
		if ( this->buffer.size >= bytes + PROTO_SLAVE_SYNC_PER_SIZE + key.size ) {
			// Send buffer still has enough space for holding the current metadata
			buf[ 0 ] = key.size;
			buf[ 1 ] = metadata.opcode;
			*( ( uint32_t * )( buf + 2 ) ) = htonl( metadata.listId );
			*( ( uint32_t * )( buf + 6 ) ) = htonl( metadata.stripeId );
			*( ( uint32_t * )( buf + 10 ) ) = htonl( metadata.chunkId );
			
			buf += PROTO_SLAVE_SYNC_PER_SIZE;
			memmove( buf, key.data, key.size );
			buf += key.size;
			bytes += PROTO_SLAVE_SYNC_PER_SIZE + key.size;
			count++;
		} else {
			break;
		}
	}
	// Clear sent metadata
	ops.erase( ops.begin(), it );

	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE );

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
		case PROTO_OPCODE_SYNC:
		case PROTO_OPCODE_GET:
		case PROTO_OPCODE_SET:
		case PROTO_OPCODE_REPLACE:
		case PROTO_OPCODE_UPDATE:
		case PROTO_OPCODE_UPDATE_DELTA:
		case PROTO_OPCODE_DELETE:
		case PROTO_OPCODE_DELETE_DELTA:
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
	value = valueSize ? key + keySize : 0;

	return true;
}

bool Protocol::parseKeyValueUpdateHeader( size_t offset, uint8_t &keySize, char *&key, uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize, char *&valueUpdate, char *buf, size_t size ) {
	if ( size < PROTO_KEY_VALUE_UPDATE_SIZE )
		return false;

	char *ptr = buf + offset;
	keySize = ( uint8_t ) ptr[ 0 ];
	valueUpdateSize = 0;
	valueUpdateSize |= ptr[ 1 ] << 24;
	valueUpdateSize |= ptr[ 2 ] << 16;
	valueUpdateSize |= ptr[ 3 ] << 8;
	valueUpdateSize = ntohl( valueUpdateSize );
	valueUpdateOffset = 0;
	valueUpdateOffset |= ptr[ 4 ] << 24;
	valueUpdateOffset |= ptr[ 5 ] << 16;
	valueUpdateOffset |= ptr[ 6 ] << 8;
	valueUpdateOffset = ntohl( valueUpdateOffset );

	if ( size < PROTO_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize )
		return false;

	key = ptr + PROTO_KEY_VALUE_UPDATE_SIZE;
	valueUpdate = valueUpdateSize ? key + keySize : 0;

	return true;
}

bool Protocol::parseHeartbeatHeader( size_t offset, uint32_t &get, uint32_t &set, uint32_t &update, uint32_t &del, char *buf, size_t size ) {
	if ( size < PROTO_HEARTBEAT_SIZE )
		return false;

	char *ptr = buf + offset;
	get = ntohl( *( ( uint32_t * )( ptr ) ) );
	set = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	update = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );
	del = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );

	return true;
}

bool Protocol::parseSlaveSyncHeader( size_t offset, uint8_t &keySize, uint8_t &opcode, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, char *&key, char *buf, size_t size ) {
	if ( size < PROTO_SLAVE_SYNC_PER_SIZE )
		return false;

	char *ptr = buf + offset;
	keySize = ( uint8_t ) ptr[ 0 ];
	opcode = ( uint8_t ) ptr[ 1 ];
	listId = ntohl( *( ( uint32_t * )( ptr + 2 ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr + 6 ) ) );
	chunkId = ntohl( *( ( uint32_t * )( ptr + 10 ) ) );

	if ( size < PROTO_SLAVE_SYNC_PER_SIZE + ( size_t ) keySize )
		return false;

	key = ptr + PROTO_SLAVE_SYNC_PER_SIZE;

	return true;
}

Protocol::Protocol( Role role ) {
	this->buffer.size = 0;
	this->buffer.send = 0;
	this->buffer.recv = 0;
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
	this->buffer.send = ( char * ) ::malloc( size );
	if ( ! this->buffer.send ) {
		__ERROR__( "Protocol", "init", "Cannot allocate memory." );
		return false;
	}

	this->buffer.recv = ( char * ) ::malloc( size );
	if ( ! this->buffer.recv ) {
		__ERROR__( "Protocol", "init", "Cannot allocate memory." );
		return false;
	}
	return true;
}

void Protocol::free() {
	if ( this->buffer.send )
		::free( this->buffer.send );
	if ( this->buffer.recv )
		::free( this->buffer.recv );
	this->buffer.size = 0;
	this->buffer.send = 0;
	this->buffer.recv = 0;
}

bool Protocol::parseHeader( struct ProtocolHeader &header, char *buf, size_t size ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
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
		buf = this->buffer.recv;
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
		buf = this->buffer.recv;
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

bool Protocol::parseKeyValueUpdateHeader( struct KeyValueUpdateHeader &header, size_t offset, char *buf, size_t size ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseKeyValueUpdateHeader(
		offset,
		header.keySize,
		header.key,
		header.valueUpdateSize,
		header.valueUpdateOffset,
		header.valueUpdate,
		buf, size
	);
}

bool Protocol::parseHeartbeatHeader( struct HeartbeatHeader &header, size_t offset, char *buf, size_t size ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseHeartbeatHeader(
		offset,
		header.get,
		header.set,
		header.update,
		header.del,
		buf, size
	);
}

bool Protocol::parseSlaveSyncHeader( struct SlaveSyncHeader &header, size_t &bytes, size_t offset, char *buf, size_t size ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	bool ret = this->parseSlaveSyncHeader(
		offset,
		header.keySize,
		header.opcode,
		header.listId,
		header.stripeId,
		header.chunkId,
		header.key,
		buf, size
	);
	bytes = PROTO_SLAVE_SYNC_PER_SIZE + header.keySize;
	return ret;
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
