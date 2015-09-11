#include <cstdlib>
#include "protocol.hh"
#include "../util/debug.hh"

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

	buf += PROTO_KEY_SIZE;
	memmove( buf, key, keySize );

	bytes += PROTO_KEY_SIZE + keySize;

	return bytes;
}

size_t Protocol::generateKeyValueHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint8_t keySize, char *key, uint32_t valueSize, char *value ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_KEY_VALUE_SIZE + keySize + valueSize );

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

size_t Protocol::generateKeyValueUpdateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_KEY_VALUE_UPDATE_SIZE + keySize + ( valueUpdate ? valueUpdateSize : 0 ) );

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

size_t Protocol::generateChunkUpdateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId, char *delta ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, delta ? PROTO_CHUNK_UPDATE_SIZE + length : PROTO_CHUNK_UPDATE_SIZE );

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( chunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( offset );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( length );
	*( ( uint32_t * )( buf + 20 ) ) = htonl( updatingChunkId );

	buf += PROTO_CHUNK_UPDATE_SIZE;

	if ( delta ) {
		memmove( buf, delta, length );
		bytes += PROTO_CHUNK_UPDATE_SIZE + length;
	} else {
		bytes += PROTO_CHUNK_UPDATE_SIZE;
	}

	return bytes;
}

size_t Protocol::generateChunkHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_SIZE );

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( chunkId );

	bytes += PROTO_CHUNK_SIZE;

	return bytes;
}

size_t Protocol::generateChunkDataHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t chunkSize, char *chunkData ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_DATA_SIZE + chunkSize );

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( chunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( chunkSize );

	buf += PROTO_CHUNK_DATA_SIZE;

	memmove( buf, chunkData, chunkSize );
	bytes += PROTO_CHUNK_DATA_SIZE + chunkSize;

	return bytes;
}

size_t Protocol::generateHeartbeatMessage( uint8_t magic, uint8_t to, uint8_t opcode, struct HeartbeatHeader &header, std::map<Key, OpMetadata> &ops, size_t &count ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	std::map<Key, OpMetadata>::iterator it;
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
		const OpMetadata &opMetadata = it->second;
		if ( this->buffer.size >= bytes + PROTO_SLAVE_SYNC_PER_SIZE + key.size ) {
			// Send buffer still has enough space for holding the current metadata
			buf[ 0 ] = key.size;
			buf[ 1 ] = opMetadata.opcode;
			*( ( uint32_t * )( buf + 2 ) ) = htonl( opMetadata.listId );
			*( ( uint32_t * )( buf + 6 ) ) = htonl( opMetadata.stripeId );
			*( ( uint32_t * )( buf + 10 ) ) = htonl( opMetadata.chunkId );

			buf += PROTO_SLAVE_SYNC_PER_SIZE;
			memcpy( buf, key.data, key.size );
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

size_t Protocol::generateAddressHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t addr, uint16_t port ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_ADDRESS_SIZE );

	// Already in network-byte order
	*( ( uint32_t * )( buf     ) ) = addr;
	*( ( uint16_t * )( buf + 4 ) ) = port;

	bytes += PROTO_ADDRESS_SIZE;

	return bytes;
}

size_t Protocol::generateLoadStatsHeader( uint8_t magic, uint8_t to, uint32_t slaveGetCount, uint32_t slaveSetCount ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to , 0, PROTO_LOAD_STATS_SIZE );
	
	*( ( uint32_t * )( buf ) ) = htonl( slaveGetCount );
	*( ( uint32_t * )( buf + sizeof( uint32_t ) ) ) = htonl( slaveSetCount );

	bytes += PROTO_LOAD_STATS_SIZE;

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
		case PROTO_MAGIC_ANNOUNCEMENT:
		case PROTO_MAGIC_LOADING_STATS:
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
		case PROTO_OPCODE_SLAVE_CONNECTED:
		case PROTO_OPCODE_GET:
		case PROTO_OPCODE_SET:
		case PROTO_OPCODE_UPDATE:
		case PROTO_OPCODE_DELETE:
		case PROTO_OPCODE_UPDATE_CHUNK:
		case PROTO_OPCODE_DELETE_CHUNK:
		case PROTO_OPCODE_GET_CHUNK:
		case PROTO_OPCODE_SET_CHUNK:
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
	unsigned char *tmp;
	keySize = ( uint8_t ) ptr[ 0 ];
	valueSize = 0;
	tmp = ( unsigned char * ) &valueSize;
	tmp[ 1 ] = ptr[ 1 ];
	tmp[ 2 ] = ptr[ 2 ];
	tmp[ 3 ] = ptr[ 3 ];
	valueSize = ntohl( valueSize );

	if ( size < PROTO_KEY_VALUE_SIZE + keySize + valueSize )
		return false;

	key = ptr + PROTO_KEY_VALUE_SIZE;
	value = valueSize ? key + keySize : 0;

	return true;
}

bool Protocol::parseKeyValueUpdateHeader( size_t offset, uint8_t &keySize, char *&key, uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize, char *buf, size_t size ) {
	if ( size < PROTO_KEY_VALUE_UPDATE_SIZE )
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

bool Protocol::parseKeyValueUpdateHeader( size_t offset, uint8_t &keySize, char *&key, uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize, char *&valueUpdate, char *buf, size_t size ) {
	if ( size < PROTO_KEY_VALUE_UPDATE_SIZE )
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

	if ( size < PROTO_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize )
		return false;

	key = ptr + PROTO_KEY_VALUE_UPDATE_SIZE;
	valueUpdate = valueUpdateSize ? key + keySize : 0;

	return true;
}

bool Protocol::parseChunkUpdateHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint32_t &updateOffset, uint32_t &updateLength, uint32_t &updatingChunkId, char *buf, size_t size ) {
	if ( size < PROTO_CHUNK_UPDATE_SIZE )
		return false;

	char *ptr = buf + offset;
	listId          = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId        = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	chunkId         = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	updateOffset    = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	updateLength    = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );
	updatingChunkId = ntohl( *( ( uint32_t * )( ptr + 20 ) ) );

	return true;
}

bool Protocol::parseChunkUpdateHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint32_t &updateOffset, uint32_t &updateLength, uint32_t &updatingChunkId, char *&delta, char *buf, size_t size ) {
	if ( size < PROTO_CHUNK_UPDATE_SIZE )
		return false;

	char *ptr = buf + offset;
	listId          = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId        = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	chunkId         = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	updateOffset    = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	updateLength    = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );
	updatingChunkId = ntohl( *( ( uint32_t * )( ptr + 20 ) ) );

	if ( size < PROTO_CHUNK_UPDATE_SIZE + updateLength )
		return false;

	delta = updateLength ? ptr + PROTO_CHUNK_UPDATE_SIZE : 0;

	return true;
}

bool Protocol::parseChunkHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, char *buf, size_t size ) {
	if ( size < PROTO_CHUNK_SIZE )
		return false;

	char *ptr = buf + offset;
	listId   = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );

	return true;
}

bool Protocol::parseChunkDataHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint32_t &chunkSize, char *&chunkData, char *buf, size_t size ) {
	if ( size < PROTO_CHUNK_DATA_SIZE )
		return false;

	char *ptr = buf + offset;
	listId    = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId  = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	chunkId   = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	chunkSize = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );

	if ( size < PROTO_CHUNK_DATA_SIZE + chunkSize )
		return false;

	chunkData = chunkSize ? ptr + PROTO_CHUNK_DATA_SIZE : 0;

	return true;
}


bool Protocol::parseHeartbeatHeader( size_t offset, uint32_t &get, uint32_t &set, uint32_t &update, uint32_t &del, char *buf, size_t size ) {
	if ( size < PROTO_HEARTBEAT_SIZE )
		return false;

	char *ptr = buf + offset;
	get    = ntohl( *( ( uint32_t * )( ptr      ) ) );
	set    = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	update = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	del    = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );

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

bool Protocol::parseAddressHeader( size_t offset, uint32_t &addr, uint16_t &port, char *buf, size_t size ) {
	if ( size < PROTO_ADDRESS_SIZE )
		return false;

	char *ptr = buf + offset;
	addr = *( ( uint32_t * )( ptr     ) );
	port = *( ( uint16_t * )( ptr + 4 ) );

	return true;
}

bool Protocol::parseLoadStatsHeader( size_t offset, uint32_t &slaveGetCount, uint32_t &slaveSetCount, char *buf, size_t size ) {
	if ( size < PROTO_LOAD_STATS_SIZE )
		return false;
	
	slaveGetCount = ntohl( *( ( uint32_t * )( buf + offset ) ) );
	slaveSetCount = ntohl( *( ( uint32_t * )( buf + offset + sizeof( uint32_t ) ) ) );

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

bool Protocol::parseKeyHeader( struct KeyHeader &header, char *buf, size_t size, size_t offset ) {
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

bool Protocol::parseKeyValueHeader( struct KeyValueHeader &header, char *buf, size_t size, size_t offset ) {
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

bool Protocol::parseKeyValueUpdateHeader( struct KeyValueUpdateHeader &header, bool withValueUpdate, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( withValueUpdate ) {
		return this->parseKeyValueUpdateHeader(
			offset,
			header.keySize,
			header.key,
			header.valueUpdateOffset,
			header.valueUpdateSize,
			header.valueUpdate,
			buf, size
		);
	} else {
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
}

bool Protocol::parseChunkUpdateHeader( struct ChunkUpdateHeader &header, bool withDelta, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( withDelta ) {
		return this->parseChunkUpdateHeader(
			offset,
			header.listId,
			header.stripeId,
			header.chunkId,
			header.offset,
			header.length,
			header.updatingChunkId,
			header.delta,
			buf, size
		);
	} else {
		header.delta = 0;
		return this->parseChunkUpdateHeader(
			offset,
			header.listId,
			header.stripeId,
			header.chunkId,
			header.offset,
			header.length,
			header.updatingChunkId,
			buf, size
		);
	}

}

bool Protocol::parseChunkHeader( struct ChunkHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseChunkHeader(
		offset,
		header.listId,
		header.stripeId,
		header.chunkId,
		buf, size
	);
}

bool Protocol::parseChunkDataHeader( struct ChunkDataHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseChunkDataHeader(
		offset,
		header.listId,
		header.stripeId,
		header.chunkId,
		header.size,
		header.data,
		buf, size
	);
}

bool Protocol::parseHeartbeatHeader( struct HeartbeatHeader &header, char *buf, size_t size, size_t offset ) {
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

bool Protocol::parseSlaveSyncHeader( struct SlaveSyncHeader &header, size_t &bytes, char *buf, size_t size, size_t offset ) {
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

bool Protocol::parseAddressHeader( struct AddressHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseAddressHeader(
		offset,
		header.addr,
		header.port,
		buf, size
	);
}

bool Protocol::parseLoadStatsHeader( struct LoadStatsHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseLoadStatsHeader(
		offset,
		header.slaveGetCount,
		header.slaveSetCount,
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
