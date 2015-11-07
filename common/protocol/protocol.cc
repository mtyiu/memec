#include <cstdlib>
#include <sys/types.h>
#include <signal.h>
#include <pthread.h>
#include "protocol.hh"
#include "../util/debug.hh"

#define PROTO_KEY_VALUE_SIZE	4 // 1 byte for key size, 3 bytes for value size

size_t Protocol::generateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t length, uint32_t id, char *sendBuf ) {
	size_t bytes = 0;
	if ( ! sendBuf ) sendBuf = this->buffer.send;

	sendBuf[ 0 ] = ( ( magic & 0x07 ) | ( this->from & 0x18 ) | ( to & 0x60 ) );
	sendBuf[ 1 ] = opcode & 0xFF;
	bytes = 2;

	*( ( uint32_t * )( sendBuf + bytes ) ) = htonl( length );
	bytes += 4;

	*( ( uint32_t * )( sendBuf + bytes ) ) = htonl( id );
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
		case PROTO_MAGIC_HEARTBEAT:
		case PROTO_MAGIC_REQUEST:
		case PROTO_MAGIC_RESPONSE_SUCCESS:
		case PROTO_MAGIC_RESPONSE_FAILURE:
		case PROTO_MAGIC_ANNOUNCEMENT:
		case PROTO_MAGIC_LOADING_STATS:
		case PROTO_MAGIC_REMAPPING:
			break;
		default:
			fprintf( stderr, "Error #1: (magic, from, to, opcode, length, id) = (%x, %x, %x, %x, %u, %u)\n", magic, from, to, opcode, length, id );
			return false;
	}

	switch( from ) {
		case PROTO_MAGIC_FROM_APPLICATION:
		case PROTO_MAGIC_FROM_COORDINATOR:
		case PROTO_MAGIC_FROM_MASTER:
		case PROTO_MAGIC_FROM_SLAVE:
			break;
		default:
			fprintf( stderr, "Error #2: (magic, from, to, opcode, length, id) = (%x, %x, %x, %x, %u, %u)\n", magic, from, to, opcode, length, id );
			return false;
	}

	if ( to != this->to ) {
		fprintf( stderr, "Error #3: (magic, from, to, opcode, length, id) = (%x, %x, %x, %x, %u, %u)\n", magic, from, to, opcode, length, id );
		return false;
	}

	switch( opcode ) {
		case PROTO_OPCODE_REGISTER:
		case PROTO_OPCODE_SYNC:
		case PROTO_OPCODE_SLAVE_CONNECTED:
		case PROTO_OPCODE_MASTER_PUSH_STATS:
		case PROTO_OPCODE_COORDINATOR_PUSH_STATS:
		case PROTO_OPCODE_SEAL_CHUNKS:
		case PROTO_OPCODE_FLUSH_CHUNKS:
		case PROTO_OPCODE_RECOVERY:

		case PROTO_OPCODE_GET:
		case PROTO_OPCODE_SET:
		case PROTO_OPCODE_UPDATE:
		case PROTO_OPCODE_DELETE:
		case PROTO_OPCODE_REDIRECT_GET:
		case PROTO_OPCODE_REDIRECT_UPDATE:
		case PROTO_OPCODE_REDIRECT_DELETE:
		case PROTO_OPCODE_DEGRADED_GET:
		case PROTO_OPCODE_DEGRADED_UPDATE:
		case PROTO_OPCODE_DEGRADED_DELETE:

		case PROTO_OPCODE_REMAPPING_LOCK:
		case PROTO_OPCODE_REMAPPING_SET:
		case PROTO_OPCODE_DEGRADED_LOCK:
		case PROTO_OPCODE_DEGRADED_UNLOCK:

		case PROTO_OPCODE_REMAPPING_UNLOCK:
		case PROTO_OPCODE_SEAL_CHUNK:
		case PROTO_OPCODE_UPDATE_CHUNK:
		case PROTO_OPCODE_DELETE_CHUNK:
		case PROTO_OPCODE_GET_CHUNK:
		case PROTO_OPCODE_SET_CHUNK:
			break;
		default:
			fprintf( stderr, "Error #4: (magic, from, to, opcode, length, id) = (%x, %x, %x, %x, %u, %u)\n", magic, from, to, opcode, length, id );
			return false;
	}

	return true;
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
		header.id,
		buf, size
	);
}

//////////////
// Register //
//////////////
size_t Protocol::generateAddressHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t addr, uint16_t port ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_ADDRESS_SIZE, id );

	// Already in network-byte order
	*( ( uint32_t * )( buf     ) ) = addr;
	*( ( uint16_t * )( buf + 4 ) ) = port;

	bytes += PROTO_ADDRESS_SIZE;

	return bytes;
}

bool Protocol::parseAddressHeader( size_t offset, uint32_t &addr, uint16_t &port, char *buf, size_t size ) {
	if ( size < PROTO_ADDRESS_SIZE )
		return false;

	char *ptr = buf + offset;
	addr = *( ( uint32_t * )( ptr     ) );
	port = *( ( uint16_t * )( ptr + 4 ) );

	return true;
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

//////////////////////////////////////////
// Heartbeat & metadata synchronization //
//////////////////////////////////////////
size_t Protocol::generateHeartbeatMessage(
	uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
	LOCK_T *sealedLock, std::unordered_set<Metadata> &sealed, uint32_t &sealedCount,
	LOCK_T *opsLock, std::unordered_map<Key, OpMetadata> &ops, uint32_t &opsCount,
	bool &isCompleted
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = PROTO_HEADER_SIZE;
	uint32_t *sealedPtr, *opsPtr;
	std::unordered_set<Metadata>::iterator sealedIt;
	std::unordered_map<Key, OpMetadata>::iterator opsIt;

	sealedCount = 0;
	opsCount = 0;
	isCompleted = true;

	sealedPtr       = ( uint32_t * )( buf     );
	opsPtr          = ( uint32_t * )( buf + 4 );

	buf += PROTO_HEARTBEAT_SIZE;
	bytes += PROTO_HEARTBEAT_SIZE;

	/**** Sealed chunks *****/
	LOCK( sealedLock );
	for ( sealedIt = sealed.begin(); sealedIt != sealed.end(); sealedIt++ ) {
		const Metadata &metadata = *sealedIt;
		if ( this->buffer.size >= bytes + PROTO_METADATA_SIZE ) {
			*( ( uint32_t * )( buf     ) ) = htonl( metadata.listId );
			*( ( uint32_t * )( buf + 4 ) ) = htonl( metadata.stripeId );
			*( ( uint32_t * )( buf + 8 ) ) = htonl( metadata.chunkId );
			buf   += PROTO_METADATA_SIZE;
			bytes += PROTO_METADATA_SIZE;
			sealedCount++;
		} else {
			isCompleted = false;
			break;
		}
	}
	sealed.erase( sealed.begin(), sealedIt );
	UNLOCK( sealedLock );

	/***** Keys in SET and DELETE requests *****/
	LOCK( opsLock );
	for ( opsIt = ops.begin(); opsIt != ops.end(); opsIt++ ) {
		Key key = opsIt->first;
		const OpMetadata &opMetadata = opsIt->second;
		if ( this->buffer.size >= bytes + PROTO_KEY_OP_METADATA_SIZE + key.size ) {
			buf[ 0 ] = key.size;
			buf[ 1 ] = opMetadata.opcode;
			*( ( uint32_t * )( buf + 2 ) ) = htonl( opMetadata.listId );
			*( ( uint32_t * )( buf + 6 ) ) = htonl( opMetadata.stripeId );
			*( ( uint32_t * )( buf + 10 ) ) = htonl( opMetadata.chunkId );

			buf += PROTO_KEY_OP_METADATA_SIZE;
			memcpy( buf, key.data, key.size );
			buf += key.size;
			bytes += PROTO_KEY_OP_METADATA_SIZE + key.size;
			opsCount++;
			key.free();
		} else {
			isCompleted = false;
			break;
		}
	}
	ops.erase( ops.begin(), opsIt );
	UNLOCK( opsLock );

	*sealedPtr = htonl( sealedCount );
	*opsPtr = htonl( opsCount );

	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, id );

	return bytes;
}

bool Protocol::parseHeartbeatHeader( size_t offset, uint32_t &sealed, uint32_t &keys, char *buf, size_t size ) {
	if ( size < PROTO_HEARTBEAT_SIZE )
		return false;

	char *ptr = buf + offset;
	sealed = ntohl( *( ( uint32_t * )( ptr     ) ) );
	keys   = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );

	return true;
}

bool Protocol::parseHeartbeatHeader( struct HeartbeatHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseHeartbeatHeader(
		offset,
		header.sealed,
		header.keys,
		buf, size
	);
}

bool Protocol::parseMetadataHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, char *buf, size_t size ) {
	if ( size - offset < PROTO_METADATA_SIZE )
		return false;

	char *ptr = buf + offset;
	listId   = ntohl( *( ( uint32_t * )( ptr     ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );

	return true;
}

bool Protocol::parseMetadataHeader( struct MetadataHeader &header, size_t &bytes, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	bool ret = this->parseMetadataHeader(
		offset,
		header.listId,
		header.stripeId,
		header.chunkId,
		buf, size
	);
	bytes = PROTO_METADATA_SIZE;
	return ret;
}

bool Protocol::parseKeyOpMetadataHeader( size_t offset, uint8_t &keySize, uint8_t &opcode, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, char *&key, char *buf, size_t size ) {
	if ( size - offset < PROTO_KEY_OP_METADATA_SIZE )
		return false;

	char *ptr = buf + offset;
	keySize  = ( uint8_t ) ptr[ 0 ];
	opcode   = ( uint8_t ) ptr[ 1 ];
	listId   = ntohl( *( ( uint32_t * )( ptr +  2 ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr +  6 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr + 10 ) ) );

	if ( size < PROTO_KEY_OP_METADATA_SIZE + ( size_t ) keySize )
		return false;

	key = ptr + PROTO_KEY_OP_METADATA_SIZE;

	return true;
}

bool Protocol::parseKeyOpMetadataHeader( struct KeyOpMetadataHeader &header, size_t &bytes, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	bool ret = this->parseKeyOpMetadataHeader(
		offset,
		header.keySize,
		header.opcode,
		header.listId,
		header.stripeId,
		header.chunkId,
		header.key,
		buf, size
	);
	bytes = PROTO_KEY_OP_METADATA_SIZE + header.keySize;
	return ret;
}

size_t Protocol::generateRemappingRecordMessage( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, LOCK_T *lock, std::unordered_map<Key, RemappingRecord> &remapRecords, size_t &remapCount ) {
	uint32_t bytes = 0;
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	std::unordered_map<Key, RemappingRecord>::iterator rit;
	bytes = PROTO_HEADER_SIZE;
	remapCount = 0;

	// reserve the header and set the record count after copying data into buffer
	buf += PROTO_REMAPPING_RECORD_SIZE;
	bytes += PROTO_REMAPPING_RECORD_SIZE;

	// append remapping record
	pthread_mutex_lock( lock );
	for ( rit = remapRecords.begin(); rit != remapRecords.end(); rit++ ) {
		if ( rit->second.sent )
			continue;
		const Key &key = rit->first;
		if ( this->buffer.size >= bytes + PROTO_SLAVE_SYNC_REMAP_PER_SIZE + key.size ) {
			buf[ 0 ] = key.size;
			buf[ 1 ] = ( rit->second.valid )? 1 : 0; // distinguish add and del
			*( ( uint32_t * )( buf + 2 ) ) = htonl( rit->second.listId );
			*( ( uint32_t * )( buf + 6 ) ) = htonl( rit->second.chunkId );

			buf += PROTO_SLAVE_SYNC_REMAP_PER_SIZE;
			memcpy( buf, key.data, key.size );

			buf += key.size;
			bytes += PROTO_SLAVE_SYNC_REMAP_PER_SIZE + key.size;
			rit->second.sent = true;
			remapCount++;
		} else {
			break;
		}
	}
	pthread_mutex_unlock( lock );

	// set the record count
	*( ( uint32_t * )( this->buffer.send + PROTO_HEADER_SIZE ) ) = htonl( remapCount );

	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, id );

	return bytes;
}

bool Protocol::parseRemappingRecordHeader( size_t offset, uint32_t &remap, char* buf, size_t size ) {
	if ( size < PROTO_REMAPPING_RECORD_SIZE )
		return false;

	char *ptr = buf + offset;
	remap = ntohl( *( ( uint32_t * )( ptr ) ) );

	return true;
}

bool Protocol::parseRemappingRecordHeader( struct RemappingRecordHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseRemappingRecordHeader(
		offset,
		header.remap,
		buf, size
	);
}

bool Protocol::parseSlaveSyncRemapHeader( size_t offset, uint8_t &keySize, uint8_t &opcode, uint32_t &listId, uint32_t &chunkId, char *&key, char *buf, size_t size ) {
	if ( size < PROTO_SLAVE_SYNC_REMAP_PER_SIZE )
		return false;

	char *ptr = buf + offset;
	keySize = ( uint8_t ) ptr[ 0 ];
	opcode = ( uint8_t ) ptr[ 1 ];
	listId = ntohl( *( ( uint32_t * )( ptr + 2 ) ) );
	chunkId = ntohl( *( ( uint32_t * )( ptr + 6 ) ) );

	if ( size < PROTO_SLAVE_SYNC_REMAP_PER_SIZE + ( size_t ) keySize )
		return false;

	key = ptr + PROTO_SLAVE_SYNC_REMAP_PER_SIZE;

	return true;
}

bool Protocol::parseSlaveSyncRemapHeader( struct SlaveSyncRemapHeader &header, size_t &bytes, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	bool ret = this->parseSlaveSyncRemapHeader(
		offset,
		header.keySize,
		header.opcode,
		header.listId,
		header.chunkId,
		header.key,
		buf, size
	);
	bytes = PROTO_SLAVE_SYNC_REMAP_PER_SIZE + header.keySize;

	return ret;
}

//////////////////////////
// Load synchronization //
//////////////////////////
size_t Protocol::generateLoadStatsHeader( uint8_t magic, uint8_t to, uint32_t id, uint32_t slaveGetCount, uint32_t slaveSetCount, uint32_t slaveOverloadCount, uint32_t recordSize, uint32_t slaveAddrSize ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, 0, PROTO_LOAD_STATS_SIZE + ( slaveGetCount + slaveSetCount ) * recordSize + ( slaveOverloadCount * slaveAddrSize ), id );

	*( ( uint32_t * )( buf ) ) = htonl( slaveGetCount );
	*( ( uint32_t * )( buf + sizeof( uint32_t ) ) ) = htonl( slaveSetCount );
	*( ( uint32_t * )( buf + sizeof( uint32_t ) * 2 ) ) = htonl( slaveOverloadCount );

	bytes += PROTO_LOAD_STATS_SIZE;

	return bytes;
}

bool Protocol::parseLoadStatsHeader( size_t offset, uint32_t &slaveGetCount, uint32_t &slaveSetCount, uint32_t &slaveOverloadCount, char *buf, size_t size ) {
	if ( size < PROTO_LOAD_STATS_SIZE )
		return false;

	slaveGetCount = ntohl( *( ( uint32_t * )( buf + offset ) ) );
	slaveSetCount = ntohl( *( ( uint32_t * )( buf + offset + sizeof( uint32_t ) ) ) );
	slaveOverloadCount = ntohl( *( ( uint32_t * )( buf + offset + sizeof( uint32_t ) * 2 ) ) );

	return true;
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
		header.slaveOverloadCount,
		buf, size
	);
}

///////////////////////
// Normal operations //
///////////////////////
size_t Protocol::generateKeyHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint8_t keySize, char *key, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_KEY_SIZE + keySize, id, sendBuf );

	buf[ 0 ] = keySize;

	buf += PROTO_KEY_SIZE;
	memmove( buf, key, keySize );

	bytes += PROTO_KEY_SIZE + keySize;

	return bytes;
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

size_t Protocol::generateChunkKeyHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint8_t keySize, char *key, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_KEY_SIZE + keySize, id, sendBuf );

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf + 8 ) ) = htonl( chunkId );
	buf[ 12 ] = keySize;

	buf += PROTO_CHUNK_KEY_SIZE;
	memmove( buf, key, keySize );

	bytes += PROTO_CHUNK_KEY_SIZE + keySize;

	return bytes;
}

bool Protocol::parseChunkKeyHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint8_t &keySize, char *&key, char *buf, size_t size ) {
	if ( size < PROTO_CHUNK_KEY_SIZE )
		return false;

	char *ptr = buf + offset;

	listId   = ntohl( *( ( uint32_t * )( ptr     ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );
	keySize = ( uint8_t ) ptr[ 12 ];

	if ( size < ( size_t ) PROTO_CHUNK_KEY_SIZE + keySize )
		return false;

	key = ptr + PROTO_CHUNK_KEY_SIZE;

	return true;
}

bool Protocol::parseChunkKeyHeader( struct ChunkKeyHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseChunkKeyHeader(
		offset,
		header.listId,
		header.stripeId,
		header.chunkId,
		header.keySize,
		header.key,
		buf, size
	);
}

size_t Protocol::generateChunkKeyValueUpdateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, uint32_t chunkUpdateOffset, char *valueUpdate, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE + keySize + ( valueUpdate ? valueUpdateSize : 0 ), id, sendBuf );

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf + 8 ) ) = htonl( chunkId );
	buf += 12;

	buf[ 0 ] = keySize;

	unsigned char *tmp;
	valueUpdateSize = htonl( valueUpdateSize );
	valueUpdateOffset = htonl( valueUpdateOffset );
	chunkUpdateOffset = htonl( chunkUpdateOffset );
	tmp = ( unsigned char * ) &valueUpdateSize;
	buf[ 1 ] = tmp[ 1 ];
	buf[ 2 ] = tmp[ 2 ];
	buf[ 3 ] = tmp[ 3 ];
	tmp = ( unsigned char * ) &valueUpdateOffset;
	buf[ 4 ] = tmp[ 1 ];
	buf[ 5 ] = tmp[ 2 ];
	buf[ 6 ] = tmp[ 3 ];
	tmp = ( unsigned char * ) &chunkUpdateOffset;
	buf[ 7 ] = tmp[ 1 ];
	buf[ 8 ] = tmp[ 2 ];
	buf[ 9 ] = tmp[ 3 ];
	valueUpdateSize = ntohl( valueUpdateSize );
	valueUpdateOffset = ntohl( valueUpdateOffset );
	chunkUpdateOffset = ntohl( chunkUpdateOffset );

	buf += 10;
	memmove( buf, key, keySize );
	buf += keySize;
	if ( valueUpdateSize && valueUpdate ) {
		memmove( buf, valueUpdate, valueUpdateSize );
		bytes += PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize;
	} else {
		bytes += PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE + keySize;
	}

	return bytes;
}

bool Protocol::parseChunkKeyValueUpdateHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint8_t &keySize, char *&key, uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize, uint32_t &chunkUpdateOffset, char *buf, size_t size ) {
	if ( size < PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE )
		return false;

	char *ptr = buf + offset;
	unsigned char *tmp;

	listId   = ntohl( *( ( uint32_t * )( ptr     ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );
	ptr += 12;

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

	chunkUpdateOffset = 0;
	tmp = ( unsigned char * ) &chunkUpdateOffset;
	tmp[ 1 ] = ptr[ 7 ];
	tmp[ 2 ] = ptr[ 8 ];
	tmp[ 3 ] = ptr[ 9 ];
	chunkUpdateOffset = ntohl( chunkUpdateOffset );

	key = ptr + 10;

	return true;
}

bool Protocol::parseChunkKeyValueUpdateHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint8_t &keySize, char *&key, uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize, uint32_t &chunkUpdateOffset, char *&valueUpdate, char *buf, size_t size ) {
	if ( size < PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE )
		return false;

	char *ptr = buf + offset;
	unsigned char *tmp;

	listId   = ntohl( *( ( uint32_t * )( ptr     ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );
	ptr += 12;

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

	chunkUpdateOffset = 0;
	tmp = ( unsigned char * ) &chunkUpdateOffset;
	tmp[ 1 ] = ptr[ 7 ];
	tmp[ 2 ] = ptr[ 8 ];
	tmp[ 3 ] = ptr[ 9 ];
	chunkUpdateOffset = ntohl( chunkUpdateOffset );

	if ( size < PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize )
		return false;

	key = ptr + 10;
	valueUpdate = valueUpdateSize ? key + keySize : 0;

	return true;
}

bool Protocol::parseChunkKeyValueUpdateHeader( struct ChunkKeyValueUpdateHeader &header, bool withValueUpdate, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( withValueUpdate ) {
		return this->parseChunkKeyValueUpdateHeader(
			offset,
			header.listId,
			header.stripeId,
			header.chunkId,
			header.keySize,
			header.key,
			header.valueUpdateOffset,
			header.valueUpdateSize,
			header.chunkUpdateOffset,
			header.valueUpdate,
			buf, size
		);
	} else {
		header.valueUpdate = 0;
		return this->parseChunkKeyValueUpdateHeader(
			offset,
			header.listId,
			header.stripeId,
			header.chunkId,
			header.keySize,
			header.key,
			header.valueUpdateOffset,
			header.valueUpdateSize,
			header.chunkUpdateOffset,
			buf, size
		);
	}
}

size_t Protocol::generateKeyValueHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint8_t keySize, char *key, uint32_t valueSize, char *value, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_KEY_VALUE_SIZE + keySize + valueSize, id, sendBuf );

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

size_t Protocol::generateKeyValueUpdateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_KEY_VALUE_UPDATE_SIZE + keySize + ( valueUpdate ? valueUpdateSize : 0 ),
		id
	);

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

size_t Protocol::generateChunkUpdateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId, char *delta, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, delta ? PROTO_CHUNK_UPDATE_SIZE + length : PROTO_CHUNK_UPDATE_SIZE, id, sendBuf );

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

///////////////
// Remapping //
///////////////
size_t Protocol::generateRemappingLockHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_REMAPPING_LOCK_SIZE + keySize, id );

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( chunkId );
	bytes += 8;
	buf += 8;

	buf[ 0 ] = keySize;
	bytes++;
	buf++;

	memmove( buf, key, keySize );
	bytes += keySize;

	return bytes;
}

bool Protocol::parseRemappingLockHeader( size_t offset, uint32_t &listId, uint32_t &chunkId, uint8_t &keySize, char *&key, char *buf, size_t size ) {
	if ( size < PROTO_REMAPPING_LOCK_SIZE )
		return false;

	char *ptr = buf + offset;
	listId  = ntohl( *( ( uint32_t * )( ptr      ) ) );
	chunkId = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	keySize = *( ptr + 8 );

	if ( size < ( size_t ) PROTO_REMAPPING_LOCK_SIZE + keySize )
		return false;

	key = ptr + 9;

	return true;
}

bool Protocol::parseRemappingLockHeader( struct RemappingLockHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseRemappingLockHeader(
		offset,
		header.listId,
		header.chunkId,
		header.keySize,
		header.key,
		buf, size
	);
}

size_t Protocol::generateRemappingSetHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t chunkId, bool needsForwarding, uint8_t keySize, char *key, uint32_t valueSize, char *value, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_REMAPPING_SET_SIZE + keySize + valueSize, id, sendBuf );

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( chunkId );
	bytes += 8;
	buf += 8;

	unsigned char *tmp;
	valueSize = htonl( valueSize );
	tmp = ( unsigned char * ) &valueSize;
	buf[ 0 ] = needsForwarding;
	buf[ 1 ] = keySize;
	buf[ 2 ] = tmp[ 1 ];
	buf[ 3 ] = tmp[ 2 ];
	buf[ 4 ] = tmp[ 3 ];
	bytes += 5;
	buf += 5;
	valueSize = ntohl( valueSize );

	memmove( buf, key, keySize );
	bytes += keySize;
	buf += keySize;

	memmove( buf, value, valueSize );
	bytes += valueSize;

	return bytes;
}

bool Protocol::parseRemappingSetHeader( size_t offset, uint32_t &listId, uint32_t &chunkId, bool &needsForwarding, uint8_t &keySize, char *&key, uint32_t &valueSize, char *&value, char *buf, size_t size ) {
	if ( size < PROTO_REMAPPING_SET_SIZE )
		return false;

	char *ptr = buf + offset;
	unsigned char *tmp;
	listId  = ntohl( *( ( uint32_t * )( ptr      ) ) );
	chunkId = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	needsForwarding = *( ptr + 8 );
	keySize = *( ptr + 9 );
	ptr += 10;

	valueSize = 0;
	tmp = ( unsigned char * ) &valueSize;
	tmp[ 1 ] = ptr[ 0 ];
	tmp[ 2 ] = ptr[ 1 ];
	tmp[ 3 ] = ptr[ 2 ];
	valueSize = ntohl( valueSize );
	ptr += 3;

	if ( size < PROTO_REMAPPING_SET_SIZE + keySize + valueSize )
		return false;

	key = ptr;
	value = ptr + keySize;

	return true;
}

bool Protocol::parseRemappingSetHeader( struct RemappingSetHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseRemappingSetHeader(
		offset,
		header.listId,
		header.chunkId,
		header.needsForwarding,
		header.keySize,
		header.key,
		header.valueSize,
		header.value,
		buf, size
	);
}

size_t Protocol::generateRedirectHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint8_t keySize, char *key, uint32_t remappedListId, uint32_t remappedChunkId ) {
	size_t bytes = this->generateKeyHeader( magic, to, opcode, id, keySize, key );
	char *buf = this->buffer.send + bytes;

	*( ( uint32_t * )( buf ) ) = htonl( remappedListId );
	*( ( uint32_t * )( buf + sizeof( uint32_t ) ) ) = htonl( remappedChunkId );

	bytes += PROTO_REDIRECT_SIZE;

	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, id );

	return bytes;
}

bool Protocol::parseRedirectHeader( size_t offset, uint8_t &keySize, char *&key, uint32_t &remappedListId, uint32_t &remappedChunkId, char *buf, size_t size ) {
	if ( ( ! this->parseKeyHeader( offset, keySize, key, buf, size ) ) || ( size < ( size_t ) PROTO_KEY_SIZE + keySize + PROTO_REDIRECT_SIZE ) )
		return false;

	offset += PROTO_KEY_SIZE + keySize;
	char *ptr = buf + offset;
	remappedListId = ntohl( *( ( uint32_t * )( ptr ) ) );
	remappedChunkId = ntohl( *( ( uint32_t * )( ptr + sizeof( uint32_t ) ) ) );

	return true;
}

bool Protocol::parseRedirectHeader( struct RedirectHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseRedirectHeader(
		offset,
		header.keySize,
		header.key,
		header.listId,
		header.chunkId,
		buf, size
	);
}

//////////////////////////
// Degraded prefetching //
//////////////////////////
size_t Protocol::generateDegradedLockReqHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t srcListId, uint32_t srcChunkId, uint32_t dstListId, uint32_t dstChunkId, uint8_t keySize, char *key ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_DEGRADED_LOCK_REQ_SIZE + keySize, id );

	*( ( uint32_t * )( buf      ) ) = htonl( srcListId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( srcChunkId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( dstListId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( dstChunkId );
	buf[ 16 ] = keySize;

	buf += PROTO_DEGRADED_LOCK_REQ_SIZE;
	memmove( buf, key, keySize );

	bytes += PROTO_DEGRADED_LOCK_REQ_SIZE + keySize;

	return bytes;
}

bool Protocol::parseDegradedLockReqHeader( size_t offset, uint32_t &srcListId, uint32_t &srcChunkId, uint32_t &dstListId, uint32_t &dstChunkId, uint8_t &keySize, char *&key, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_LOCK_REQ_SIZE )
		return false;

	char *ptr = buf + offset;
	srcListId  = ntohl( *( ( uint32_t * )( ptr      ) ) );
	srcChunkId = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	dstListId  = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	dstChunkId = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	keySize = ptr[ 16 ];

	if ( size < PROTO_DEGRADED_LOCK_REQ_SIZE + ( size_t ) keySize )
		return false;

	key = ptr + PROTO_DEGRADED_LOCK_REQ_SIZE;

	return true;
}

bool Protocol::parseDegradedLockReqHeader( struct DegradedLockReqHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseDegradedLockReqHeader(
		offset,
		header.srcListId,
		header.srcChunkId,
		header.dstListId,
		header.dstChunkId,
		header.keySize,
		header.key,
		buf, size
	);
}

size_t Protocol::generateDegradedLockResHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint8_t type, uint8_t keySize, char *key, char *&buf ) {
	uint32_t length;
	buf = this->buffer.send + PROTO_HEADER_SIZE;
	switch( type ) {
		case PROTO_DEGRADED_LOCK_RES_IS_LOCKED:
		case PROTO_DEGRADED_LOCK_RES_WAS_LOCKED:
			length = PROTO_DEGRADED_LOCK_RES_LOCK_SIZE;
			break;
		case PROTO_DEGRADED_LOCK_RES_REMAPPED:
			length = PROTO_DEGRADED_LOCK_RES_REMAP_SIZE;
			break;
		case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
		default:
			length = 0;
			break;
	}
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_LOCK_RES_BASE_SIZE + keySize + length,
		id
	);

	buf[ 0 ] = type;
	buf[ 1 ] = keySize;

	buf += PROTO_DEGRADED_LOCK_RES_BASE_SIZE;
	memmove( buf, key, keySize );
	buf += keySize;

	bytes += PROTO_DEGRADED_LOCK_RES_BASE_SIZE + keySize;

	return bytes;
}

size_t Protocol::generateDegradedLockResHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, bool isLocked, bool isSealed, uint8_t keySize, char *key, uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId, uint32_t dstListId, uint32_t dstChunkId ) {
	char *buf;
	size_t bytes = this->generateDegradedLockResHeader(
		magic, to, opcode, id,
		isLocked ? PROTO_DEGRADED_LOCK_RES_IS_LOCKED : PROTO_DEGRADED_LOCK_RES_WAS_LOCKED,
		keySize, key, buf
	);

	*( ( uint32_t * )( buf      ) ) = htonl( srcListId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( srcStripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( srcChunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( dstListId );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( dstChunkId );
	buf[ 20 ] = isSealed;

	bytes += PROTO_DEGRADED_LOCK_RES_LOCK_SIZE;

	return bytes;
}

size_t Protocol::generateDegradedLockResHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint8_t keySize, char *key, uint32_t listId, uint32_t chunkId ) {
	char *buf;
	size_t bytes = this->generateDegradedLockResHeader(
		magic, to, opcode, id,
		PROTO_DEGRADED_LOCK_RES_REMAPPED,
		keySize, key, buf
	);

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( chunkId );

	bytes += PROTO_DEGRADED_LOCK_RES_REMAP_SIZE;

	return bytes;
}

size_t Protocol::generateDegradedLockResHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint8_t keySize, char *key ) {
	char *buf;
	size_t bytes = this->generateDegradedLockResHeader(
		magic, to, opcode, id,
		PROTO_DEGRADED_LOCK_RES_NOT_EXIST,
		keySize, key, buf
	);
	return bytes;
}

bool Protocol::parseDegradedLockResHeader( size_t offset, uint8_t &type, uint8_t &keySize, char *&key, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_LOCK_RES_BASE_SIZE )
		return false;

	char *ptr = buf + offset;
	type = ptr[ 0 ];
	keySize = ptr[ 1 ];

	if ( size < PROTO_DEGRADED_LOCK_RES_BASE_SIZE + ( size_t ) keySize )
		return false;

	key = ptr + PROTO_DEGRADED_LOCK_RES_BASE_SIZE;

	return true;
}

bool Protocol::parseDegradedLockResHeader( size_t offset, uint32_t &srcListId, uint32_t &srcStripeId, uint32_t &srcChunkId, uint32_t &dstListId, uint32_t &dstChunkId, bool &isSealed, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_LOCK_RES_LOCK_SIZE )
		return false;

	char *ptr = buf + offset;
	srcListId   = ntohl( *( ( uint32_t * )( ptr      ) ) );
	srcStripeId = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	srcChunkId  = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	dstListId   = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	dstChunkId  = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );
	isSealed = ptr[ 20 ];

	return true;
}

bool Protocol::parseDegradedLockResHeader( size_t offset, uint32_t &listId, uint32_t &chunkId, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_LOCK_RES_REMAP_SIZE )
		return false;

	char *ptr = buf + offset;
	listId   = ntohl( *( ( uint32_t * )( ptr     ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );

	return true;
}

bool Protocol::parseDegradedLockResHeader( struct DegradedLockResHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	bool ret = this->parseDegradedLockResHeader(
		offset,
		header.type,
		header.keySize,
		header.key,
		buf, size
	);
	if ( ! ret )
		return false;

	offset += PROTO_DEGRADED_LOCK_RES_BASE_SIZE + header.keySize;
	switch( header.type ) {
		case PROTO_DEGRADED_LOCK_RES_IS_LOCKED:
		case PROTO_DEGRADED_LOCK_RES_WAS_LOCKED:
			ret = this->parseDegradedLockResHeader(
				offset,
				header.srcListId,
				header.srcStripeId,
				header.srcChunkId,
				header.dstListId,
				header.dstChunkId,
				header.isSealed,
				buf, size
			);
			break;
		case PROTO_DEGRADED_LOCK_RES_REMAPPED:
			ret = this->parseDegradedLockResHeader(
				offset,
				header.srcListId,
				header.srcChunkId,
				buf, size
			);
			break;
		case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
			return true;
		default:
			return false;
	}
	return ret;
}

size_t Protocol::generateDegradedReqHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isSealed, uint8_t keySize, char *key ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_REQ_BASE_SIZE + PROTO_KEY_SIZE + keySize,
		id
	);

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf + 8 ) ) = htonl( chunkId );
	buf += PROTO_DEGRADED_REQ_BASE_SIZE;

	buf[ 0 ] = keySize;

	buf += PROTO_KEY_SIZE;
	memmove( buf, key, keySize );

	bytes += PROTO_DEGRADED_REQ_BASE_SIZE + PROTO_KEY_SIZE + keySize;

	return bytes;
}

size_t Protocol::generateDegradedReqHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isSealed, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_REQ_BASE_SIZE + PROTO_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize,
		id
	);

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf + 8 ) ) = htonl( chunkId );
	buf[ 12 ] = isSealed;
	buf += PROTO_DEGRADED_REQ_BASE_SIZE;

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
	memmove( buf, valueUpdate, valueUpdateSize );

	bytes += PROTO_DEGRADED_REQ_BASE_SIZE + PROTO_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize;

	return bytes;
}

bool Protocol::parseDegradedReqHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, bool &isSealed, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_REQ_BASE_SIZE )
		return false;

	char *ptr = buf + offset;
	listId   = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	isSealed = ptr[ 12 ];

	return true;
}

bool Protocol::parseDegradedReqHeader( struct DegradedReqHeader &header, uint8_t opcode, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	bool ret = this->parseDegradedReqHeader(
		offset,
		header.listId,
		header.stripeId,
		header.chunkId,
		header.isSealed,
		buf, size
	);
	if ( ! ret )
		return false;

	offset += PROTO_DEGRADED_REQ_BASE_SIZE;
	switch( opcode ) {
		case PROTO_OPCODE_DEGRADED_GET:
		case PROTO_OPCODE_DEGRADED_DELETE:
			ret = this->parseKeyHeader(
				offset,
				header.data.key.keySize,
				header.data.key.key,
				buf, size
			);
			break;
		case PROTO_OPCODE_DEGRADED_UPDATE:
			ret = this->parseKeyValueUpdateHeader(
				offset,
				header.data.keyValueUpdate.keySize,
				header.data.keyValueUpdate.key,
				header.data.keyValueUpdate.valueUpdateOffset,
				header.data.keyValueUpdate.valueUpdateSize,
				header.data.keyValueUpdate.valueUpdate,
				buf, size
			);
			break;
		default:
			return false;
	}
	return ret;
}

//////////////
// Recovery //
//////////////
size_t Protocol::generateRecoveryHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t stripeIdFrom, uint32_t stripeIdTo, uint32_t chunkId, uint32_t addr, uint16_t port, std::unordered_set<Metadata> unsealedChunks, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes;
	uint32_t count = 0;

	buf += PROTO_HEADER_SIZE;
	bytes = PROTO_HEADER_SIZE;

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeIdFrom );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( stripeIdTo );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( chunkId );
	*( ( uint32_t * )( buf + 20 ) ) = htonl( addr );
	*( ( uint32_t * )( buf + 24 ) ) = htons( port );

	buf += PROTO_RECOVERY_SIZE;
	bytes += PROTO_RECOVERY_SIZE;

	std::unordered_set<Metadata>::iterator it = unsealedChunks.begin();
	while ( bytes + PROTO_CHUNK_SIZE <= this->buffer.size && it != unsealedChunks.end() ) {
		*( ( uint32_t * )( buf      ) ) = htonl( it->listId );
		*( ( uint32_t * )( buf +  4 ) ) = htonl( it->stripeId );
		*( ( uint32_t * )( buf +  8 ) ) = htonl( it->chunkId );

		buf += PROTO_CHUNK_SIZE;
		bytes += PROTO_CHUNK_SIZE;

		count++;
		it++;
	}
	unsealedChunks.erase( unsealedChunks.begin(), it );

	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, id, sendBuf );
	*( ( uint32_t * )( sendBuf + PROTO_HEADER_SIZE + 16 ) ) = htonl( count );

	return bytes;
}

bool Protocol::parseRecoveryHeader( size_t offset, uint32_t &listId, uint32_t &stripeIdFrom, uint32_t &stripeIdTo, uint32_t &chunkId, uint32_t &unsealedChunkCount, uint32_t &addr, uint16_t &port, char *buf, size_t size ) {
	if ( size < PROTO_RECOVERY_SIZE )
		return false;

	char *ptr = buf + offset;
	listId             = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeIdFrom       = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	stripeIdTo         = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	chunkId            = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	unsealedChunkCount = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );
	addr               = ntohl( *( ( uint32_t * )( ptr + 20 ) ) );
	port               = ntohs( *( ( uint32_t * )( ptr + 24 ) ) );

	return true;
}

bool Protocol::parseRecoveryHeader( struct RecoveryHeader &header, std::unordered_set<Metadata> &unsealedChunks, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( this->parseRecoveryHeader( offset, header.listId, header.stripeIdFrom, header.stripeIdTo, header.chunkId, header.unsealedChunkCount, header.addr, header.port, buf, size ) ) {
		uint32_t count = 0;
		offset += PROTO_RECOVERY_SIZE;

		while ( count < header.unsealedChunkCount ) {
			Metadata metadata;
			if ( ! this->parseChunkHeader( offset, metadata.listId, metadata.stripeId, metadata.chunkId, buf, size ) )
				return false;
			count++;
			offset += PROTO_CHUNK_SIZE;

			unsealedChunks.insert( metadata );

			if ( offset > size )
				return false;
		}

		return true;
	} else {
		return false;
	}
}

//////////
// Seal //
//////////
size_t Protocol::generateChunkSealHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t count, uint32_t dataLength, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_SEAL_SIZE + dataLength, id, sendBuf );

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( chunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( count );

	bytes += PROTO_CHUNK_SEAL_SIZE + dataLength;

	return bytes;
}

bool Protocol::parseChunkSealHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint32_t &count, char *buf, size_t size ) {
	if ( size < PROTO_CHUNK_SEAL_SIZE )
		return false;

	char *ptr = buf + offset;
	listId   = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	count    = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );

	return true;
}

bool Protocol::parseChunkSealHeader( struct ChunkSealHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseChunkSealHeader(
		offset,
		header.listId,
		header.stripeId,
		header.chunkId,
		header.count,
		buf, size
	);
}

bool Protocol::parseChunkSealHeaderData( size_t offset, uint8_t &keySize, uint32_t &keyOffset, char *&key, char *buf, size_t size ) {
	// Note: Also implemented in slave/buffer/parity_chunk_buffer.cc
	if ( size < PROTO_CHUNK_SEAL_DATA_SIZE )
		return false;

	char *ptr = buf + offset;
	keySize = ptr[ 0 ];
	offset  = ntohl( *( ( uint32_t * )( ptr + 1 ) ) );

	if ( size < ( size_t ) PROTO_CHUNK_SEAL_DATA_SIZE + keySize )
		return false;

	key = ptr + PROTO_CHUNK_SEAL_DATA_SIZE;

	return true;
}

bool Protocol::parseChunkSealHeaderData( struct ChunkSealHeaderData &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseChunkSealHeaderData(
		offset,
		header.keySize,
		header.offset,
		header.key,
		buf, size
	);
}

//////////////////////
// Chunk operations //
//////////////////////
size_t Protocol::generateChunkHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_SIZE, id );

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( chunkId );

	bytes += PROTO_CHUNK_SIZE;

	return bytes;
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

size_t Protocol::generateChunkDataHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t chunkSize, char *chunkData ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_DATA_SIZE + chunkSize, id );

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( chunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( chunkSize );

	buf += PROTO_CHUNK_DATA_SIZE;

	memmove( buf, chunkData, chunkSize );
	bytes += PROTO_CHUNK_DATA_SIZE + chunkSize;

	return bytes;
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

////////////////////////////////////////////////////////////////////////////////

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
