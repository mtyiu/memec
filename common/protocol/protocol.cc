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
		case PROTO_OPCODE_RECONSTRUCTION:
		case PROTO_OPCODE_SYNC_META:
		case PROTO_OPCODE_RELEASE_DEGRADED_LOCKS:
		case PROTO_OPCODE_SLAVE_RECONSTRUCTED:
		case PROTO_OPCODE_BACKUP_SLAVE_PROMOTED:
		case PROTO_OPCODE_PARITY_MIGRATE:

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

		case PROTO_OPCODE_REMAPPING_SET:
		case PROTO_OPCODE_DEGRADED_LOCK:
		case PROTO_OPCODE_DEGRADED_UNLOCK:

		case PROTO_OPCODE_REMAPPING_LOCK:

		case PROTO_OPCODE_REMAPPING_UNLOCK:
		case PROTO_OPCODE_SEAL_CHUNK:
		case PROTO_OPCODE_UPDATE_CHUNK:
		case PROTO_OPCODE_DELETE_CHUNK:
		case PROTO_OPCODE_GET_CHUNK:
		case PROTO_OPCODE_GET_CHUNKS:
		case PROTO_OPCODE_SET_CHUNK:
		case PROTO_OPCODE_SET_CHUNK_UNSEALED:
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
size_t Protocol::generateAddressHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t addr, uint16_t port, char* buf ) {
	if ( ! buf ) buf = this->buffer.send;

	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_ADDRESS_SIZE, id, buf );

	buf += bytes;

	// Already in network-byte order
	*( ( uint32_t * )( buf     ) ) = addr;
	*( ( uint16_t * )( buf + 4 ) ) = port;

	bytes += PROTO_ADDRESS_SIZE;

	return bytes;
}

bool Protocol::parseAddressHeader( size_t offset, uint32_t &addr, uint16_t &port, char *buf, size_t size ) {
	if ( size - offset < PROTO_ADDRESS_SIZE )
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

size_t Protocol::generateSrcDstAddressHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t srcAddr, uint16_t srcPort, uint32_t dstAddr, uint16_t dstPort ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_ADDRESS_SIZE * 2, id );

	// Already in network-byte order
	*( ( uint32_t * )( buf     ) ) = srcAddr;
	*( ( uint16_t * )( buf + 4 ) ) = srcPort;
	*( ( uint32_t * )( buf + 6 ) ) = dstAddr;
	*( ( uint16_t * )( buf + 10 ) ) = dstPort;

	bytes += PROTO_ADDRESS_SIZE * 2;

	return bytes;
}

bool Protocol::parseSrcDstAddressHeader( struct AddressHeader &srcHeader, struct AddressHeader &dstHeader, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	bool ret = this->parseAddressHeader(
		offset,
		srcHeader.addr,
		srcHeader.port,
		buf, size
	);
	return ret ? this->parseAddressHeader(
		offset + PROTO_ADDRESS_SIZE,
		dstHeader.addr,
		dstHeader.port,
		buf, size
	) : false;
}

size_t Protocol::generatePromoteBackupSlaveHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
	uint32_t addr, uint16_t port,
	std::unordered_set<Metadata> &chunks,
	std::unordered_set<Metadata>::iterator &it,
	bool &isCompleted
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE, *tmp;
	size_t bytes = PROTO_HEADER_SIZE;
	uint32_t count = 0;

	isCompleted = true;

	// Already in network-byte order
	*( ( uint32_t * )( buf     ) ) = addr;
	*( ( uint16_t * )( buf + 4 ) ) = port;
	tmp = buf + 6;

	buf += PROTO_PROMOTE_BACKUP_SLAVE_SIZE;
	bytes += PROTO_PROMOTE_BACKUP_SLAVE_SIZE;

	for ( ; it != chunks.end(); it++ ) {
		const Metadata &metadata = *it;
		if ( this->buffer.size >= bytes + PROTO_METADATA_SIZE ) {
			*( ( uint32_t * )( buf     ) ) = htonl( metadata.listId );
			*( ( uint32_t * )( buf + 4 ) ) = htonl( metadata.stripeId );
			*( ( uint32_t * )( buf + 8 ) ) = htonl( metadata.chunkId );
			buf   += PROTO_METADATA_SIZE;
			bytes += PROTO_METADATA_SIZE;
			count++;
		} else {
			isCompleted = false;
			break;
		}
	}

	*( ( uint32_t * )( tmp ) ) = htonl( count );

	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, id );

	return bytes;
}

size_t Protocol::generatePromoteBackupSlaveHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t addr, uint16_t port, uint32_t numReconstructed ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_PROMOTE_BACKUP_SLAVE_SIZE, id );

	// Already in network-byte order
	*( ( uint32_t * )( buf     ) ) = addr;
	*( ( uint16_t * )( buf + 4 ) ) = port;
	*( ( uint32_t * )( buf + 6 ) ) = htonl( numReconstructed );

	bytes += PROTO_PROMOTE_BACKUP_SLAVE_SIZE;
	return bytes;
}

bool Protocol::parsePromoteBackupSlaveHeader( size_t offset, uint32_t &addr, uint16_t &port, uint32_t &count, uint32_t *&metadata, char *buf, size_t size ) {
	if ( size - offset < PROTO_PROMOTE_BACKUP_SLAVE_SIZE )
		return false;

	char *ptr = buf + offset;
	addr = *( ( uint32_t * )( ptr     ) );
	port = *( ( uint16_t * )( ptr + 4 ) );
	count = ntohl( *( ( uint32_t * )( ptr + 6 ) ) );
	metadata = ( uint32_t * )( ptr + 10 );

	for ( uint32_t i = 0; i < count; i++ ) {
		for ( uint32_t j = 0; j < 3; j++ ) {
			metadata[ i * 3 + j ] = ntohl( metadata[ i * 3 + j ] );
		}
	}

	return true;
}

bool Protocol::parsePromoteBackupSlaveHeader( size_t offset, uint32_t &addr, uint16_t &port, uint32_t &numReconstructed, char *buf, size_t size ) {
	if ( size - offset < PROTO_PROMOTE_BACKUP_SLAVE_SIZE )
		return false;

	char *ptr = buf + offset;
	addr = *( ( uint32_t * )( ptr     ) );
	port = *( ( uint16_t * )( ptr + 4 ) );
	numReconstructed = ntohl( *( ( uint32_t * )( ptr + 6 ) ) );

	return true;
}

bool Protocol::parsePromoteBackupSlaveHeader( struct PromoteBackupSlaveHeader &header, bool isRequest, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( isRequest ) {
		return this->parsePromoteBackupSlaveHeader(
			offset,
			header.addr,
			header.port,
			header.count,
			header.metadata,
			buf, size
		);
	} else {
		return this->parsePromoteBackupSlaveHeader(
			offset,
			header.addr,
			header.port,
			header.count,
			buf, size
		);
	}
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
	uint8_t *isLastPtr;
	std::unordered_set<Metadata>::iterator sealedIt;
	std::unordered_map<Key, OpMetadata>::iterator opsIt;

	sealedCount = 0;
	opsCount = 0;
	isCompleted = true;

	sealedPtr       = ( uint32_t * )( buf     );
	opsPtr          = ( uint32_t * )( buf + 4 );
	isLastPtr       = ( uint8_t * )( buf + 8 );

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
	*isLastPtr = isCompleted? 1 : 0;

	// TODO tell coordinator whether this is the last packet of records

	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, id );

	return bytes;
}

bool Protocol::parseHeartbeatHeader( size_t offset, uint32_t &sealed, uint32_t &keys, bool isLast, char *buf, size_t size ) {
	if ( size - offset < PROTO_HEARTBEAT_SIZE )
		return false;

	char *ptr = buf + offset;
	sealed = ntohl( *( ( uint32_t * )( ptr     ) ) );
	keys   = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	isLast = ( *( ( uint8_t * )( ptr + 8 ) ) == 0 )? false : true;

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
		header.isLast,
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

	if ( size - offset < PROTO_KEY_OP_METADATA_SIZE + ( size_t ) keySize )
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

size_t Protocol::generateRemappingRecordMessage( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, LOCK_T *lock, std::unordered_map<Key, RemappingRecord> &remapRecords, size_t &remapCount, char *buf ) {
	if ( ! buf ) buf = this->buffer.send;
	buf += PROTO_HEADER_SIZE;

	uint32_t bytes = 0;
	std::unordered_map<Key, RemappingRecord>::iterator rit;
	bytes = PROTO_HEADER_SIZE;
	remapCount = 0;

	// reserve the header and set the record count after copying data into buffer
	buf += PROTO_REMAPPING_RECORD_SIZE;
	bytes += PROTO_REMAPPING_RECORD_SIZE;

	// append remapping record
	if ( lock ) LOCK( lock );
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
	if ( lock ) UNLOCK( lock );

	// set the record count
	*( ( uint32_t * )( buf - bytes + PROTO_HEADER_SIZE ) ) = htonl( remapCount );

	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, id, buf - bytes );

	return bytes;
}

bool Protocol::parseRemappingRecordHeader( size_t offset, uint32_t &remap, char* buf, size_t size ) {
	if ( size - offset < PROTO_REMAPPING_RECORD_SIZE )
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
	if ( size - offset < PROTO_SLAVE_SYNC_REMAP_PER_SIZE )
		return false;

	char *ptr = buf + offset;
	keySize = ( uint8_t ) ptr[ 0 ];
	opcode = ( uint8_t ) ptr[ 1 ];
	listId = ntohl( *( ( uint32_t * )( ptr + 2 ) ) );
	chunkId = ntohl( *( ( uint32_t * )( ptr + 6 ) ) );

	if ( size - offset < PROTO_SLAVE_SYNC_REMAP_PER_SIZE + ( size_t ) keySize )
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
	if ( size - offset < PROTO_LOAD_STATS_SIZE )
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
	if ( size - offset < PROTO_CHUNK_KEY_SIZE )
		return false;

	char *ptr = buf + offset;

	listId   = ntohl( *( ( uint32_t * )( ptr     ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );
	keySize = ( uint8_t ) ptr[ 12 ];

	if ( size - offset < ( size_t ) PROTO_CHUNK_KEY_SIZE + keySize )
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
	if ( size - offset < PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE )
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
	if ( size - offset < PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE )
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

	if ( size - offset < PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize )
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

bool Protocol::parseKeyValueUpdateHeader( size_t offset, uint8_t &keySize, char *&key, uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize, char *&valueUpdate, char *buf, size_t size ) {
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

	if ( size - offset < PROTO_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize )
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
	if ( size - offset < PROTO_CHUNK_UPDATE_SIZE )
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
	if ( size - offset < PROTO_CHUNK_UPDATE_SIZE )
		return false;

	char *ptr = buf + offset;
	listId          = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId        = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	chunkId         = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	updateOffset    = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	updateLength    = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );
	updatingChunkId = ntohl( *( ( uint32_t * )( ptr + 20 ) ) );

	if ( size - offset < PROTO_CHUNK_UPDATE_SIZE + updateLength )
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
size_t Protocol::generateRemappingLockHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t chunkId, bool isRemapped, uint8_t keySize, char *key, uint32_t sockfd, uint32_t payload ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_REMAPPING_LOCK_SIZE + keySize + payload, id );

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( chunkId );
	*( ( uint32_t * )( buf + 8 ) ) = htonl( sockfd );
	buf[ 12 ] = isRemapped ? 1 : 0;
	bytes += 13;
	buf += 13;

	buf[ 0 ] = keySize;
	bytes++;
	buf++;

	memmove( buf, key, keySize );
	bytes += keySize;

	return bytes;
}

bool Protocol::parseRemappingLockHeader( size_t offset, uint32_t &listId, uint32_t &chunkId, bool &isRemapped, uint8_t &keySize, char *&key, char *buf, size_t size, uint32_t &sockfd ) {
	if ( size - offset < PROTO_REMAPPING_LOCK_SIZE )
		return false;

	char *ptr = buf + offset;
	listId  = ntohl( *( ( uint32_t * )( ptr      ) ) );
	chunkId = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	sockfd = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	isRemapped = ( ptr[ 12 ] != 0 );
	keySize = *( ptr + 13 );

	if ( size - offset < ( size_t ) PROTO_REMAPPING_LOCK_SIZE + keySize )
		return false;

	key = ptr + PROTO_REMAPPING_LOCK_SIZE;

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
		header.isRemapped,
		header.keySize,
		header.key,
		buf, size,
		header.sockfd
	);
}

size_t Protocol::generateRemappingSetHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key, uint32_t valueSize, char *value, char *sendBuf, uint32_t sockfd, bool isParity, struct sockaddr_in *target ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	uint32_t payload = PROTO_REMAPPING_SET_SIZE + keySize + valueSize;
	if ( target ) payload += 6; // ip + port
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, payload , id, sendBuf );

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( chunkId );
	*( ( uint32_t * )( buf + 8 ) ) = htonl( sockfd );
	buf[ 12 ] = isParity;
	bytes += 13;
	buf += 13;

	unsigned char *tmp;
	valueSize = htonl( valueSize );
	tmp = ( unsigned char * ) &valueSize;
	buf[ 0 ] = keySize;
	buf[ 1 ] = tmp[ 1 ];
	buf[ 2 ] = tmp[ 2 ];
	buf[ 3 ] = tmp[ 3 ];
	bytes += 4;
	buf += 4;
	valueSize = ntohl( valueSize );

	memmove( buf, key, keySize );
	bytes += keySize;
	buf += keySize;

	memmove( buf, value, valueSize );
	bytes += valueSize;
	buf += valueSize;

	if ( target ) {
		*( ( uint32_t * )( buf     ) ) = target->sin_addr.s_addr;
		*( ( uint16_t * )( buf + 4 ) ) = target->sin_port;
		bytes += 6;
	}

	return bytes;
}

bool Protocol::parseRemappingSetHeader( size_t offset, uint32_t &listId, uint32_t &chunkId, uint8_t &keySize, char *&key, uint32_t &valueSize, char *&value, char *buf, size_t size, uint32_t &sockfd, bool &isParity, struct sockaddr_in *target ) {
	if ( size - offset < PROTO_REMAPPING_SET_SIZE )
		return false;

	char *ptr = buf + offset;
	unsigned char *tmp;
	listId  = ntohl( *( ( uint32_t * )( ptr      ) ) );
	chunkId = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	sockfd = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	isParity = *( ptr + 12 );
	keySize = *( ptr + 13 );
	ptr += 14;

	valueSize = 0;
	tmp = ( unsigned char * ) &valueSize;
	tmp[ 1 ] = ptr[ 0 ];
	tmp[ 2 ] = ptr[ 1 ];
	tmp[ 3 ] = ptr[ 2 ];
	valueSize = ntohl( valueSize );
	ptr += 3;

	if ( size - offset < PROTO_REMAPPING_SET_SIZE + keySize + valueSize )
		return false;

	key = ptr;
	value = ptr + keySize;

	ptr += keySize + valueSize;

	if ( size - offset >= PROTO_REMAPPING_SET_SIZE + keySize + valueSize + 6 && target != 0 ) {
		target->sin_addr.s_addr = *( uint32_t * )( ptr );
		target->sin_port = *( uint16_t * )( ptr + 4 );
	}
	return true;
}

bool Protocol::parseRemappingSetHeader( struct RemappingSetHeader &header, char *buf, size_t size, size_t offset, struct sockaddr_in *target ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseRemappingSetHeader(
		offset,
		header.listId,
		header.chunkId,
		header.keySize,
		header.key,
		header.valueSize,
		header.value,
		buf, size,
		header.sockfd,
		header.remapped,
		target
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

////////////////////////
// Degraded operation //
////////////////////////
size_t Protocol::generateDegradedLockReqHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId, uint8_t keySize, char *key ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_DEGRADED_LOCK_REQ_SIZE + keySize, id );

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( srcDataChunkId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( dstDataChunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( srcParityChunkId );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( dstParityChunkId );
	buf[ 20 ] = keySize;

	buf += PROTO_DEGRADED_LOCK_REQ_SIZE;
	memmove( buf, key, keySize );

	bytes += PROTO_DEGRADED_LOCK_REQ_SIZE + keySize;

	return bytes;
}

bool Protocol::parseDegradedLockReqHeader( size_t offset, uint32_t &listId, uint32_t &srcDataChunkId, uint32_t &dstDataChunkId, uint32_t &srcParityChunkId, uint32_t &dstParityChunkId, uint8_t &keySize, char *&key, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_LOCK_REQ_SIZE )
		return false;

	char *ptr = buf + offset;
	listId           = ntohl( *( ( uint32_t * )( ptr      ) ) );
	srcDataChunkId   = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	dstDataChunkId   = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	srcParityChunkId = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	dstParityChunkId = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );
	keySize = ptr[ 20 ];

	if ( size - offset < PROTO_DEGRADED_LOCK_REQ_SIZE + ( size_t ) keySize )
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
		header.listId,
		header.srcDataChunkId,
		header.dstDataChunkId,
		header.srcParityChunkId,
		header.dstParityChunkId,
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
		case PROTO_DEGRADED_LOCK_RES_NOT_LOCKED:
		case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
		default:
			length = PROTO_DEGRADED_LOCK_RES_NOT_SIZE;
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

size_t Protocol::generateDegradedLockResHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, bool isLocked, bool isSealed, uint8_t keySize, char *key, uint32_t listId, uint32_t stripeId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId ) {
	char *buf;
	size_t bytes = this->generateDegradedLockResHeader(
		magic, to, opcode, id,
		isLocked ? PROTO_DEGRADED_LOCK_RES_IS_LOCKED : PROTO_DEGRADED_LOCK_RES_WAS_LOCKED,
		keySize, key, buf
	);

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( srcDataChunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( dstDataChunkId );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( srcParityChunkId );
	*( ( uint32_t * )( buf + 20 ) ) = htonl( dstParityChunkId );
	buf[ 24 ] = isSealed;

	bytes += PROTO_DEGRADED_LOCK_RES_LOCK_SIZE;

	return bytes;
}

size_t Protocol::generateDegradedLockResHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, bool exist, uint8_t keySize, char *key, uint32_t listId, uint32_t srcDataChunkId, uint32_t srcParityChunkId ) {
	char *buf;
	size_t bytes = this->generateDegradedLockResHeader(
		magic, to, opcode, id,
		exist ? PROTO_DEGRADED_LOCK_RES_NOT_LOCKED : PROTO_DEGRADED_LOCK_RES_NOT_EXIST,
		keySize, key, buf
	);

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( srcDataChunkId );
	*( ( uint32_t * )( buf + 8 ) ) = htonl( srcParityChunkId );

	bytes += PROTO_DEGRADED_LOCK_RES_NOT_SIZE;

	return bytes;
}

size_t Protocol::generateDegradedLockResHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint8_t keySize, char *key, uint32_t listId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId ) {
	char *buf;
	size_t bytes = this->generateDegradedLockResHeader(
		magic, to, opcode, id,
		PROTO_DEGRADED_LOCK_RES_REMAPPED,
		keySize, key, buf
	);

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( srcDataChunkId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( dstDataChunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( srcParityChunkId );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( dstParityChunkId );

	bytes += PROTO_DEGRADED_LOCK_RES_REMAP_SIZE;

	return bytes;
}

bool Protocol::parseDegradedLockResHeader( size_t offset, uint8_t &type, uint8_t &keySize, char *&key, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_LOCK_RES_BASE_SIZE )
		return false;

	char *ptr = buf + offset;
	type = ptr[ 0 ];
	keySize = ptr[ 1 ];

	if ( size - offset < PROTO_DEGRADED_LOCK_RES_BASE_SIZE + ( size_t ) keySize )
		return false;

	key = ptr + PROTO_DEGRADED_LOCK_RES_BASE_SIZE;

	return true;
}

bool Protocol::parseDegradedLockResHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &srcDataChunkId, uint32_t &dstDataChunkId, uint32_t &srcParityChunkId, uint32_t &dstParityChunkId, bool &isSealed, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_LOCK_RES_LOCK_SIZE )
		return false;

	char *ptr = buf + offset;
	listId           = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId         = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	srcDataChunkId   = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	dstDataChunkId   = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	srcParityChunkId = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );
	dstParityChunkId = ntohl( *( ( uint32_t * )( ptr + 20 ) ) );
	isSealed = ptr[ 24 ];

	return true;
}

bool Protocol::parseDegradedLockResHeader( size_t offset, uint32_t &listId, uint32_t &srcDataChunkId, uint32_t &srcParityChunkId, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_LOCK_RES_NOT_SIZE )
		return false;

	char *ptr = buf + offset;
	listId           = ntohl( *( ( uint32_t * )( ptr     ) ) );
	srcDataChunkId   = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	srcParityChunkId = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );

	return true;
}

bool Protocol::parseDegradedLockResHeader( size_t offset, uint32_t &listId, uint32_t &srcDataChunkId, uint32_t &dstDataChunkId, uint32_t &srcParityChunkId, uint32_t &dstParityChunkId, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_LOCK_RES_REMAP_SIZE )
		return false;

	char *ptr = buf + offset;
	listId           = ntohl( *( ( uint32_t * )( ptr      ) ) );
	srcDataChunkId   = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	dstDataChunkId   = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	srcParityChunkId = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	dstParityChunkId = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );

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
				header.listId,
				header.stripeId,
				header.srcDataChunkId,
				header.dstDataChunkId,
				header.srcParityChunkId,
				header.dstParityChunkId,
				header.isSealed,
				buf, size
			);
			break;
		case PROTO_DEGRADED_LOCK_RES_NOT_LOCKED:
		case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
			ret = this->parseDegradedLockResHeader(
				offset,
				header.listId,
				header.srcDataChunkId,
				header.srcParityChunkId,
				buf, size
			);
			break;
		case PROTO_DEGRADED_LOCK_RES_REMAPPED:
			ret = this->parseDegradedLockResHeader(
				offset,
				header.listId,
				header.srcDataChunkId,
				header.dstDataChunkId,
				header.srcParityChunkId,
				header.dstParityChunkId,
				buf, size
			);
			break;
		default:
			return false;
	}
	return ret;
}

size_t Protocol::generateDegradedReqHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId, bool isSealed, uint8_t keySize, char *key ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_REQ_BASE_SIZE + PROTO_KEY_SIZE + keySize,
		id
	);

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( srcDataChunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( dstDataChunkId );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( srcParityChunkId );
	*( ( uint32_t * )( buf + 20 ) ) = htonl( dstParityChunkId );
	buf[ 24 ] = isSealed;
	buf += PROTO_DEGRADED_REQ_BASE_SIZE;

	buf[ 0 ] = keySize;

	buf += PROTO_KEY_SIZE;
	memmove( buf, key, keySize );

	bytes += PROTO_DEGRADED_REQ_BASE_SIZE + PROTO_KEY_SIZE + keySize;

	return bytes;
}

size_t Protocol::generateDegradedReqHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId, bool isSealed, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_REQ_BASE_SIZE + PROTO_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize,
		id
	);

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( srcDataChunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( dstDataChunkId );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( srcParityChunkId );
	*( ( uint32_t * )( buf + 20 ) ) = htonl( dstParityChunkId );
	buf[ 24 ] = isSealed;
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

bool Protocol::parseDegradedReqHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &srcDataChunkId, uint32_t &dstDataChunkId, uint32_t &srcParityChunkId, uint32_t &dstParityChunkId, bool &isSealed, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_REQ_BASE_SIZE )
		return false;

	char *ptr = buf + offset;
	listId           = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId         = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	srcDataChunkId   = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	dstDataChunkId   = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	srcParityChunkId = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );
	dstParityChunkId = ntohl( *( ( uint32_t * )( ptr + 20 ) ) );
	isSealed = ptr[ 24 ];

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
		header.srcDataChunkId,
		header.dstDataChunkId,
		header.srcParityChunkId,
		header.dstParityChunkId,
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

size_t Protocol::generateListStripeKeyHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_LIST_STRIPE_KEY_SIZE + keySize, id );

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( chunkId );
	buf[ 8 ] = keySize;

	buf += PROTO_LIST_STRIPE_KEY_SIZE;
	memmove( buf, key, keySize );

	bytes += PROTO_LIST_STRIPE_KEY_SIZE + keySize;

	return bytes;
}

bool Protocol::parseListStripeKeyHeader( size_t offset, uint32_t &listId, uint32_t &chunkId, uint8_t &keySize, char *&key, char *buf, size_t size ) {
	if ( size - offset < PROTO_LIST_STRIPE_KEY_SIZE )
		return false;

	char *ptr = buf + offset;
	listId  = ntohl( *( ( uint32_t * )( ptr     ) ) );
	chunkId = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	keySize = ptr[ 8 ];

	if ( size - offset < ( size_t ) PROTO_LIST_STRIPE_KEY_SIZE + keySize )
		return false;

	key = ptr + PROTO_LIST_STRIPE_KEY_SIZE;

	return true;
}

bool Protocol::parseListStripeKeyHeader( struct ListStripeKeyHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseListStripeKeyHeader(
		offset,
		header.listId,
		header.chunkId,
		header.keySize,
		header.key,
		buf, size
	);
}

size_t Protocol::generateDegradedReleaseReqHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, std::vector<Metadata> &chunks, bool &isCompleted ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = 0;
	uint32_t count = 0;

	isCompleted = true;

	for ( size_t i = 0, len = chunks.size(); i < len; i++ ) {
		if ( this->buffer.size >= bytes + PROTO_DEGRADED_RELEASE_REQ_SIZE ) {
			*( ( uint32_t * )( buf      ) ) = htonl( chunks[ i ].listId );
			*( ( uint32_t * )( buf +  4 ) ) = htonl( chunks[ i ].stripeId );
			*( ( uint32_t * )( buf +  8 ) ) = htonl( chunks[ i ].chunkId );

			count++;
		} else {
			isCompleted = false;
			break;
		}

		buf += PROTO_DEGRADED_RELEASE_REQ_SIZE;
		bytes += PROTO_DEGRADED_RELEASE_REQ_SIZE;
	}

	bytes += this->generateHeader( magic, to, opcode, bytes, id );

	return bytes;
}

size_t Protocol::generateDegradedReleaseResHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t count ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_DEGRADED_RELEASE_RES_SIZE, id );

	*( ( uint32_t * )( buf ) ) = htonl( count );

	bytes += PROTO_DEGRADED_RELEASE_RES_SIZE;

	return bytes;
}

bool Protocol::parseDegradedReleaseResHeader( size_t offset, uint32_t &count, char* buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_RELEASE_RES_SIZE )
		return false;

	char *ptr = buf + offset;
	count = ntohl( *( ( uint32_t * )( ptr ) ) );

	return true;
}

bool Protocol::parseDegradedReleaseResHeader( struct DegradedReleaseResHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseDegradedReleaseResHeader(
		offset,
		header.count,
		buf, size
	);
}

////////////////////
// Reconstruction //
////////////////////
size_t Protocol::generateReconstructionHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t chunkId, std::unordered_set<uint32_t> &stripeIds, std::unordered_set<uint32_t>::iterator &it, uint32_t numChunks, bool &isCompleted ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes;
	uint32_t count = 0;
	uint32_t *tmp, *numStripes;

	isCompleted = true;

	bytes = PROTO_HEADER_SIZE;

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( chunkId );
	numStripes = ( uint32_t * )( buf + 8 );

	buf += PROTO_RECONSTRUCTION_SIZE;
	bytes += PROTO_RECONSTRUCTION_SIZE;

	tmp = ( uint32_t * ) buf;
	for ( count = 0; count < numChunks && it != stripeIds.end(); count++, it++ ) {
		if ( this->buffer.size >= bytes + 4 && count < 3000 ) {
			tmp[ count ] = htonl( *it );
			bytes += 4;
		} else {
			isCompleted = false;
			break;
		}
	}

	*numStripes = htonl( count );

	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, id, this->buffer.send );

	return bytes;
}

size_t Protocol::generateReconstructionHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t chunkId, uint32_t numChunks ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_RECONSTRUCTION_SIZE, id, this->buffer.send );

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( chunkId );
	*( ( uint32_t * )( buf + 8 ) ) = htonl( numChunks );
	bytes += PROTO_RECONSTRUCTION_SIZE;

	return bytes;
}

bool Protocol::parseReconstructionHeader( size_t offset, uint32_t &listId, uint32_t &chunkId, uint32_t &numStripes, uint32_t *&stripeIds, char *buf, size_t size ) {
	if ( size - offset < PROTO_RECONSTRUCTION_SIZE )
		return false;

	char *ptr = buf + offset;
	listId     = ntohl( *( ( uint32_t * )( ptr     ) ) );
	chunkId    = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	numStripes = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );
	stripeIds = ( uint32_t * )( ptr + PROTO_RECONSTRUCTION_SIZE );

	if ( size - offset < ( size_t ) PROTO_RECONSTRUCTION_SIZE + numStripes * 4 )
		return false;

	for ( uint32_t i = 0; i < numStripes; i++ ) {
		stripeIds[ i ] = ntohl( stripeIds[ i ] );
	}

	return true;
}

bool Protocol::parseReconstructionHeader( size_t offset, uint32_t &listId, uint32_t &chunkId, uint32_t &numStripes, char *buf, size_t size ) {
	if ( size - offset < PROTO_RECONSTRUCTION_SIZE )
		return false;

	char *ptr = buf + offset;
	listId     = ntohl( *( ( uint32_t * )( ptr     ) ) );
	chunkId    = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	numStripes = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );

	return true;
}

bool Protocol::parseReconstructionHeader( struct ReconstructionHeader &header, bool isRequest, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( isRequest ) {
		return this->parseReconstructionHeader(
			offset,
			header.listId,
			header.chunkId,
			header.numStripes,
			header.stripeIds,
			buf, size
		);
	} else {
		header.stripeIds = 0;
		return this->parseReconstructionHeader(
			offset,
			header.listId,
			header.chunkId,
			header.numStripes,
			buf, size
		);
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
	if ( size - offset < PROTO_CHUNK_SEAL_SIZE )
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
	if ( size - offset < PROTO_CHUNK_SEAL_DATA_SIZE )
		return false;

	char *ptr = buf + offset;
	keySize = ptr[ 0 ];
	offset  = ntohl( *( ( uint32_t * )( ptr + 1 ) ) );

	if ( size - offset < ( size_t ) PROTO_CHUNK_SEAL_DATA_SIZE + keySize )
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
size_t Protocol::generateChunkHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_SIZE, id, sendBuf );

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( chunkId );

	bytes += PROTO_CHUNK_SIZE;

	return bytes;
}

bool Protocol::parseChunkHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, char *buf, size_t size ) {
	if ( size - offset < PROTO_CHUNK_SIZE )
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

size_t Protocol::generateChunksHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t chunkId, std::vector<uint32_t> &stripeIds, uint32_t &count ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = PROTO_HEADER_SIZE;
	uint32_t *tmp;

	count = 0;

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( chunkId );
	tmp = ( uint32_t * )( buf + 8 );

	buf += PROTO_CHUNKS_SIZE;
	bytes += PROTO_CHUNKS_SIZE;

	for ( size_t i = 0, len = stripeIds.size(); i < len; i++ ) {
		if ( bytes + 4 > this->buffer.size ) // no more space
			break;
		*( ( uint32_t * )( buf ) ) = htonl( stripeIds[ i ] );
		buf += 4;
		bytes += 4;
		count++;
	}

	*tmp = htonl( count );
	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, id );

	return bytes;
}

bool Protocol::parseChunksHeader( size_t offset, uint32_t &listId, uint32_t &chunkId, uint32_t &numStripes, uint32_t *&stripeIds, char *buf, size_t size ) {
	if ( size - offset < PROTO_CHUNKS_SIZE )
		return false;

	char *ptr = buf + offset;
	listId     = ntohl( *( ( uint32_t * )( ptr     ) ) );
	chunkId    = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	numStripes = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );

	ptr += PROTO_CHUNKS_SIZE;

	if ( size - offset < PROTO_CHUNKS_SIZE + numStripes * 4 )
		return false;

	stripeIds = ( uint32_t * ) ptr;

	for ( uint32_t i = 0; i < numStripes; i++ )
		stripeIds[ i ] = ntohl( stripeIds[ i ] );

	return true;
}

bool Protocol::parseChunksHeader( struct ChunksHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseChunksHeader(
		offset,
		header.listId,
		header.chunkId,
		header.numStripes,
		header.stripeIds,
		buf, size
	);
}

size_t Protocol::generateChunkDataHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t chunkSize, uint32_t chunkOffset, char *chunkData ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_DATA_SIZE + chunkSize, id );

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( chunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( chunkSize );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( chunkOffset );

	buf += PROTO_CHUNK_DATA_SIZE;

	if ( chunkSize && chunkData )
		memmove( buf, chunkData, chunkSize );
	bytes += PROTO_CHUNK_DATA_SIZE + chunkSize;

	return bytes;
}

bool Protocol::parseChunkDataHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint32_t &chunkSize, uint32_t &chunkOffset, char *&chunkData, char *buf, size_t size ) {
	if ( size - offset < PROTO_CHUNK_DATA_SIZE )
		return false;

	char *ptr = buf + offset;
	listId      = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId    = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	chunkId     = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	chunkSize   = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	chunkOffset = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );

	if ( size - offset < PROTO_CHUNK_DATA_SIZE + chunkSize )
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
		header.offset,
		header.data,
		buf, size
	);
}

size_t Protocol::generateChunkKeyValueHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
	uint32_t listId, uint32_t stripeId, uint32_t chunkId,
	std::unordered_map<Key, KeyValue> *values,
	std::unordered_multimap<Metadata, Key> *metadataRev,
	std::unordered_set<Key> *deleted,
	LOCK_T *lock,
	bool &isCompleted
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE + PROTO_CHUNK_KEY_VALUE_SIZE;
	size_t bytes = PROTO_HEADER_SIZE + PROTO_CHUNK_KEY_VALUE_SIZE;
	uint32_t numValues = 0, numDeleted = 0;

	Metadata metadata;
	Key key;
	KeyValue keyValue;
	uint8_t keySize;
	uint32_t valueSize;
	char *keyStr, *valueStr;
	unsigned char *tmp;

	metadata.set( listId, stripeId, chunkId );
	isCompleted = true;

	LOCK( lock );
	std::pair<
		std::unordered_multimap<Metadata, Key>::iterator,
		std::unordered_multimap<Metadata, Key>::iterator
	> mp;
	std::unordered_multimap<Metadata, Key>::iterator current, it;
	std::unordered_map<Key, KeyValue>::iterator keyValueIt;
	std::unordered_set<Key>::iterator deletedIt;

	// Deleted keys
	mp = metadataRev->equal_range( metadata );
	for ( it = mp.first; it != mp.second; ) {
		key = it->second;
		current = it;
		it++;

		deletedIt = deleted->find( key );
		if ( deletedIt != deleted->end() ) {
			key = *deletedIt;

			if ( this->buffer.size >= bytes + PROTO_KEY_SIZE + key.size ) {
				buf[ 0 ] = key.size;
				memmove( buf + 1, key.data, key.size );

				buf += PROTO_KEY_SIZE + key.size;
				bytes += PROTO_KEY_SIZE + key.size;

				deleted->erase( deletedIt );
				metadataRev->erase( current );

				key.free();

				numDeleted++;
			} else {
				isCompleted = false;
				break;
			}
		}
	}

	// Updated key-value pairs
	mp = metadataRev->equal_range( metadata );
	for ( it = mp.first; it != mp.second; ) {
		key = it->second;
		current = it;
		it++;

		keyValueIt = values->find( key );
		if ( keyValueIt != values->end() ) {
			keyValue = keyValueIt->second;
			keyValue.deserialize( keyStr, keySize, valueStr, valueSize );

			if ( this->buffer.size >= bytes + PROTO_KEY_VALUE_SIZE + keySize + valueSize ) {
				buf[ 0 ] = key.size;

				valueSize = htonl( valueSize );
				tmp = ( unsigned char * ) &valueSize;
				buf[ 1 ] = tmp[ 1 ];
				buf[ 2 ] = tmp[ 2 ];
				buf[ 3 ] = tmp[ 3 ];
				valueSize = ntohl( valueSize );

				memmove( buf + 4, keyStr, keySize );
				memmove( buf + 4 + keySize, valueStr, valueSize );

				buf += PROTO_KEY_VALUE_SIZE + keySize + valueSize;
				bytes += PROTO_KEY_VALUE_SIZE + keySize + valueSize;

				values->erase( keyValueIt );
				metadataRev->erase( current );

				keyValue.free();

				numValues++;
			} else {
				isCompleted = false;
				break;
			}
		}
	}

	UNLOCK( lock );

	buf = this->buffer.send + PROTO_HEADER_SIZE;

	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, id );

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( chunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( numDeleted );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( numValues );
	buf[ 20 ] = isCompleted;

	return bytes;
}

bool Protocol::parseChunkKeyValueHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint32_t &deleted, uint32_t &count, bool &isCompleted, char *&dataPtr, char *buf, size_t size ) {
	if ( size - offset < PROTO_CHUNK_KEY_VALUE_SIZE )
		return false;

	char *ptr = buf + offset;
	listId   = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	deleted  = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	count    = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );
	isCompleted = ptr[ 20 ];

	dataPtr = ptr + PROTO_CHUNK_KEY_VALUE_SIZE;

	return true;
}

bool Protocol::parseChunkKeyValueHeader( struct ChunkKeyValueHeader &header, char *&ptr, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseChunkKeyValueHeader(
		offset,
		header.listId,
		header.stripeId,
		header.chunkId,
		header.deleted,
		header.count,
		header.isCompleted,
		ptr,
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
