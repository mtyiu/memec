#include "protocol.hh"

size_t Protocol::generatePromoteBackupSlaveHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	uint32_t addr, uint16_t port,
	std::unordered_set<Metadata> &chunks,
	std::unordered_set<Metadata>::iterator &chunksIt,
	std::unordered_set<Key> &keys,
	std::unordered_set<Key>::iterator &keysIt,
	bool &isCompleted
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE, *tmp;
	size_t bytes = PROTO_HEADER_SIZE;
	uint32_t chunkCount = 0, unsealedCount = 0;

	isCompleted = true;

	// Already in network-byte order
	*( ( uint32_t * )( buf     ) ) = addr;
	*( ( uint16_t * )( buf + 4 ) ) = port;
	tmp = buf + 6;

	buf += PROTO_PROMOTE_BACKUP_SLAVE_SIZE;
	bytes += PROTO_PROMOTE_BACKUP_SLAVE_SIZE;

	for ( ; chunksIt != chunks.end(); chunksIt++ ) {
		const Metadata &metadata = *chunksIt;
		if ( this->buffer.size >= bytes + PROTO_METADATA_SIZE ) {
			*( ( uint32_t * )( buf     ) ) = htonl( metadata.listId );
			*( ( uint32_t * )( buf + 4 ) ) = htonl( metadata.stripeId );
			*( ( uint32_t * )( buf + 8 ) ) = htonl( metadata.chunkId );
			buf   += PROTO_METADATA_SIZE;
			bytes += PROTO_METADATA_SIZE;
			chunkCount++;
		} else {
			isCompleted = false;
			break;
		}
	}

	if ( isCompleted ) {
		for ( ; keysIt != keys.end(); keysIt++ ) {
			const Key &key = *keysIt;
			if ( this->buffer.size >= bytes + PROTO_KEY_SIZE + key.size ) {
				buf[ 0 ] = key.size;
				memcpy( buf + 1, key.data, key.size );

				buf   += PROTO_KEY_SIZE + key.size;
				bytes += PROTO_KEY_SIZE + key.size;
				unsealedCount++;
			} else {
				isCompleted = false;
				break;
			}
		}
	}

	*( ( uint32_t * )( tmp ) ) = htonl( chunkCount );
	*( ( uint32_t * )( tmp + 4 ) ) = htonl( unsealedCount );

	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, instanceId, requestId );

	return bytes;
}

size_t Protocol::generatePromoteBackupSlaveHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t addr, uint16_t port, uint32_t numReconstructedChunks, uint32_t numReconstructedKeys ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_PROMOTE_BACKUP_SLAVE_SIZE, instanceId, requestId );

	// Already in network-byte order
	*( ( uint32_t * )( buf     ) ) = addr;
	*( ( uint16_t * )( buf + 4 ) ) = port;
	*( ( uint32_t * )( buf + 6 ) ) = htonl( numReconstructedChunks );
	*( ( uint32_t * )( buf + 10 ) ) = htonl( numReconstructedKeys );

	bytes += PROTO_PROMOTE_BACKUP_SLAVE_SIZE;
	return bytes;
}

bool Protocol::parsePromoteBackupSlaveHeader( size_t offset, uint32_t &addr, uint16_t &port, uint32_t &chunkCount, uint32_t &unsealedCount, uint32_t *&metadata, char *&keys, char *buf, size_t size ) {
	if ( size - offset < PROTO_PROMOTE_BACKUP_SLAVE_SIZE )
		return false;

	char *ptr = buf + offset;
	addr = *( ( uint32_t * )( ptr     ) );
	port = *( ( uint16_t * )( ptr + 4 ) );
	chunkCount = ntohl( *( ( uint32_t * )( ptr + 6 ) ) );
	unsealedCount = ntohl( *( ( uint32_t * )( ptr + 10 ) ) );

	ptr += PROTO_PROMOTE_BACKUP_SLAVE_SIZE;

	metadata = ( uint32_t * )( ptr );
	for ( uint32_t i = 0; i < chunkCount; i++ ) {
		for ( uint32_t j = 0; j < 3; j++ ) {
			metadata[ i * 3 + j ] = ntohl( metadata[ i * 3 + j ] );
		}
		ptr += PROTO_METADATA_SIZE;
	}

	keys = ptr;

	return true;
}

bool Protocol::parsePromoteBackupSlaveHeader( size_t offset, uint32_t &addr, uint16_t &port, uint32_t &numReconstructedChunks, uint32_t &numReconstructedKeys, char *buf, size_t size ) {
	if ( size - offset < PROTO_PROMOTE_BACKUP_SLAVE_SIZE )
		return false;

	char *ptr = buf + offset;
	addr = *( ( uint32_t * )( ptr     ) );
	port = *( ( uint16_t * )( ptr + 4 ) );
	numReconstructedChunks = ntohl( *( ( uint32_t * )( ptr + 6 ) ) );
	numReconstructedKeys   = ntohl( *( ( uint32_t * )( ptr + 10 ) ) );

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
			header.chunkCount,
			header.unsealedCount,
			header.metadata,
			header.keys,
			buf, size
		);
	} else {
		return this->parsePromoteBackupSlaveHeader(
			offset,
			header.addr,
			header.port,
			header.chunkCount,
			header.unsealedCount,
			buf, size
		);
	}
}

size_t Protocol::generateReconstructionHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, std::unordered_set<uint32_t> &stripeIds, std::unordered_set<uint32_t>::iterator &it, uint32_t numChunks, bool &isCompleted ) {
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

	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, instanceId, requestId, this->buffer.send );

	return bytes;
}

size_t Protocol::generateReconstructionHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, uint32_t numChunks ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_RECONSTRUCTION_SIZE, instanceId, requestId, this->buffer.send );

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
