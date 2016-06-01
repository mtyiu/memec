#include "protocol.hh"

size_t Protocol::generatePromoteBackupServerHeader(
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
	bytes += ProtocolUtil::write4Bytes( buf, addr, false );
	bytes += ProtocolUtil::write2Bytes( buf, port, false );
	tmp = buf;
	buf   += PROTO_PROMOTE_BACKUP_SERVER_SIZE - 6;
	bytes += PROTO_PROMOTE_BACKUP_SERVER_SIZE - 6;
	for ( ; chunksIt != chunks.end(); chunksIt++ ) {
		const Metadata &metadata = *chunksIt;
		if ( this->buffer.size >= bytes + PROTO_METADATA_SIZE ) {
			bytes += ProtocolUtil::write4Bytes( buf, metadata.listId   );
			bytes += ProtocolUtil::write4Bytes( buf, metadata.stripeId );
			bytes += ProtocolUtil::write4Bytes( buf, metadata.chunkId  );
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
				bytes += ProtocolUtil::write1Byte( buf, key.size );
				bytes += ProtocolUtil::write( buf, key.data, key.size );
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

size_t Protocol::generatePromoteBackupServerHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t addr, uint16_t port, uint32_t numReconstructedChunks, uint32_t numReconstructedKeys ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_PROMOTE_BACKUP_SERVER_SIZE, instanceId, requestId );
	bytes += ProtocolUtil::write4Bytes( buf, addr, false );
	bytes += ProtocolUtil::write2Bytes( buf, port, false );
	bytes += ProtocolUtil::write4Bytes( buf, numReconstructedChunks );
	bytes += ProtocolUtil::write4Bytes( buf, numReconstructedKeys );
	return bytes;
}

bool Protocol::parsePromoteBackupServerHeader( struct PromoteBackupServerHeader &header, bool isRequest, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_PROMOTE_BACKUP_SERVER_SIZE ) return false;
	char *ptr = buf + offset;
	header.addr          = ProtocolUtil::read4Bytes( ptr, false );
	header.port          = ProtocolUtil::read2Bytes( ptr, false );
	header.chunkCount    = ProtocolUtil::read4Bytes( ptr );
	header.unsealedCount = ProtocolUtil::read4Bytes( ptr );
	if ( isRequest ) {
		header.metadata = ( uint32_t * ) ptr;
		for ( uint32_t i = 0; i < header.chunkCount; i++ ) {
			for ( uint32_t j = 0; j < 3; j++ )
				header.metadata[ i * 3 + j ] = ntohl( header.metadata[ i * 3 + j ] );
			ptr += PROTO_METADATA_SIZE;
		}
		header.keys = ptr;
	}
	return true;
}

size_t Protocol::generateReconstructionHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, std::unordered_set<uint32_t> &stripeIds, std::unordered_set<uint32_t>::iterator &it, uint32_t numChunks, bool &isCompleted ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = PROTO_HEADER_SIZE;
	uint32_t count = 0;
	uint32_t *numStripes;
	isCompleted = true;
	bytes += ProtocolUtil::write4Bytes( buf, listId  );
	bytes += ProtocolUtil::write4Bytes( buf, chunkId );
	numStripes = ( uint32_t * ) buf;
	buf   += PROTO_RECONSTRUCTION_SIZE - 8;
	bytes += PROTO_RECONSTRUCTION_SIZE - 8;
	for ( count = 0; count < numChunks && it != stripeIds.end(); count++, it++ ) {
		if ( this->buffer.size >= bytes + 4 && count < 3000 ) {
			bytes += ProtocolUtil::write4Bytes( buf, *it );
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
	bytes += ProtocolUtil::write4Bytes( buf, listId    );
	bytes += ProtocolUtil::write4Bytes( buf, chunkId   );
	bytes += ProtocolUtil::write4Bytes( buf, numChunks );
	return bytes;
}

bool Protocol::parseReconstructionHeader( struct ReconstructionHeader &header, bool isRequest, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_RECONSTRUCTION_SIZE ) return false;
	char *ptr = buf + offset;
	header.listId     = ProtocolUtil::read4Bytes( ptr );
	header.chunkId    = ProtocolUtil::read4Bytes( ptr );
	header.numStripes = ProtocolUtil::read4Bytes( ptr );
	if ( isRequest ) {
		header.stripeIds = ( uint32_t * ) ptr;
		if ( size - offset < ( size_t ) PROTO_RECONSTRUCTION_SIZE + header.numStripes * 4 ) return false;
		for ( uint32_t i = 0; i < header.numStripes; i++ )
			header.stripeIds[ i ] = ntohl( header.stripeIds[ i ] );
	} else {
		header.stripeIds = 0;
	}
	return true;
}
