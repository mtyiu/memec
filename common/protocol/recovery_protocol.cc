#include "protocol.hh"

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
