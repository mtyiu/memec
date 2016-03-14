#include "protocol.hh"

size_t Protocol::generateChunkSealHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t count, uint32_t dataLength, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_SEAL_SIZE + dataLength, instanceId, requestId, sendBuf );

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
	// Note: Also implemented in server/buffer/parity_chunk_buffer.cc
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
