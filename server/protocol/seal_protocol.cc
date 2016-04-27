#include <cassert>
#include "protocol.hh"
#include "../../common/ds/chunk_pool.hh"

char *ServerProtocol::reqSealChunk( size_t &size, uint16_t instanceId, uint32_t requestId, Chunk *chunk, uint32_t startPos, char *buf ) {
	// -- common/protocol/seal_protocol.cc --
	if ( ! buf ) buf = this->buffer.send;

	char *ptr = buf + PROTO_HEADER_SIZE + PROTO_CHUNK_SEAL_SIZE;
	size_t bytes = 0; // data length only

	int currentOffset = startPos, nextOffset = 0;
	uint32_t count = 0;
	char *key;
	uint8_t keySize;
	while ( ( nextOffset = ChunkUtil::next( chunk, currentOffset, key, keySize ) ) != -1 ) {
		ptr[ 0 ] = keySize;
		*( ( uint32_t * )( ptr + 1 ) ) = htonl( currentOffset );
		memmove( ptr + 5, key, keySize );

		count++;
		bytes += PROTO_CHUNK_SEAL_DATA_SIZE + keySize;
		ptr += PROTO_CHUNK_SEAL_DATA_SIZE + keySize;

		// printf( "%.*s ", keySize, key );

		currentOffset = nextOffset;
	}
	// printf( "\n" );

	// The seal request should not exceed the size of the send buffer
	assert( bytes <= this->buffer.size );

	Metadata metadata = ChunkUtil::getMetadata( chunk );
	size = this->generateChunkSealHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_SEAL_CHUNK,
		instanceId, requestId,
		metadata.listId,
		metadata.stripeId,
		metadata.chunkId,
		count,
		bytes,
		buf
	);
	return buf;
}
