#include "protocol.hh"

size_t Protocol::generateChunkSealHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t count, uint32_t dataLength, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_SEAL_SIZE + dataLength, instanceId, requestId, sendBuf );
	bytes += ProtocolUtil::write4Bytes( buf, listId   );
	bytes += ProtocolUtil::write4Bytes( buf, stripeId );
	bytes += ProtocolUtil::write4Bytes( buf, chunkId  );
	bytes += ProtocolUtil::write4Bytes( buf, count    );
	bytes += dataLength;
	return bytes;
}

bool Protocol::parseChunkSealHeader( struct ChunkSealHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_CHUNK_SEAL_SIZE ) return false;
	char *ptr = buf + offset;
	header.listId   = ProtocolUtil::read4Bytes( ptr );
	header.stripeId = ProtocolUtil::read4Bytes( ptr );
	header.chunkId  = ProtocolUtil::read4Bytes( ptr );
	header.count    = ProtocolUtil::read4Bytes( ptr );
	return true;
}

bool Protocol::parseChunkSealHeaderData( struct ChunkSealHeaderData &header, char *buf, size_t size, size_t offset ) {
	// Note: Also implemented in server/buffer/parity_chunk_buffer.cc
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_CHUNK_SEAL_DATA_SIZE ) return false;
	char *ptr = buf + offset;
	header.keySize = ProtocolUtil::read1Byte ( ptr );
	header.offset  = ProtocolUtil::read4Bytes( ptr );
	header.key     = ptr;
	return ( size - offset >= ( size_t ) PROTO_CHUNK_SEAL_DATA_SIZE + header.keySize );
}
