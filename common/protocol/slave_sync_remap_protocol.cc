#include "protocol.hh"

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
