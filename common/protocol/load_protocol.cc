#include "protocol.hh"

size_t Protocol::generateLoadStatsHeader( uint8_t magic, uint8_t to, uint16_t instanceId, uint32_t requestId, uint32_t serverGetCount, uint32_t serverSetCount, uint32_t serverOverloadCount, uint32_t recordSize, uint32_t serverAddrSize ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, 0, PROTO_LOAD_STATS_SIZE + ( serverGetCount + serverSetCount ) * recordSize + ( serverOverloadCount * serverAddrSize ), instanceId, requestId );

	*( ( uint32_t * )( buf ) ) = htonl( serverGetCount );
	*( ( uint32_t * )( buf + sizeof( uint32_t ) ) ) = htonl( serverSetCount );
	*( ( uint32_t * )( buf + sizeof( uint32_t ) * 2 ) ) = htonl( serverOverloadCount );

	bytes += PROTO_LOAD_STATS_SIZE;

	return bytes;
}

bool Protocol::parseLoadStatsHeader( size_t offset, uint32_t &serverGetCount, uint32_t &serverSetCount, uint32_t &serverOverloadCount, char *buf, size_t size ) {
	if ( size - offset < PROTO_LOAD_STATS_SIZE )
		return false;

	serverGetCount = ntohl( *( ( uint32_t * )( buf + offset ) ) );
	serverSetCount = ntohl( *( ( uint32_t * )( buf + offset + sizeof( uint32_t ) ) ) );
	serverOverloadCount = ntohl( *( ( uint32_t * )( buf + offset + sizeof( uint32_t ) * 2 ) ) );

	return true;
}

bool Protocol::parseLoadStatsHeader( struct LoadStatsHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseLoadStatsHeader(
		offset,
		header.serverGetCount,
		header.serverSetCount,
		header.serverOverloadCount,
		buf, size
	);
}
