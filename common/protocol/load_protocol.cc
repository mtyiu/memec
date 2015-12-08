#include "protocol.hh"

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
