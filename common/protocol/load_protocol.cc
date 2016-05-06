#include "protocol.hh"

// ---------------------------- Load ----------------------------------

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

// ---------------------------- Hotness ----------------------------------

size_t Protocol::generateHotnessStatsHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t getCount, uint32_t updateCount, uint32_t metadataSize ) {
	char *buf = this->buffer.send;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_HOTNESS_STATS_SIZE + ( getCount + updateCount ) * metadataSize, instanceId, requestId, buf, timestamp );

	buf += bytes;

	*( ( uint32_t * )( buf ) ) = htonl( getCount );
	*( ( uint32_t * )( buf + sizeof( uint32_t ) ) ) = htonl( updateCount );

	bytes += PROTO_HOTNESS_STATS_SIZE;

	return bytes;
}

bool Protocol::parseHotnessStatsHeader( size_t offset, uint32_t &getCount, uint32_t &updateCount, char *buf, size_t size ) {
	if ( size - offset < PROTO_HOTNESS_STATS_SIZE )
		return false;

	getCount    = ntohl( *( ( uint32_t * )( buf + offset ) ) );
	updateCount = ntohl( *( ( uint32_t * )( buf + offset + sizeof( uint32_t ) ) ) );

	return true;
}

bool Protocol::parseHotnessStatsHeader( struct HotnessStatsHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseHotnessStatsHeader(
		offset,
		header.getCount,
		header.updateCount,
		buf, size
	);
}

