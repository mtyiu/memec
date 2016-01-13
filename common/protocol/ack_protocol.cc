#include "protocol.hh"

size_t Protocol::generateAcknowledgementHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t fromTimestamp, uint32_t toTimestamp, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_ACK_BASE_SIZE, instanceId, requestId, sendBuf );

	*( ( uint32_t * )( buf     ) ) = htonl( fromTimestamp );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( toTimestamp );

	bytes += PROTO_ACK_BASE_SIZE;

	return bytes;
}

bool Protocol::parseAcknowledgementHeader( size_t offset, uint32_t &fromTimestamp, uint32_t &toTimestamp, char *buf, size_t size ) {
	if ( size - offset < PROTO_ACK_BASE_SIZE )
		return false;

	char *ptr = buf + offset;
	fromTimestamp = ntohl( *( ( uint32_t * )( ptr     ) ) );
	toTimestamp   = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );

	return true;
}

bool Protocol::parseAcknowledgementHeader( struct AcknowledgementHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseAcknowledgementHeader(
		offset,
		header.fromTimestamp,
		header.toTimestamp,
		buf, size
	);
}

size_t Protocol::generateDeltaAcknowledgementHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, std::vector<uint32_t> &timestamps, uint16_t targetId, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_ACK_PARITY_DELTA_SIZE + timestamps.size() * 4, instanceId, requestId, sendBuf );

	*( ( uint32_t * )( buf     ) ) = htonl( timestamps.size() );
	*( ( uint16_t * )( buf + 4 ) ) = htons( targetId );

	bytes += PROTO_ACK_PARITY_DELTA_SIZE;
	buf += PROTO_ACK_PARITY_DELTA_SIZE;

	for ( uint32_t i = 0, len = timestamps.size(); i < len; i++ ) {
		*( ( uint32_t * )( buf ) ) = htonl( timestamps[ i ] );
		buf += 4;
		bytes += 4;
	}

	return bytes;
}

bool Protocol::parseDeltaAcknowledgementHeader( size_t offset, uint32_t &count, std::vector<uint32_t> *timestamps, uint16_t &targetId, char *buf, size_t size ) {
	if ( size - offset < PROTO_ACK_PARITY_DELTA_SIZE )
		return false;

	char *ptr = buf + offset;
	count         = ntohl( *( ( uint32_t * )( ptr ) ) );
	targetId      = ntohs( *( ( uint16_t * )( ptr + 4 ) ) );

	ptr += PROTO_ACK_PARITY_DELTA_SIZE;

	if ( size - offset < PROTO_ACK_PARITY_DELTA_SIZE + count * 4 )
		return false;

	if ( ! timestamps )
		return true;

	for ( uint32_t i = 0; i < count; i ++ )
		timestamps->push_back( ntohl( *( ( uint32_t * )( ptr + i * 4 ) ) ) );

	return true;
}

bool Protocol::parseDeltaAcknowledgementHeader( struct DeltaAcknowledgementHeader &header, std::vector<uint32_t> *timestamps, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseDeltaAcknowledgementHeader(
		offset,
		header.count,
		timestamps,
		header.targetId,
		buf, size
	);
}
