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

size_t Protocol::generateDeltaAcknowledgementHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, const std::vector<uint32_t> &timestamps, const std::vector<Key> &requests, uint16_t targetId, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;

	size_t bytes = 0;

	*( ( uint32_t * )( buf     ) ) = htonl( timestamps.size() );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( requests.size() );
	*( ( uint16_t * )( buf + 8 ) ) = htons( targetId );

	bytes += PROTO_ACK_DELTA_SIZE;
	buf += PROTO_ACK_DELTA_SIZE;

	for ( uint32_t i = 0, len = timestamps.size(); i < len; i++ ) {
		*( ( uint32_t * )( buf ) ) = htonl( timestamps[ i ] );
		buf += 4;
		bytes += 4;
	}

	Key key;
	for ( uint32_t i = 0, len = requests.size(); i < len; i++ ) {
		key = requests[ i ];

		buf[ 0 ] = key.size;
		memmove( buf + 1, key.data, key.size );

		buf += 1 + key.size;
		bytes += 1 + key.size;
	}

	bytes += this->generateHeader( magic, to, opcode, bytes, instanceId, requestId, sendBuf );

	return bytes;
}

bool Protocol::parseDeltaAcknowledgementHeader( size_t offset, uint32_t &tsCount, uint32_t &keyCount, std::vector<uint32_t> *timestamps, std::vector<Key> *requests, uint16_t &targetId, char *buf, size_t size ) {
	if ( size - offset < PROTO_ACK_DELTA_SIZE )
		return false;

	char *ptr = buf + offset;
	tsCount         = ntohl( *( ( uint32_t * )( ptr ) ) );
	keyCount        = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	targetId        = ntohs( *( ( uint16_t * )( ptr + 8 ) ) );

	ptr += PROTO_ACK_DELTA_SIZE;

	if ( size - offset < PROTO_ACK_DELTA_SIZE + tsCount * 4 + keyCount ) // assume 0-sized keys
		return false;

	if ( ! timestamps )
		return true;

	for ( uint32_t i = 0; i < tsCount; i ++ )
		timestamps->push_back( ntohl( *( ( uint32_t * )( ptr + i * 4 ) ) ) );

	ptr += tsCount * 4;

	if ( ! requests )
		return true;

	Key key;
	uint8_t keySize;
	uint32_t bytes = 0;
	for ( uint32_t i = 0; i < keyCount; i++ ) {

		keySize = ptr[ 0 ];
		bytes += 1 + keySize;

		if ( size - offset < PROTO_ACK_DELTA_SIZE + tsCount * 4 + bytes )
			return false;

		key.dup( keySize, ptr + 1  );
		requests->push_back( key );

		ptr += 1 + keySize;
	}

	return true;
}

bool Protocol::parseDeltaAcknowledgementHeader( struct DeltaAcknowledgementHeader &header, std::vector<uint32_t> *timestamps, std::vector<Key> *requests, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseDeltaAcknowledgementHeader(
		offset,
		header.tsCount,
		header.keyCount,
		timestamps,
		requests,
		header.targetId,
		buf, size
	);
}
