#include "protocol.hh"

size_t Protocol::generateAcknowledgementHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t fromTimestamp, uint32_t toTimestamp, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_ACK_BASE_SIZE, instanceId, requestId, sendBuf );

	bytes += ProtocolUtil::write4Bytes( buf, fromTimestamp );
	bytes += ProtocolUtil::write4Bytes( buf, toTimestamp );

	return bytes;
}

bool Protocol::parseAcknowledgementHeader( struct AcknowledgementHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_ACK_BASE_SIZE ) return false;
	char *ptr = buf + offset;
	header.fromTimestamp = ProtocolUtil::read4Bytes( ptr );
	header.toTimestamp   = ProtocolUtil::read4Bytes( ptr );
	return true;
}

size_t Protocol::generateDeltaAcknowledgementHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, const std::vector<uint32_t> &timestamps, const std::vector<Key> &requests, uint16_t targetId, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = 0;
	bytes += ProtocolUtil::write4Bytes( buf, timestamps.size() );
	bytes += ProtocolUtil::write4Bytes( buf, requests.size() );
	bytes += ProtocolUtil::write2Bytes( buf, targetId );
	for ( uint32_t i = 0, len = timestamps.size(); i < len; i++ )
		bytes += ProtocolUtil::write4Bytes( buf, timestamps[ i ] );
	for ( uint32_t i = 0, len = requests.size(); i < len; i++ ) {
		const Key &key = requests[ i ];
		bytes += ProtocolUtil::write1Byte( buf, key.size );
		bytes += ProtocolUtil::write( buf, key.data, key.size );
	}
	bytes += this->generateHeader( magic, to, opcode, bytes, instanceId, requestId, sendBuf );
	return bytes;
}

bool Protocol::parseDeltaAcknowledgementHeader( struct DeltaAcknowledgementHeader &header, std::vector<uint32_t> *timestamps, std::vector<Key> *requests, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_ACK_DELTA_SIZE ) return false;

	char *ptr = buf + offset;
	header.tsCount  = ProtocolUtil::read4Bytes( ptr );
	header.keyCount = ProtocolUtil::read4Bytes( ptr );
	header.targetId = ProtocolUtil::read2Bytes( ptr );

	if ( size - offset < PROTO_ACK_DELTA_SIZE + header.tsCount * 4 + header.keyCount ) return false; // assume 0-sized keys
	if ( ! timestamps ) return true;
	for ( uint32_t i = 0; i < header.tsCount; i ++ )
		timestamps->push_back( ProtocolUtil::read4Bytes( ptr ) );
	if ( ! requests ) return true;

	Key key;
	uint8_t keySize;
	uint32_t bytes = 0;
	for ( uint32_t i = 0; i < header.keyCount; i++ ) {
		keySize = ProtocolUtil::read1Byte( ptr );
		bytes += 1 + keySize;

		if ( size - offset < PROTO_ACK_DELTA_SIZE + header.tsCount * 4 + bytes ) return false;

		key.dup( keySize, ptr );
		ptr += keySize;
		requests->push_back( key );
	}
	return true;
}
