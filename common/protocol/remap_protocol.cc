#include "protocol.hh"

size_t Protocol::generateRemappingLockHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t *original, uint32_t *remapped, uint32_t remappedCount, uint8_t keySize, char *key ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_REMAPPING_LOCK_SIZE + keySize + remappedCount * 4 * 4,
		instanceId, requestId
	);
	bytes += ProtocolUtil::write4Bytes( buf, remappedCount );
	bytes += ProtocolUtil::write1Byte ( buf, keySize );
	bytes += ProtocolUtil::write( buf, key, keySize );
	for ( uint32_t i = 0; i < remappedCount; i++ ) {
		bytes += ProtocolUtil::write4Bytes( buf, original[ i * 2     ] );
		bytes += ProtocolUtil::write4Bytes( buf, original[ i * 2 + 1 ] );
	}
	for ( uint32_t i = 0; i < remappedCount; i++ ) {
		bytes += ProtocolUtil::write4Bytes( buf, remapped[ i * 2     ] );
		bytes += ProtocolUtil::write4Bytes( buf, remapped[ i * 2 + 1 ] );
	}
	return bytes;
}

bool Protocol::parseRemappingLockHeader( struct RemappingLockHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_REMAPPING_LOCK_SIZE ) return false;
	char *ptr = buf + offset;
	header.remappedCount = ProtocolUtil::read4Bytes( ptr );
	header.keySize       = ProtocolUtil::read1Byte ( ptr );
	if ( size - offset < ( size_t ) PROTO_REMAPPING_LOCK_SIZE + header.keySize + header.remappedCount * 4 * 4 ) return false;
	header.key = ptr;
	ptr += header.keySize;
	header.original = ( uint32_t * ) ptr;
	header.remapped = ( ( uint32_t * ) ptr ) + header.remappedCount * 2;
	for ( uint32_t i = 0; i < header.remappedCount; i++ ) {
		header.original[ i * 2     ] = ntohl( header.original[ i * 2     ] );
		header.original[ i * 2 + 1 ] = ntohl( header.original[ i * 2 + 1 ] );
		header.remapped[ i * 2     ] = ntohl( header.remapped[ i * 2     ] );
		header.remapped[ i * 2 + 1 ] = ntohl( header.remapped[ i * 2 + 1 ] );
	}
	return true;
}

size_t Protocol::generateDegradedSetHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, uint32_t *original, uint32_t *remapped, uint32_t remappedCount, uint8_t keySize, char *key, uint32_t valueSize, char *value, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_SET_SIZE + keySize + valueSize + remappedCount * 4 * 4,
		instanceId, requestId, sendBuf
	);
	bytes += ProtocolUtil::write4Bytes( buf, listId );
	bytes += ProtocolUtil::write4Bytes( buf, chunkId );
	bytes += ProtocolUtil::write4Bytes( buf, remappedCount );
	bytes += ProtocolUtil::write1Byte ( buf, keySize );
	bytes += ProtocolUtil::write3Bytes( buf, valueSize );
	bytes += ProtocolUtil::write( buf, key, keySize );
	bytes += ProtocolUtil::write( buf, value, valueSize );
	if ( remappedCount ) {
		remappedCount *= 2; // Include both list ID and chunk ID
		for ( uint32_t i = 0; i < remappedCount; i++ )
			bytes += ProtocolUtil::write4Bytes( buf, original[ i ] );
		for ( uint32_t i = 0; i < remappedCount; i++ )
			bytes += ProtocolUtil::write4Bytes( buf, remapped[ i ] );
	}
	return bytes;
}

bool Protocol::parseDegradedSetHeader( struct DegradedSetHeader &header, char *buf, size_t size, size_t offset, struct sockaddr_in *target ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_DEGRADED_SET_SIZE ) return false;
	char *ptr = buf + offset;
	header.listId        = ProtocolUtil::read4Bytes( ptr );
	header.chunkId       = ProtocolUtil::read4Bytes( ptr );
	header.remappedCount = ProtocolUtil::read4Bytes( ptr );
	header.keySize       = ProtocolUtil::read1Byte ( ptr );
	header.valueSize     = ProtocolUtil::read3Bytes( ptr );
	if ( size - offset < PROTO_DEGRADED_SET_SIZE + header.keySize + header.valueSize + header.remappedCount * 4 * 4 ) return false;
	header.key   = ptr;
	header.value = ptr + header.keySize;
	ptr += header.keySize + header.valueSize;
	if ( header.remappedCount ) {
		uint32_t count = header.remappedCount * 2;
		header.original = ( uint32_t * ) ptr;
		header.remapped = ( ( uint32_t * ) ptr ) + count;
		for ( uint32_t i = 0; i < count; i++ ) {
			header.original[ i ] = ntohl( header.original[ i ] );
			header.remapped[ i ] = ntohl( header.remapped[ i ] );
		}
	} else {
		header.original = 0;
		header.remapped = 0;
	}
	return true;
}
