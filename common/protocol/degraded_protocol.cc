#include <cassert>
#include "protocol.hh"

size_t Protocol::generateDegradedLockReqHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	uint8_t keySize, char *key, bool isLarge
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_LOCK_REQ_SIZE + keySize + SPLIT_OFFSET_SIZE + reconstructedCount * 4 * 4,
		instanceId, requestId
	);

	bytes += ProtocolUtil::write4Bytes( buf, reconstructedCount );
	bytes += ProtocolUtil::write1Byte ( buf, keySize );
	bytes += ProtocolUtil::write1Byte ( buf, isLarge );
	bytes += ProtocolUtil::write( buf, key, keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ) );
	if ( ! isLarge ) // 3-byte placeholder
		bytes += ProtocolUtil::write3Bytes( buf, 0 );

	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		bytes += ProtocolUtil::write4Bytes( buf, original[ i * 2     ] );
		bytes += ProtocolUtil::write4Bytes( buf, original[ i * 2 + 1 ] );
	}
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		bytes += ProtocolUtil::write4Bytes( buf, reconstructed[ i * 2     ] );
		bytes += ProtocolUtil::write4Bytes( buf, reconstructed[ i * 2 + 1 ] );
	}

	return bytes;
}

bool Protocol::parseDegradedLockReqHeader( struct DegradedLockReqHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_DEGRADED_LOCK_REQ_SIZE ) return false;
	char *ptr = buf + offset;
	header.reconstructedCount = ProtocolUtil::read4Bytes( ptr );
	header.keySize            = ProtocolUtil::read1Byte ( ptr );
	header.isLarge            = ProtocolUtil::read1Byte ( ptr );

	if ( size - offset < ( size_t ) PROTO_DEGRADED_LOCK_REQ_SIZE + header.keySize + header.reconstructedCount * 4 * 4 ) return false;

	header.key = ptr;
	ptr += header.keySize;

	if ( ! header.isLarge )
		memset( ptr, 0, SPLIT_OFFSET_SIZE );
	ptr += SPLIT_OFFSET_SIZE;

	header.original = ( uint32_t * ) ptr;
	header.reconstructed = ( ( uint32_t * ) ptr ) + header.reconstructedCount * 2;
	for ( uint32_t i = 0; i < header.reconstructedCount; i++ ) {
		header.original[ i * 2     ]      = ntohl( header.original[ i * 2     ]      );
		header.original[ i * 2 + 1 ]      = ntohl( header.original[ i * 2 + 1 ]      );
		header.reconstructed[ i * 2     ] = ntohl( header.reconstructed[ i * 2     ] );
		header.reconstructed[ i * 2 + 1 ] = ntohl( header.reconstructed[ i * 2 + 1 ] );
	}
	return true;
}

size_t Protocol::generateDegradedLockResHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t length, uint8_t type, uint8_t keySize, char *key, bool isLarge, char *&buf ) {
	buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_LOCK_RES_BASE_SIZE + keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ) + length,
		instanceId, requestId
	);
	bytes += ProtocolUtil::write1Byte( buf, type    );
	bytes += ProtocolUtil::write1Byte( buf, keySize );
	bytes += ProtocolUtil::write1Byte( buf, isLarge );
	bytes += ProtocolUtil::write( buf, key, keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ) );
	return bytes;
}

size_t Protocol::generateDegradedLockResHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	bool isLocked, uint8_t keySize, char *key, bool isLarge,
	bool isSealed, uint32_t stripeId, uint32_t dataChunkId, uint32_t dataChunkCount,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	uint32_t ongoingAtChunk,
	uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds
) {
	char *buf;
	size_t bytes = this->generateDegradedLockResHeader(
		magic, to, opcode, instanceId, requestId,
		PROTO_DEGRADED_LOCK_RES_LOCK_SIZE + numSurvivingChunkIds * 4 + reconstructedCount * 4 * 4,
		isLocked ? PROTO_DEGRADED_LOCK_RES_IS_LOCKED : PROTO_DEGRADED_LOCK_RES_WAS_LOCKED,
		keySize, key, isLarge, buf
	);

	bytes += ProtocolUtil::write1Byte ( buf, isSealed             );
	bytes += ProtocolUtil::write4Bytes( buf, stripeId             );
	bytes += ProtocolUtil::write4Bytes( buf, reconstructedCount   );
	bytes += ProtocolUtil::write4Bytes( buf, ongoingAtChunk       );
	bytes += ProtocolUtil::write1Byte ( buf, numSurvivingChunkIds );
	for ( uint8_t i = 0; i < numSurvivingChunkIds; i++ )
		bytes += ProtocolUtil::write4Bytes( buf, survivingChunkIds[ i ] );

	// Entries that are related to this key
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		uint32_t chunkId = original[ i * 2 + 1 ];
		if ( ( chunkId < dataChunkCount && chunkId == dataChunkId ) ||
		     ( chunkId >= dataChunkCount ) ) {
			bytes += ProtocolUtil::write4Bytes( buf, original[ i * 2     ] );
	 		bytes += ProtocolUtil::write4Bytes( buf, original[ i * 2 + 1 ] );
		}
	}
	// Entries that are not related to this key
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		uint32_t chunkId = original[ i * 2 + 1 ];
		if ( ( chunkId < dataChunkCount && chunkId == dataChunkId ) ||
		     ( chunkId >= dataChunkCount ) ) {
			// Copied
		} else {
			bytes += ProtocolUtil::write4Bytes( buf, original[ i * 2     ] );
	 		bytes += ProtocolUtil::write4Bytes( buf, original[ i * 2 + 1 ] );
		}
	}
	// Entries that are related to this key
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		uint32_t chunkId = original[ i * 2 + 1 ];
		if ( ( chunkId < dataChunkCount && chunkId == dataChunkId ) ||
		     ( chunkId >= dataChunkCount ) ) {
			bytes += ProtocolUtil::write4Bytes( buf, reconstructed[ i * 2     ] );
	 		bytes += ProtocolUtil::write4Bytes( buf, reconstructed[ i * 2 + 1 ] );
		}
	}
	// Entries that are not related to this key
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		uint32_t chunkId = original[ i * 2 + 1 ];
		if ( ( chunkId < dataChunkCount && chunkId == dataChunkId ) ||
		     ( chunkId >= dataChunkCount ) ) {
			// Copied
		} else {
			bytes += ProtocolUtil::write4Bytes( buf, reconstructed[ i * 2     ] );
			bytes += ProtocolUtil::write4Bytes( buf, reconstructed[ i * 2 + 1 ] );
		}
	}
	return bytes;
}

size_t Protocol::generateDegradedLockResHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	bool exist, uint8_t keySize, char *key, bool isLarge
) {
	char *buf;
	size_t bytes = this->generateDegradedLockResHeader(
		magic, to, opcode, instanceId, requestId,
		PROTO_DEGRADED_LOCK_RES_NOT_SIZE,
		exist ? PROTO_DEGRADED_LOCK_RES_NOT_LOCKED : PROTO_DEGRADED_LOCK_RES_NOT_EXIST,
		keySize, key, isLarge, buf
	);
	return bytes;
}

size_t Protocol::generateDegradedLockResHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	uint8_t keySize, char *key, bool isLarge,
	uint32_t *original, uint32_t *remapped, uint32_t remappedCount
) {
	char *buf;
	size_t bytes = this->generateDegradedLockResHeader(
		magic, to, opcode, instanceId, requestId,
		PROTO_DEGRADED_LOCK_RES_REMAP_SIZE + remappedCount * 4 * 4,
		PROTO_DEGRADED_LOCK_RES_REMAPPED,
		keySize, key, isLarge, buf
	);
	bytes += ProtocolUtil::write4Bytes( buf, remappedCount );
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

bool Protocol::parseDegradedLockResHeader( size_t offset, uint8_t &type, uint8_t &keySize, char *&key, bool &isLarge, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_LOCK_RES_BASE_SIZE ) return false;
	char *ptr = buf + offset;
	type    = ProtocolUtil::read1Byte( ptr );
	keySize = ProtocolUtil::read1Byte( ptr );
	isLarge = ProtocolUtil::read1Byte( ptr );
	key     = ptr;
	return ( size - offset >= PROTO_DEGRADED_LOCK_RES_BASE_SIZE + ( size_t ) keySize );
}

bool Protocol::parseDegradedLockResHeader(
	size_t offset, bool &isSealed,
	uint32_t &stripeId,
	uint32_t *&original, uint32_t *&reconstructed, uint32_t &reconstructedCount,
	uint32_t &ongoingAtChunk, uint8_t &numSurvivingChunkIds, uint32_t *&survivingChunkIds,
	char *buf, size_t size
) {
	if ( size - offset < PROTO_DEGRADED_LOCK_RES_LOCK_SIZE ) return false;
	char *ptr = buf + offset;
	isSealed             = ProtocolUtil::read1Byte ( ptr );
	stripeId             = ProtocolUtil::read4Bytes( ptr );
	reconstructedCount   = ProtocolUtil::read4Bytes( ptr );
	ongoingAtChunk       = ProtocolUtil::read4Bytes( ptr );
	numSurvivingChunkIds = ProtocolUtil::read1Byte ( ptr );

	survivingChunkIds = ( uint32_t * )( ptr );
	for ( uint8_t i = 0; i < numSurvivingChunkIds; i++ )
		survivingChunkIds[ i ] = ntohl( survivingChunkIds[ i ] );
	ptr += numSurvivingChunkIds * 4;

	original = ( uint32_t * ) ptr;
	reconstructed = ( ( uint32_t * ) ptr ) + reconstructedCount * 2;
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		original[ i * 2     ] = ntohl( original[ i * 2     ] );
		original[ i * 2 + 1 ] = ntohl( original[ i * 2 + 1 ] );
		reconstructed[ i * 2     ] = ntohl( reconstructed[ i * 2     ] );
		reconstructed[ i * 2 + 1 ] = ntohl( reconstructed[ i * 2 + 1 ] );
	}
	return true;
}

bool Protocol::parseDegradedLockResHeader(
	size_t offset,
	uint32_t *&original, uint32_t *&remapped, uint32_t &remappedCount,
	char *buf, size_t size
) {
	if ( size - offset < PROTO_DEGRADED_LOCK_RES_REMAP_SIZE ) return false;
	char *ptr = buf + offset;
	remappedCount = ProtocolUtil::read4Bytes( ptr );
	original = ( uint32_t * ) ptr;
	remapped = ( ( uint32_t * ) ptr ) + remappedCount * 2;
	for ( uint32_t i = 0; i < remappedCount; i++ ) {
		original[ i * 2     ] = ntohl( original[ i * 2     ] );
		original[ i * 2 + 1 ] = ntohl( original[ i * 2 + 1 ] );
		remapped[ i * 2     ] = ntohl( remapped[ i * 2     ] );
		remapped[ i * 2 + 1 ] = ntohl( remapped[ i * 2 + 1 ] );
	}
	return true;
}

bool Protocol::parseDegradedLockResHeader( struct DegradedLockResHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	bool ret = this->parseDegradedLockResHeader(
		offset,
		header.type,
		header.keySize,
		header.key,
		header.isLarge,
		buf, size
	);
	if ( ! ret ) return false;
	offset += PROTO_DEGRADED_LOCK_RES_BASE_SIZE + header.keySize + ( header.isLarge ? SPLIT_OFFSET_SIZE : 0 );
	switch( header.type ) {
		case PROTO_DEGRADED_LOCK_RES_IS_LOCKED:
		case PROTO_DEGRADED_LOCK_RES_WAS_LOCKED:
			ret = this->parseDegradedLockResHeader(
				offset,
				header.isSealed,
				header.stripeId,
				header.original,
				header.reconstructed,
				header.reconstructedCount,
				header.ongoingAtChunk,
				header.numSurvivingChunkIds,
				header.survivingChunkIds,
				buf, size
			);
			break;
		case PROTO_DEGRADED_LOCK_RES_NOT_LOCKED:
		case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
			break;
		case PROTO_DEGRADED_LOCK_RES_REMAPPED:
			ret = this->parseDegradedLockResHeader(
				offset,
				header.original,
				header.remapped,
				header.remappedCount,
				buf, size
			);
			break;
		default:
			return false;
	}
	return ret;
}

size_t Protocol::generateDegradedReqHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	bool isSealed, uint32_t stripeId,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds,
	uint8_t keySize, char *key, bool isLarge,
	uint32_t timestamp
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_REQ_BASE_SIZE + numSurvivingChunkIds * 4 + reconstructedCount * 4 * 4 + PROTO_KEY_SIZE + keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ),
		instanceId, requestId, 0,
		timestamp, isLarge
	);
	bytes += ProtocolUtil::write1Byte ( buf, isLarge              );
	bytes += ProtocolUtil::write1Byte ( buf, isSealed             );
	bytes += ProtocolUtil::write4Bytes( buf, stripeId             );
	bytes += ProtocolUtil::write4Bytes( buf, reconstructedCount   );
	bytes += ProtocolUtil::write4Bytes( buf, ongoingAtChunk       );
	bytes += ProtocolUtil::write1Byte ( buf, numSurvivingChunkIds );
	for ( uint8_t i = 0; i < numSurvivingChunkIds; i++ )
		bytes += ProtocolUtil::write4Bytes( buf, survivingChunkIds[ i ] );
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		bytes += ProtocolUtil::write4Bytes( buf, original[ i * 2     ] );
		bytes += ProtocolUtil::write4Bytes( buf, original[ i * 2 + 1 ] );
	}
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		bytes += ProtocolUtil::write4Bytes( buf, reconstructed[ i * 2     ] );
		bytes += ProtocolUtil::write4Bytes( buf, reconstructed[ i * 2 + 1 ] );
	}
	bytes += ProtocolUtil::write1Byte( buf, keySize );
	bytes += ProtocolUtil::write( buf, key, keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ) );
	return bytes;
}

size_t Protocol::generateDegradedReqHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	bool isSealed, uint32_t stripeId,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds,
	uint8_t keySize, char *key, bool isLarge,
	uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate,
	uint32_t timestamp
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_REQ_BASE_SIZE + numSurvivingChunkIds * 4 + reconstructedCount * 4 * 4 + PROTO_KEY_VALUE_UPDATE_SIZE + keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ) + valueUpdateSize,
		instanceId, requestId, 0, timestamp
	);

	bytes += ProtocolUtil::write1Byte ( buf, isLarge              );
	bytes += ProtocolUtil::write1Byte ( buf, isSealed             );
	bytes += ProtocolUtil::write4Bytes( buf, stripeId             );
	bytes += ProtocolUtil::write4Bytes( buf, reconstructedCount   );
	bytes += ProtocolUtil::write4Bytes( buf, ongoingAtChunk       );
	bytes += ProtocolUtil::write1Byte ( buf, numSurvivingChunkIds );
	for ( uint8_t i = 0; i < numSurvivingChunkIds; i++ )
		bytes += ProtocolUtil::write4Bytes( buf, survivingChunkIds[ i ] );
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		bytes += ProtocolUtil::write4Bytes( buf, original[ i * 2     ] );
		bytes += ProtocolUtil::write4Bytes( buf, original[ i * 2 + 1 ] );
	}
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		bytes += ProtocolUtil::write4Bytes( buf, reconstructed[ i * 2     ] );
		bytes += ProtocolUtil::write4Bytes( buf, reconstructed[ i * 2 + 1 ] );
	}
	bytes += ProtocolUtil::write1Byte ( buf, keySize );
	bytes += ProtocolUtil::write3Bytes( buf, valueUpdateSize );
	bytes += ProtocolUtil::write3Bytes( buf, valueUpdateOffset );
	bytes += ProtocolUtil::write( buf, key, keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ) );
	bytes += ProtocolUtil::write( buf, valueUpdate, valueUpdateSize );
	return bytes;
}

bool Protocol::parseDegradedReqHeader(
	size_t offset,
	bool &isSealed, uint32_t &stripeId, bool &isLarge,
	uint32_t *&original, uint32_t *&reconstructed, uint32_t &reconstructedCount,
	uint32_t &ongoingAtChunk, uint8_t &numSurvivingChunkIds, uint32_t *&survivingChunkIds,
	char *buf, size_t size
) {
	if ( size - offset < PROTO_DEGRADED_REQ_BASE_SIZE ) return false;
	char *ptr = buf + offset;
	isLarge              = ProtocolUtil::read1Byte ( ptr );
	isSealed             = ProtocolUtil::read1Byte ( ptr );
	stripeId             = ProtocolUtil::read4Bytes( ptr );
	reconstructedCount   = ProtocolUtil::read4Bytes( ptr );
	ongoingAtChunk       = ProtocolUtil::read4Bytes( ptr );
	numSurvivingChunkIds = ProtocolUtil::read1Byte ( ptr );

	survivingChunkIds = ( uint32_t * )( ptr );
	for ( uint8_t i = 0; i < numSurvivingChunkIds; i++ )
		survivingChunkIds[ i ] = ntohl( survivingChunkIds[ i ] );
	ptr += numSurvivingChunkIds * 4;

	original = ( uint32_t * ) ptr;
	reconstructed = ( ( uint32_t * ) ptr ) + reconstructedCount * 2;
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		original[ i * 2     ] = ntohl( original[ i * 2     ] );
		original[ i * 2 + 1 ] = ntohl( original[ i * 2 + 1 ] );
		reconstructed[ i * 2     ] = ntohl( reconstructed[ i * 2     ] );
		reconstructed[ i * 2 + 1 ] = ntohl( reconstructed[ i * 2 + 1 ] );
	}
	return true;
}

bool Protocol::parseDegradedReqHeader( struct DegradedReqHeader &header, uint8_t opcode, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	bool ret = this->parseDegradedReqHeader(
		offset,
		header.isSealed,
		header.stripeId,
		header.isLarge,
		header.original,
		header.reconstructed,
		header.reconstructedCount,
		header.ongoingAtChunk,
		header.numSurvivingChunkIds,
		header.survivingChunkIds,
		buf, size
	);
	if ( ! ret )
		return false;
	offset += PROTO_DEGRADED_REQ_BASE_SIZE + header.numSurvivingChunkIds * 4 + header.reconstructedCount * 4 * 4;

	switch( opcode ) {
		case PROTO_OPCODE_DEGRADED_GET:
		case PROTO_OPCODE_DEGRADED_DELETE:
			return this->parseKeyHeader( header.data.key, buf, size, offset );
		case PROTO_OPCODE_DEGRADED_UPDATE:
			ret = this->parseKeyValueUpdateHeader( header.data.keyValueUpdate, true, buf, size, offset );
			if ( header.isLarge )
				header.data.keyValueUpdate.valueUpdate += SPLIT_OFFSET_SIZE;
			return ret;
		default:
			return false;
	}
}

size_t Protocol::generateForwardKeyReqHeader(
	uint8_t magic, uint8_t to, uint8_t opcode,
	uint16_t instanceId, uint32_t requestId,
	uint8_t degradedOpcode, uint32_t listId, uint32_t stripeId, uint32_t chunkId,
	uint8_t keySize, char *key, uint32_t valueSize, char *value,
	uint32_t valueUpdateSize, uint32_t valueUpdateOffset, char *valueUpdate
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_FORWARD_KEY_BASE_SIZE + keySize + valueSize +
			( degradedOpcode == PROTO_OPCODE_DEGRADED_UPDATE ? PROTO_FORWARD_KEY_UPDATE_SIZE + valueUpdateSize : 0 ),
		instanceId, requestId
	);
	bytes += ProtocolUtil::write1Byte ( buf, degradedOpcode );
	bytes += ProtocolUtil::write4Bytes( buf, listId         );
	bytes += ProtocolUtil::write4Bytes( buf, stripeId       );
	bytes += ProtocolUtil::write4Bytes( buf, chunkId        );
	bytes += ProtocolUtil::write1Byte ( buf, keySize        );
	bytes += ProtocolUtil::write3Bytes( buf, valueSize      );
	bytes += ProtocolUtil::write( buf, key, keySize     );
	bytes += ProtocolUtil::write( buf, value, valueSize );
	if ( degradedOpcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		bytes += ProtocolUtil::write3Bytes( buf, valueUpdateSize   );
		bytes += ProtocolUtil::write3Bytes( buf, valueUpdateOffset );
		bytes += ProtocolUtil::write( buf, valueUpdate, valueUpdateSize );
	}
	return bytes;
}

bool Protocol::parseForwardKeyReqHeader( struct ForwardKeyHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_FORWARD_KEY_BASE_SIZE ) return false;
	char *ptr = buf + offset;
	header.opcode    = ProtocolUtil::read1Byte ( ptr );
	header.listId    = ProtocolUtil::read4Bytes( ptr );
	header.stripeId  = ProtocolUtil::read4Bytes( ptr );
	header.chunkId   = ProtocolUtil::read4Bytes( ptr );
	header.keySize   = ProtocolUtil::read1Byte ( ptr );
	header.valueSize = ProtocolUtil::read3Bytes( ptr );
	if ( size - offset < ( size_t ) PROTO_FORWARD_KEY_BASE_SIZE + header.keySize + header.valueSize ) return false;

	header.key = ptr;
	header.value = ptr + header.keySize;
	ptr += header.keySize + header.valueSize;

	if ( header.opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		if ( size - offset < ( size_t ) PROTO_FORWARD_KEY_BASE_SIZE + header.keySize + header.valueSize + PROTO_FORWARD_KEY_UPDATE_SIZE ) return false;
		header.valueUpdateSize   = ProtocolUtil::read3Bytes( ptr );
		header.valueUpdateOffset = ProtocolUtil::read3Bytes( ptr );
		header.valueUpdate       = ptr;
		if ( size - offset < ( size_t ) PROTO_FORWARD_KEY_BASE_SIZE + header.keySize + header.valueSize + PROTO_FORWARD_KEY_UPDATE_SIZE + header.valueUpdateSize ) return false;
	}
	return true;
}

size_t Protocol::generateForwardKeyResHeader(
	uint8_t magic, uint8_t to, uint8_t opcode,
	uint16_t instanceId, uint32_t requestId,
	uint8_t degradedOpcode, uint32_t listId, uint32_t stripeId, uint32_t chunkId,
	uint8_t keySize, char *key, uint32_t valueSize,
	uint32_t valueUpdateSize, uint32_t valueUpdateOffset
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_FORWARD_KEY_BASE_SIZE + keySize +
			( degradedOpcode == PROTO_OPCODE_DEGRADED_UPDATE ? PROTO_FORWARD_KEY_UPDATE_SIZE : 0 ),
		instanceId, requestId
	);
	bytes += ProtocolUtil::write1Byte ( buf, degradedOpcode );
	bytes += ProtocolUtil::write4Bytes( buf, listId         );
	bytes += ProtocolUtil::write4Bytes( buf, stripeId       );
	bytes += ProtocolUtil::write4Bytes( buf, chunkId        );
	bytes += ProtocolUtil::write1Byte ( buf, keySize        );
	bytes += ProtocolUtil::write3Bytes( buf, valueSize      );
	bytes += ProtocolUtil::write( buf, key, keySize     );
	if ( degradedOpcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		bytes += ProtocolUtil::write3Bytes( buf, valueUpdateSize   );
		bytes += ProtocolUtil::write3Bytes( buf, valueUpdateOffset );
	}
	return bytes;
}

bool Protocol::parseForwardKeyResHeader( struct ForwardKeyHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_FORWARD_KEY_BASE_SIZE ) return false;
	char *ptr = buf + offset;
	header.opcode    = ProtocolUtil::read1Byte ( ptr );
	header.listId    = ProtocolUtil::read4Bytes( ptr );
	header.stripeId  = ProtocolUtil::read4Bytes( ptr );
	header.chunkId   = ProtocolUtil::read4Bytes( ptr );
	header.keySize   = ProtocolUtil::read1Byte ( ptr );
	header.valueSize = ProtocolUtil::read3Bytes( ptr );
	if ( size - offset < ( size_t ) PROTO_FORWARD_KEY_BASE_SIZE + header.keySize ) return false;
	header.key = ptr;
	ptr += header.keySize;
	if ( header.opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		if ( size - offset < ( size_t ) PROTO_FORWARD_KEY_BASE_SIZE + header.keySize + PROTO_FORWARD_KEY_UPDATE_SIZE ) return false;
		header.valueUpdateSize   = ProtocolUtil::read3Bytes( ptr );
		header.valueUpdateOffset = ProtocolUtil::read3Bytes( ptr );
	}
	return true;
}

size_t Protocol::generateListStripeKeyHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key, bool isLarge ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_LIST_STRIPE_KEY_SIZE + keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ),
		instanceId, requestId, 0, 0, isLarge
	);
	bytes += ProtocolUtil::write4Bytes( buf, listId  );
	bytes += ProtocolUtil::write4Bytes( buf, chunkId );
	bytes += ProtocolUtil::write1Byte ( buf, keySize );
	bytes += ProtocolUtil::write( buf, key, keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ) );
	return bytes;
}

bool Protocol::parseListStripeKeyHeader( struct ListStripeKeyHeader &header, bool isLarge, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_LIST_STRIPE_KEY_SIZE ) return false;
	char *ptr = buf + offset;
	header.listId  = ProtocolUtil::read4Bytes( ptr );
	header.chunkId = ProtocolUtil::read4Bytes( ptr );
	header.keySize = ProtocolUtil::read1Byte ( ptr );
	header.key = ptr;
	return ( size - offset >= ( size_t ) PROTO_LIST_STRIPE_KEY_SIZE + header.keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ) );
}

size_t Protocol::generateDegradedReleaseReqHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, std::vector<Metadata> &chunks, bool &isCompleted ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = 0;
	uint32_t count = 0;
	isCompleted = true;
	for ( size_t i = 0, len = chunks.size(); i < len; i++ ) {
		if ( this->buffer.size >= bytes + PROTO_DEGRADED_RELEASE_REQ_SIZE ) {
			bytes += ProtocolUtil::write4Bytes( buf, chunks[ i ].listId   );
			bytes += ProtocolUtil::write4Bytes( buf, chunks[ i ].stripeId );
			bytes += ProtocolUtil::write4Bytes( buf, chunks[ i ].chunkId  );
			count++;
		} else {
			isCompleted = false;
			break;
		}
	}
	// TODO: remove only after all servers acknowledge
	chunks.erase( chunks.begin(), chunks.begin() + count );
	bytes += this->generateHeader( magic, to, opcode, bytes, instanceId, requestId );
	return bytes;
}

size_t Protocol::generateDegradedReleaseResHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t count ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_DEGRADED_RELEASE_RES_SIZE, instanceId, requestId );
	bytes += ProtocolUtil::write4Bytes( buf, count );
	return bytes;
}

bool Protocol::parseDegradedReleaseResHeader( struct DegradedReleaseResHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_DEGRADED_RELEASE_RES_SIZE ) return false;
	char *ptr = buf + offset;
	header.count = ProtocolUtil::read4Bytes( ptr );
	return true;
}
