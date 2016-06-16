#include "protocol.hh"

size_t Protocol::generateKeyHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint8_t keySize, char *key, char *sendBuf, uint32_t timestamp, bool isLarge ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_KEY_SIZE + keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ), instanceId, requestId, sendBuf, timestamp, isLarge );
	bytes += ProtocolUtil::write1Byte( buf, keySize );
	bytes += ProtocolUtil::write( buf, key, keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ) );
	return bytes;
}

bool Protocol::parseKeyHeader( struct KeyHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_KEY_SIZE ) return false;
	char *ptr = buf + offset;
	header.keySize = ProtocolUtil::read1Byte( ptr );
	header.key     = ptr;
	return ( size - offset >= ( size_t ) PROTO_KEY_SIZE + header.keySize );
}

size_t Protocol::generateKeyBackupHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId, Metadata *sealed, uint8_t sealedCount, uint8_t keySize, char *key, bool isLarge, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_KEY_BACKUP_BASE_SIZE + PROTO_KEY_BACKUP_FOR_DATA_SIZE + sealedCount * PROTO_KEY_BACKUP_SEALED_SIZE + keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ),
		instanceId, requestId, sendBuf, 0, isLarge
	);
	bytes += ProtocolUtil::write1Byte ( buf, false          ); // isParity
	bytes += ProtocolUtil::write1Byte ( buf, keySize        );
	bytes += ProtocolUtil::write1Byte ( buf, isLarge        );
	bytes += ProtocolUtil::write4Bytes( buf, timestamp      );
	bytes += ProtocolUtil::write4Bytes( buf, listId         );
	bytes += ProtocolUtil::write4Bytes( buf, stripeId       );
	bytes += ProtocolUtil::write4Bytes( buf, chunkId        );
	bytes += ProtocolUtil::write1Byte ( buf, sealedCount    );
	for ( uint8_t i = 0; i < sealedCount; i++ ) {
		bytes += ProtocolUtil::write4Bytes( buf, sealed[ i ].listId   );
		bytes += ProtocolUtil::write4Bytes( buf, sealed[ i ].stripeId );
		bytes += ProtocolUtil::write4Bytes( buf, sealed[ i ].chunkId  );
	}
	bytes += ProtocolUtil::write( buf, key, keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ) );
	return bytes;
}

size_t Protocol::generateKeyBackupHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint8_t keySize, char *key, bool isLarge, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_KEY_BACKUP_BASE_SIZE + PROTO_KEY_BACKUP_FOR_DATA_SIZE + keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ),
		instanceId, requestId, sendBuf, 0, isLarge
	);
	bytes += ProtocolUtil::write1Byte ( buf, false     ); // isParity
	bytes += ProtocolUtil::write1Byte ( buf, keySize   );
	bytes += ProtocolUtil::write1Byte ( buf, isLarge   );
	bytes += ProtocolUtil::write4Bytes( buf, timestamp );
	bytes += ProtocolUtil::write4Bytes( buf, listId    );
	bytes += ProtocolUtil::write4Bytes( buf, stripeId  );
	bytes += ProtocolUtil::write4Bytes( buf, chunkId   );
	bytes += ProtocolUtil::write1Byte ( buf, 0         ); // sealedCount
	bytes += ProtocolUtil::write( buf, key, keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ) );
	return bytes;
}

size_t Protocol::generateKeyBackupHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint8_t keySize, char *key, bool isLarge, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_KEY_BACKUP_BASE_SIZE + keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ), instanceId, requestId, sendBuf, 0, isLarge );
	bytes += ProtocolUtil::write1Byte ( buf, true    ); // isParity
	bytes += ProtocolUtil::write1Byte ( buf, keySize );
	bytes += ProtocolUtil::write1Byte ( buf, isLarge );
	bytes += ProtocolUtil::write( buf, key, keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ) );
	return bytes;
}

bool Protocol::parseKeyBackupHeader( struct KeyBackupHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_KEY_BACKUP_BASE_SIZE ) return false;
	char *ptr = buf + offset;
	header.isParity = ProtocolUtil::read1Byte( ptr );
	header.keySize  = ProtocolUtil::read1Byte( ptr );
	header.isLarge  = ProtocolUtil::read1Byte( ptr );
	if ( header.isParity ) {
		if ( size - offset < ( size_t ) PROTO_KEY_BACKUP_BASE_SIZE + header.keySize ) return false;
		header.key = ptr;
	} else {
		if ( size - offset < ( size_t ) PROTO_KEY_BACKUP_BASE_SIZE + PROTO_KEY_BACKUP_FOR_DATA_SIZE + header.keySize ) return false;
		header.timestamp   = ProtocolUtil::read4Bytes( ptr );
		header.listId      = ProtocolUtil::read4Bytes( ptr );
		header.stripeId    = ProtocolUtil::read4Bytes( ptr );
		header.chunkId     = ProtocolUtil::read4Bytes( ptr );
		header.sealedCount = ProtocolUtil::read1Byte ( ptr );
		if ( size - offset < ( size_t ) PROTO_KEY_BACKUP_BASE_SIZE + PROTO_KEY_BACKUP_FOR_DATA_SIZE + header.sealedCount * PROTO_KEY_BACKUP_SEALED_SIZE + header.keySize ) return false;
		for ( uint8_t i = 0; i < header.sealedCount; i++ ) {
			header.sealed[ i ].listId   = ProtocolUtil::read4Bytes( ptr );
			header.sealed[ i ].stripeId = ProtocolUtil::read4Bytes( ptr );
			header.sealed[ i ].chunkId  = ProtocolUtil::read4Bytes( ptr );
		}
		header.key = ptr;
	}
	return true;
}

size_t Protocol::generateChunkKeyHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint8_t keySize, char *key, char *sendBuf, uint32_t timestamp ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_KEY_SIZE + keySize, instanceId, requestId, sendBuf, timestamp );
	bytes += ProtocolUtil::write4Bytes( buf, listId );
	bytes += ProtocolUtil::write4Bytes( buf, stripeId );
	bytes += ProtocolUtil::write4Bytes( buf, chunkId );
	bytes += ProtocolUtil::write1Byte ( buf, keySize );
	bytes += ProtocolUtil::write( buf, key, keySize );
	return bytes;
}

bool Protocol::parseChunkKeyHeader( struct ChunkKeyHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_CHUNK_KEY_SIZE ) return false;
	char *ptr = buf + offset;
	header.listId   = ProtocolUtil::read4Bytes( ptr );
	header.stripeId = ProtocolUtil::read4Bytes( ptr );
	header.chunkId  = ProtocolUtil::read4Bytes( ptr );
	header.keySize  = ProtocolUtil::read1Byte ( ptr );
	header.key = ptr;
	return ( size - offset >= ( size_t ) PROTO_CHUNK_KEY_SIZE + header.keySize );
}

size_t Protocol::generateChunkKeyValueUpdateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint8_t keySize, bool isLarge, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, uint32_t chunkUpdateOffset, char *valueUpdate, char *sendBuf, uint32_t timestamp ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE + keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ) + ( valueUpdate ? valueUpdateSize : 0 ), instanceId, requestId, sendBuf, timestamp );
	bytes += ProtocolUtil::write4Bytes( buf, listId            );
	bytes += ProtocolUtil::write4Bytes( buf, stripeId          );
	bytes += ProtocolUtil::write4Bytes( buf, chunkId           );
	bytes += ProtocolUtil::write1Byte ( buf, keySize           );
	bytes += ProtocolUtil::write3Bytes( buf, valueUpdateSize   );
	bytes += ProtocolUtil::write3Bytes( buf, valueUpdateOffset );
	bytes += ProtocolUtil::write3Bytes( buf, chunkUpdateOffset );
	bytes += ProtocolUtil::write1Byte ( buf, isLarge           );
	bytes += ProtocolUtil::write( buf, key, keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ) );
	if ( valueUpdateSize && valueUpdate )
		bytes += ProtocolUtil::write( buf, valueUpdate, valueUpdateSize );
	return bytes;
}

bool Protocol::parseChunkKeyValueUpdateHeader( struct ChunkKeyValueUpdateHeader &header, bool withValueUpdate, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE ) return false;
	char *ptr = buf + offset;
	header.listId            = ProtocolUtil::read4Bytes( ptr );
	header.stripeId          = ProtocolUtil::read4Bytes( ptr );
	header.chunkId           = ProtocolUtil::read4Bytes( ptr );
	header.keySize           = ProtocolUtil::read1Byte ( ptr );
	header.valueUpdateSize   = ProtocolUtil::read3Bytes( ptr );
	header.valueUpdateOffset = ProtocolUtil::read3Bytes( ptr );
	header.chunkUpdateOffset = ProtocolUtil::read3Bytes( ptr );
	header.isLarge           = ProtocolUtil::read1Byte ( ptr );
	header.key = ptr;
	header.valueUpdate = withValueUpdate && header.valueUpdateSize ? header.key + header.keySize + ( header.isLarge ? SPLIT_OFFSET_SIZE : 0 ) : 0;
	return ( size - offset >= PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE + header.keySize + ( header.isLarge ? SPLIT_OFFSET_SIZE : 0 ) + ( withValueUpdate ? header.valueUpdateSize : 0 ) );
}

size_t Protocol::generateKeyValueHeader(
	uint8_t magic, uint8_t to, uint8_t opcode,
	uint16_t instanceId, uint32_t requestId,
	uint8_t keySize, char *key,
	uint32_t valueSize, char *value,
	char *sendBuf, uint32_t timestamp,
	uint32_t splitOffset, uint32_t splitSize, bool isLarge
) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = 0;
	bytes += ProtocolUtil::write1Byte ( buf, keySize );
	bytes += ProtocolUtil::write3Bytes( buf, valueSize );
	bytes += ProtocolUtil::write( buf, key, keySize );

	if ( splitSize == 0 || splitSize == valueSize ) {
		if ( valueSize ) bytes += ProtocolUtil::write( buf, value, valueSize );
	} else {
		bytes += ProtocolUtil::write3Bytes( buf, splitOffset );
		if ( splitOffset + splitSize > valueSize )
			splitSize = valueSize - splitOffset;
		if ( valueSize ) bytes += ProtocolUtil::write( buf, value, splitSize );
	}
	bytes += this->generateHeader(
		magic, to, opcode,
		bytes,
		instanceId, requestId, sendBuf, timestamp, isLarge
	);
	return bytes;
}

bool Protocol::parseKeyValueHeader( struct KeyValueHeader &header, char *buf, size_t size, size_t offset, bool enableSplit, bool isLarge ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_KEY_VALUE_SIZE ) return false;
	char *ptr = buf + offset;
	uint32_t numOfSplit, splitSize;

	header.keySize   = ProtocolUtil::read1Byte ( ptr );
	header.valueSize = ProtocolUtil::read3Bytes( ptr );
	header.key       = ptr;
	ptr += header.keySize;

	if ( enableSplit && LargeObjectUtil::isLarge( header.keySize, header.valueSize, &numOfSplit, &splitSize ) ) {
		header.splitOffset = ProtocolUtil::read3Bytes( ptr );
		if ( header.splitOffset + splitSize > header.valueSize )
			splitSize = header.valueSize - header.splitOffset;
		if ( size - offset < PROTO_KEY_VALUE_SIZE + PROTO_SPLIT_OFFSET_SIZE + header.keySize + splitSize )
			return false;
	} else {
		header.splitOffset = 0;
		if ( size - offset < PROTO_KEY_VALUE_SIZE + header.keySize + header.valueSize )
			return false;
		if ( isLarge )
			ptr += SPLIT_OFFSET_SIZE;
	}

	header.value = header.valueSize ? ptr : 0;
	return true;
}

size_t Protocol::generateKeyValueUpdateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate, char *sendBuf, uint32_t timestamp ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_KEY_VALUE_UPDATE_SIZE + keySize + ( valueUpdate ? valueUpdateSize : 0 ),
		instanceId, requestId, sendBuf, timestamp
	);
	bytes += ProtocolUtil::write1Byte ( buf, keySize );
	bytes += ProtocolUtil::write3Bytes( buf, valueUpdateSize );
	bytes += ProtocolUtil::write3Bytes( buf, valueUpdateOffset );
	bytes += ProtocolUtil::write( buf, key, keySize );
	if ( valueUpdateSize && valueUpdate )
		bytes += ProtocolUtil::write( buf, valueUpdate, valueUpdateSize );
	return bytes;
}

bool Protocol::parseKeyValueUpdateHeader( struct KeyValueUpdateHeader &header, bool withValueUpdate, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_KEY_VALUE_UPDATE_SIZE ) return false;
	char *ptr = buf + offset;
	header.keySize           = ProtocolUtil::read1Byte ( ptr );
	header.valueUpdateSize   = ProtocolUtil::read3Bytes( ptr );
	header.valueUpdateOffset = ProtocolUtil::read3Bytes( ptr );
	header.key = ptr;
	header.valueUpdate = withValueUpdate && header.valueUpdateSize ? ptr + header.keySize : 0;
	return ( size - offset >= PROTO_KEY_VALUE_UPDATE_SIZE + header.keySize + ( withValueUpdate ? header.valueUpdateSize : 0 ) );
}

size_t Protocol::generateChunkUpdateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId, char *delta, char *sendBuf, uint32_t timestamp ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, delta ? PROTO_CHUNK_UPDATE_SIZE + length : PROTO_CHUNK_UPDATE_SIZE, instanceId, requestId, sendBuf, timestamp );
	bytes += ProtocolUtil::write4Bytes( buf, listId );
	bytes += ProtocolUtil::write4Bytes( buf, stripeId );
	bytes += ProtocolUtil::write4Bytes( buf, chunkId );
	bytes += ProtocolUtil::write4Bytes( buf, offset );
	bytes += ProtocolUtil::write4Bytes( buf, length );
	bytes += ProtocolUtil::write4Bytes( buf, updatingChunkId );
	if ( delta )
		bytes += ProtocolUtil::write( buf, delta, length );
	return bytes;
}

bool Protocol::parseChunkUpdateHeader( struct ChunkUpdateHeader &header, bool withDelta, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_CHUNK_UPDATE_SIZE ) return false;
	char *ptr = buf + offset;
	header.listId          = ProtocolUtil::read4Bytes( ptr );
	header.stripeId        = ProtocolUtil::read4Bytes( ptr );
	header.chunkId         = ProtocolUtil::read4Bytes( ptr );
	header.offset          = ProtocolUtil::read4Bytes( ptr );
	header.length          = ProtocolUtil::read4Bytes( ptr );
	header.updatingChunkId = ProtocolUtil::read4Bytes( ptr );
	header.delta           = withDelta && header.length ? ptr : 0;
	return ( size - offset >= PROTO_CHUNK_UPDATE_SIZE + ( withDelta ? header.length : 0 ) );
}
