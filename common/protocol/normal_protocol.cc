#include "protocol.hh"

size_t Protocol::generateKeyHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint8_t keySize, char *key, char *sendBuf, uint32_t timestamp ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_KEY_SIZE + keySize, instanceId, requestId, sendBuf, timestamp );

	buf[ 0 ] = keySize;

	buf += PROTO_KEY_SIZE;
	memmove( buf, key, keySize );

	bytes += PROTO_KEY_SIZE + keySize;

	return bytes;
}

bool Protocol::parseKeyHeader( size_t offset, uint8_t &keySize, char *&key, char *buf, size_t size ) {
	if ( size - offset < PROTO_KEY_SIZE )
		return false;

	char *ptr = buf + offset;
	keySize = ( uint8_t ) ptr[ 0 ];

	if ( size - offset < ( size_t ) PROTO_KEY_SIZE + keySize )
		return false;

	key = ptr + PROTO_KEY_SIZE;

	return true;
}

bool Protocol::parseKeyHeader( struct KeyHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseKeyHeader(
		offset,
		header.keySize,
		header.key,
		buf, size
	);
}

size_t Protocol::generateKeyBackupHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t sealedListId, uint32_t sealedStripeId, uint32_t sealedChunkId, uint8_t keySize, char *key, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_KEY_BACKUP_BASE_SIZE + PROTO_KEY_BACKUP_FOR_DATA_SIZE + PROTO_KEY_BACKUP_SEALED_SIZE + keySize,
		instanceId, requestId, sendBuf
	);

	buf[ 0 ] = false; // isParity
	buf[ 1 ] = keySize;
	buf += PROTO_KEY_BACKUP_BASE_SIZE;
	bytes += PROTO_KEY_BACKUP_BASE_SIZE;

	*( ( uint32_t * )( buf      ) ) = htonl( timestamp );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( chunkId );
	buf[ 16 ] = true; // isSealed
	buf += PROTO_KEY_BACKUP_FOR_DATA_SIZE;
	bytes += PROTO_KEY_BACKUP_FOR_DATA_SIZE;

	*( ( uint32_t * )( buf      ) ) = htonl( sealedListId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( sealedStripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( sealedChunkId );
	buf += PROTO_KEY_BACKUP_SEALED_SIZE;
	bytes += PROTO_KEY_BACKUP_SEALED_SIZE;

	memmove( buf, key, keySize );
	bytes += keySize;

	return bytes;
}

size_t Protocol::generateKeyBackupHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint8_t keySize, char *key, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_KEY_BACKUP_BASE_SIZE + PROTO_KEY_BACKUP_FOR_DATA_SIZE + keySize,
		instanceId, requestId, sendBuf
	);

	buf[ 0 ] = false; // isParity
	buf[ 1 ] = keySize;
	buf += PROTO_KEY_BACKUP_BASE_SIZE;
	bytes += PROTO_KEY_BACKUP_BASE_SIZE;

	*( ( uint32_t * )( buf      ) ) = htonl( timestamp );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( chunkId );
	buf[ 16 ] = false; // isSealed
	buf += PROTO_KEY_BACKUP_FOR_DATA_SIZE;
	bytes += PROTO_KEY_BACKUP_FOR_DATA_SIZE;

	memmove( buf, key, keySize );
	bytes += keySize;

	return bytes;
}

size_t Protocol::generateKeyBackupHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint8_t keySize, char *key, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_KEY_BACKUP_BASE_SIZE + keySize, instanceId, requestId, sendBuf );

	buf[ 0 ] = true; // isParity
	buf[ 1 ] = keySize;

	buf += PROTO_KEY_BACKUP_BASE_SIZE;
	memmove( buf, key, keySize );

	bytes += PROTO_KEY_BACKUP_BASE_SIZE + keySize;

	return bytes;
}

bool Protocol::parseKeyBackupHeader( size_t offset, bool &isParity, uint8_t &keySize, uint32_t &timestamp, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, bool &isSealed, uint32_t &sealedListId, uint32_t &sealedStripeId, uint32_t &sealedChunkId, char *&key, char *buf, size_t size ) {
	if ( size - offset < PROTO_KEY_BACKUP_BASE_SIZE )
		return false;

	char *ptr = buf + offset;
	isParity = ptr[ 0 ];
	keySize  = ptr[ 1 ];
	ptr += PROTO_KEY_BACKUP_BASE_SIZE;

	if ( isParity ) {
		if ( size - offset < ( size_t ) PROTO_KEY_BACKUP_BASE_SIZE + keySize )
			return false;
		key = ptr;
	} else {
		if ( size - offset < ( size_t ) PROTO_KEY_BACKUP_BASE_SIZE + PROTO_KEY_BACKUP_FOR_DATA_SIZE + keySize )
			return false;

		timestamp = ntohl( *( ( uint32_t * )( ptr      ) ) );
		listId    = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
		stripeId  = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
		chunkId   = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
		isSealed  = ptr[ 16 ];

		ptr += PROTO_KEY_BACKUP_FOR_DATA_SIZE;

		if ( isSealed ) {
			sealedListId   = ntohl( *( ( uint32_t * )( ptr      ) ) );
			sealedStripeId = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
			sealedChunkId  = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );

			if ( size - offset < ( size_t ) PROTO_KEY_BACKUP_BASE_SIZE + PROTO_KEY_BACKUP_FOR_DATA_SIZE + PROTO_KEY_BACKUP_SEALED_SIZE + keySize )
				return false;

			ptr += PROTO_KEY_BACKUP_SEALED_SIZE;
		}

		key = ptr;
	}

	return true;
}

bool Protocol::parseKeyBackupHeader( struct KeyBackupHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseKeyBackupHeader(
		offset,
		header.isParity,
		header.keySize,
		header.timestamp,
		header.listId,
		header.stripeId,
		header.chunkId,
		header.isSealed,
		header.sealedListId,
		header.sealedStripeId,
		header.sealedChunkId,
		header.key,
		buf, size
	);
}

size_t Protocol::generateChunkKeyHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint8_t keySize, char *key, char *sendBuf, uint32_t timestamp ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_KEY_SIZE + keySize, instanceId, requestId, sendBuf, timestamp );

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf + 8 ) ) = htonl( chunkId );
	buf[ 12 ] = keySize;

	buf += PROTO_CHUNK_KEY_SIZE;
	memmove( buf, key, keySize );

	bytes += PROTO_CHUNK_KEY_SIZE + keySize;

	return bytes;
}

bool Protocol::parseChunkKeyHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint8_t &keySize, char *&key, char *buf, size_t size ) {
	if ( size - offset < PROTO_CHUNK_KEY_SIZE )
		return false;

	char *ptr = buf + offset;

	listId   = ntohl( *( ( uint32_t * )( ptr     ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );
	keySize = ( uint8_t ) ptr[ 12 ];

	if ( size - offset < ( size_t ) PROTO_CHUNK_KEY_SIZE + keySize )
		return false;

	key = ptr + PROTO_CHUNK_KEY_SIZE;

	return true;
}

bool Protocol::parseChunkKeyHeader( struct ChunkKeyHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseChunkKeyHeader(
		offset,
		header.listId,
		header.stripeId,
		header.chunkId,
		header.keySize,
		header.key,
		buf, size
	);
}

size_t Protocol::generateChunkKeyValueUpdateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, uint32_t chunkUpdateOffset, char *valueUpdate, char *sendBuf, uint32_t timestamp ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE + keySize + ( valueUpdate ? valueUpdateSize : 0 ), instanceId, requestId, sendBuf, timestamp );

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf + 8 ) ) = htonl( chunkId );
	buf += 12;

	buf[ 0 ] = keySize;

	unsigned char *tmp;
	valueUpdateSize = htonl( valueUpdateSize );
	valueUpdateOffset = htonl( valueUpdateOffset );
	chunkUpdateOffset = htonl( chunkUpdateOffset );
	tmp = ( unsigned char * ) &valueUpdateSize;
	buf[ 1 ] = tmp[ 1 ];
	buf[ 2 ] = tmp[ 2 ];
	buf[ 3 ] = tmp[ 3 ];
	tmp = ( unsigned char * ) &valueUpdateOffset;
	buf[ 4 ] = tmp[ 1 ];
	buf[ 5 ] = tmp[ 2 ];
	buf[ 6 ] = tmp[ 3 ];
	tmp = ( unsigned char * ) &chunkUpdateOffset;
	buf[ 7 ] = tmp[ 1 ];
	buf[ 8 ] = tmp[ 2 ];
	buf[ 9 ] = tmp[ 3 ];
	valueUpdateSize = ntohl( valueUpdateSize );
	valueUpdateOffset = ntohl( valueUpdateOffset );
	chunkUpdateOffset = ntohl( chunkUpdateOffset );

	buf += 10;
	memmove( buf, key, keySize );
	buf += keySize;
	if ( valueUpdateSize && valueUpdate ) {
		memmove( buf, valueUpdate, valueUpdateSize );
		bytes += PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize;
	} else {
		bytes += PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE + keySize;
	}

	return bytes;
}

bool Protocol::parseChunkKeyValueUpdateHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint8_t &keySize, char *&key, uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize, uint32_t &chunkUpdateOffset, char *buf, size_t size ) {
	if ( size - offset < PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE )
		return false;

	char *ptr = buf + offset;
	unsigned char *tmp;

	listId   = ntohl( *( ( uint32_t * )( ptr     ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );
	ptr += 12;

	keySize = ( uint8_t ) ptr[ 0 ];

	valueUpdateSize = 0;
	tmp = ( unsigned char * ) &valueUpdateSize;
	tmp[ 1 ] = ptr[ 1 ];
	tmp[ 2 ] = ptr[ 2 ];
	tmp[ 3 ] = ptr[ 3 ];
	valueUpdateSize = ntohl( valueUpdateSize );

	valueUpdateOffset = 0;
	tmp = ( unsigned char * ) &valueUpdateOffset;
	tmp[ 1 ] = ptr[ 4 ];
	tmp[ 2 ] = ptr[ 5 ];
	tmp[ 3 ] = ptr[ 6 ];
	valueUpdateOffset = ntohl( valueUpdateOffset );

	chunkUpdateOffset = 0;
	tmp = ( unsigned char * ) &chunkUpdateOffset;
	tmp[ 1 ] = ptr[ 7 ];
	tmp[ 2 ] = ptr[ 8 ];
	tmp[ 3 ] = ptr[ 9 ];
	chunkUpdateOffset = ntohl( chunkUpdateOffset );

	key = ptr + 10;

	return true;
}

bool Protocol::parseChunkKeyValueUpdateHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint8_t &keySize, char *&key, uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize, uint32_t &chunkUpdateOffset, char *&valueUpdate, char *buf, size_t size ) {
	if ( size - offset < PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE )
		return false;

	char *ptr = buf + offset;
	unsigned char *tmp;

	listId   = ntohl( *( ( uint32_t * )( ptr     ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );
	ptr += 12;

	keySize = ( uint8_t ) ptr[ 0 ];

	valueUpdateSize = 0;
	tmp = ( unsigned char * ) &valueUpdateSize;
	tmp[ 1 ] = ptr[ 1 ];
	tmp[ 2 ] = ptr[ 2 ];
	tmp[ 3 ] = ptr[ 3 ];
	valueUpdateSize = ntohl( valueUpdateSize );

	valueUpdateOffset = 0;
	tmp = ( unsigned char * ) &valueUpdateOffset;
	tmp[ 1 ] = ptr[ 4 ];
	tmp[ 2 ] = ptr[ 5 ];
	tmp[ 3 ] = ptr[ 6 ];
	valueUpdateOffset = ntohl( valueUpdateOffset );

	chunkUpdateOffset = 0;
	tmp = ( unsigned char * ) &chunkUpdateOffset;
	tmp[ 1 ] = ptr[ 7 ];
	tmp[ 2 ] = ptr[ 8 ];
	tmp[ 3 ] = ptr[ 9 ];
	chunkUpdateOffset = ntohl( chunkUpdateOffset );

	if ( size - offset < PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize )
		return false;

	key = ptr + 10;
	valueUpdate = valueUpdateSize ? key + keySize : 0;

	return true;
}

bool Protocol::parseChunkKeyValueUpdateHeader( struct ChunkKeyValueUpdateHeader &header, bool withValueUpdate, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( withValueUpdate ) {
		return this->parseChunkKeyValueUpdateHeader(
			offset,
			header.listId,
			header.stripeId,
			header.chunkId,
			header.keySize,
			header.key,
			header.valueUpdateOffset,
			header.valueUpdateSize,
			header.chunkUpdateOffset,
			header.valueUpdate,
			buf, size
		);
	} else {
		header.valueUpdate = 0;
		return this->parseChunkKeyValueUpdateHeader(
			offset,
			header.listId,
			header.stripeId,
			header.chunkId,
			header.keySize,
			header.key,
			header.valueUpdateOffset,
			header.valueUpdateSize,
			header.chunkUpdateOffset,
			buf, size
		);
	}
}

size_t Protocol::generateKeyValueHeader(
	uint8_t magic, uint8_t to, uint8_t opcode,
	uint16_t instanceId, uint32_t requestId,
	uint8_t keySize, char *key,
	uint32_t valueSize, char *value,
	char *sendBuf, uint32_t timestamp,
	uint32_t splitOffset, uint32_t splitSize
) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = 0;

	buf[ 0 ] = keySize;

	valueSize = htonl( valueSize );
	unsigned char *tmp = ( unsigned char * ) &valueSize;
	buf[ 1 ] = tmp[ 1 ];
	buf[ 2 ] = tmp[ 2 ];
	buf[ 3 ] = tmp[ 3 ];
	valueSize = ntohl( valueSize );
	buf += PROTO_KEY_VALUE_SIZE;

	if ( splitSize == 0 || splitSize == valueSize ) {
		// No need to split
		memmove( buf, key, keySize );
		buf += keySize;
		if ( valueSize )
			memmove( buf, value, valueSize );
		bytes += PROTO_KEY_VALUE_SIZE + keySize + valueSize;
	} else {
		// Include split offset
		splitOffset = htonl( splitOffset );
		unsigned char *tmp = ( unsigned char * ) &splitOffset;
		buf[ 0 ] = tmp[ 1 ];
		buf[ 1 ] = tmp[ 2 ];
		buf[ 2 ] = tmp[ 3 ];
		splitOffset = ntohl( splitOffset );
		buf += PROTO_SPLIT_OFFSET_SIZE;

		memmove( buf, key, keySize );
		buf += keySize;

		if ( splitOffset + splitSize > valueSize )
			splitSize = valueSize - splitOffset;

		if ( valueSize )
			memmove( buf, value + splitOffset, splitSize );
		bytes += PROTO_KEY_VALUE_SIZE + PROTO_SPLIT_OFFSET_SIZE + keySize + splitSize;
	}

	bytes += this->generateHeader(
		magic, to, opcode,
		bytes,
		instanceId, requestId, sendBuf, timestamp
	);

	return bytes;
}

bool Protocol::parseKeyValueHeader( size_t offset, uint8_t &keySize, char *&key, uint32_t &valueSize, char *&value, uint32_t &splitOffset, char *buf, size_t size, bool enableSplit ) {
	if ( size - offset < PROTO_KEY_VALUE_SIZE )
		return false;

	char *ptr = buf + offset;
	unsigned char *tmp;
	uint32_t numOfSplit, splitSize;

	keySize = ( uint8_t ) ptr[ 0 ];
	valueSize = 0;
	tmp = ( unsigned char * ) &valueSize;
	tmp[ 1 ] = ptr[ 1 ];
	tmp[ 2 ] = ptr[ 2 ];
	tmp[ 3 ] = ptr[ 3 ];
	valueSize = ntohl( valueSize );

	ptr += PROTO_KEY_VALUE_SIZE;

	if ( enableSplit && LargeObjectUtil::isLarge( keySize, valueSize, &numOfSplit, &splitSize ) ) {
		splitOffset = 0;
		tmp = ( unsigned char * ) &splitOffset;
		tmp[ 1 ] = ptr[ 0 ];
		tmp[ 2 ] = ptr[ 1 ];
		tmp[ 3 ] = ptr[ 2 ];
		splitOffset = ntohl( splitOffset );
		ptr += PROTO_SPLIT_OFFSET_SIZE;

		if ( splitOffset + splitSize > valueSize )
			splitSize = valueSize - splitOffset;

		if ( size - offset < PROTO_KEY_VALUE_SIZE + PROTO_SPLIT_OFFSET_SIZE + keySize + splitSize )
			return false;
	} else {
		splitOffset = 0;

		if ( size - offset < PROTO_KEY_VALUE_SIZE + keySize + valueSize )
			return false;
	}

	key = ptr;
	value = valueSize ? key + keySize : 0;

	return true;
}

bool Protocol::parseKeyValueHeader( struct KeyValueHeader &header, char *buf, size_t size, size_t offset, bool enableSplit ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseKeyValueHeader(
		offset,
		header.keySize,
		header.key,
		header.valueSize,
		header.value,
		header.splitOffset,
		buf, size,
		enableSplit
	);
}

size_t Protocol::generateKeyValueUpdateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate, char *sendBuf, uint32_t timestamp ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_KEY_VALUE_UPDATE_SIZE + keySize + ( valueUpdate ? valueUpdateSize : 0 ),
		instanceId, requestId, sendBuf, timestamp
	);

	buf[ 0 ] = keySize;

	unsigned char *tmp;
	valueUpdateSize = htonl( valueUpdateSize );
	valueUpdateOffset = htonl( valueUpdateOffset );
	tmp = ( unsigned char * ) &valueUpdateSize;
	buf[ 1 ] = tmp[ 1 ];
	buf[ 2 ] = tmp[ 2 ];
	buf[ 3 ] = tmp[ 3 ];
	tmp = ( unsigned char * ) &valueUpdateOffset;
	buf[ 4 ] = tmp[ 1 ];
	buf[ 5 ] = tmp[ 2 ];
	buf[ 6 ] = tmp[ 3 ];
	valueUpdateSize = ntohl( valueUpdateSize );
	valueUpdateOffset = ntohl( valueUpdateOffset );

	buf += PROTO_KEY_VALUE_UPDATE_SIZE;
	memmove( buf, key, keySize );
	buf += keySize;
	if ( valueUpdateSize && valueUpdate ) {
		memmove( buf, valueUpdate, valueUpdateSize );
		bytes += PROTO_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize;
	} else {
		bytes += PROTO_KEY_VALUE_UPDATE_SIZE + keySize;
	}

	return bytes;
}

bool Protocol::parseKeyValueUpdateHeader( size_t offset, uint8_t &keySize, char *&key, uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize, char *buf, size_t size ) {
	if ( size - offset < PROTO_KEY_VALUE_UPDATE_SIZE )
		return false;

	char *ptr = buf + offset;
	unsigned char *tmp;
	keySize = ( uint8_t ) ptr[ 0 ];

	valueUpdateSize = 0;
	tmp = ( unsigned char * ) &valueUpdateSize;
	tmp[ 1 ] = ptr[ 1 ];
	tmp[ 2 ] = ptr[ 2 ];
	tmp[ 3 ] = ptr[ 3 ];
	valueUpdateSize = ntohl( valueUpdateSize );

	valueUpdateOffset = 0;
	tmp = ( unsigned char * ) &valueUpdateOffset;
	tmp[ 1 ] = ptr[ 4 ];
	tmp[ 2 ] = ptr[ 5 ];
	tmp[ 3 ] = ptr[ 6 ];
	valueUpdateOffset = ntohl( valueUpdateOffset );

	key = ptr + PROTO_KEY_VALUE_UPDATE_SIZE;

	return true;
}

bool Protocol::parseKeyValueUpdateHeader( size_t offset, uint8_t &keySize, char *&key, uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize, char *&valueUpdate, char *buf, size_t size ) {
	if ( size - offset < PROTO_KEY_VALUE_UPDATE_SIZE )
		return false;

	char *ptr = buf + offset;
	unsigned char *tmp;
	keySize = ( uint8_t ) ptr[ 0 ];

	valueUpdateSize = 0;
	tmp = ( unsigned char * ) &valueUpdateSize;
	tmp[ 1 ] = ptr[ 1 ];
	tmp[ 2 ] = ptr[ 2 ];
	tmp[ 3 ] = ptr[ 3 ];
	valueUpdateSize = ntohl( valueUpdateSize );

	valueUpdateOffset = 0;
	tmp = ( unsigned char * ) &valueUpdateOffset;
	tmp[ 1 ] = ptr[ 4 ];
	tmp[ 2 ] = ptr[ 5 ];
	tmp[ 3 ] = ptr[ 6 ];
	valueUpdateOffset = ntohl( valueUpdateOffset );

	if ( size - offset < PROTO_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize )
		return false;

	key = ptr + PROTO_KEY_VALUE_UPDATE_SIZE;
	valueUpdate = valueUpdateSize ? key + keySize : 0;

	return true;
}

bool Protocol::parseKeyValueUpdateHeader( struct KeyValueUpdateHeader &header, bool withValueUpdate, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( withValueUpdate ) {
		return this->parseKeyValueUpdateHeader(
			offset,
			header.keySize,
			header.key,
			header.valueUpdateOffset,
			header.valueUpdateSize,
			header.valueUpdate,
			buf, size
		);
	} else {
		header.valueUpdate = 0;
		return this->parseKeyValueUpdateHeader(
			offset,
			header.keySize,
			header.key,
			header.valueUpdateOffset,
			header.valueUpdateSize,
			buf, size
		);
	}
}

size_t Protocol::generateChunkUpdateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId, char *delta, char *sendBuf, uint32_t timestamp ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, delta ? PROTO_CHUNK_UPDATE_SIZE + length : PROTO_CHUNK_UPDATE_SIZE, instanceId, requestId, sendBuf, timestamp );

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( chunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( offset );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( length );
	*( ( uint32_t * )( buf + 20 ) ) = htonl( updatingChunkId );

	buf += PROTO_CHUNK_UPDATE_SIZE;

	if ( delta ) {
		memmove( buf, delta, length );
		bytes += PROTO_CHUNK_UPDATE_SIZE + length;
	} else {
		bytes += PROTO_CHUNK_UPDATE_SIZE;
	}

	return bytes;
}

bool Protocol::parseChunkUpdateHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint32_t &updateOffset, uint32_t &updateLength, uint32_t &updatingChunkId, char *buf, size_t size ) {
	if ( size - offset < PROTO_CHUNK_UPDATE_SIZE )
		return false;

	char *ptr = buf + offset;
	listId          = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId        = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	chunkId         = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	updateOffset    = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	updateLength    = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );
	updatingChunkId = ntohl( *( ( uint32_t * )( ptr + 20 ) ) );

	return true;
}

bool Protocol::parseChunkUpdateHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint32_t &updateOffset, uint32_t &updateLength, uint32_t &updatingChunkId, char *&delta, char *buf, size_t size ) {
	if ( size - offset < PROTO_CHUNK_UPDATE_SIZE )
		return false;

	char *ptr = buf + offset;
	listId          = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId        = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	chunkId         = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	updateOffset    = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	updateLength    = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );
	updatingChunkId = ntohl( *( ( uint32_t * )( ptr + 20 ) ) );

	if ( size - offset < PROTO_CHUNK_UPDATE_SIZE + updateLength )
		return false;

	delta = updateLength ? ptr + PROTO_CHUNK_UPDATE_SIZE : 0;

	return true;
}

bool Protocol::parseChunkUpdateHeader( struct ChunkUpdateHeader &header, bool withDelta, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( withDelta ) {
		return this->parseChunkUpdateHeader(
			offset,
			header.listId,
			header.stripeId,
			header.chunkId,
			header.offset,
			header.length,
			header.updatingChunkId,
			header.delta,
			buf, size
		);
	} else {
		header.delta = 0;
		return this->parseChunkUpdateHeader(
			offset,
			header.listId,
			header.stripeId,
			header.chunkId,
			header.offset,
			header.length,
			header.updatingChunkId,
			buf, size
		);
	}
}
