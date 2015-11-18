#include "parity_chunk_buffer.hh"
#include "../../common/util/debug.hh"

ParityChunkWrapper::ParityChunkWrapper() {
	this->pending = new bool[ ChunkBuffer::dataChunkCount ];
	LOCK_INIT( &this->lock );
	this->chunk = 0;

	for ( uint32_t i = 0; i < ChunkBuffer::dataChunkCount; i++ )
		this->pending[ i ] = true;
}

uint32_t ParityChunkWrapper::countPending() {
	uint32_t ret = 0;
	for ( uint32_t i = 0; i < ChunkBuffer::dataChunkCount; i++ )
		if ( this->pending[ i ] )
			ret++;
	return ret;
}

void ParityChunkWrapper::free() {
	delete[] this->pending;
}

///////////////////////////////////////////////////////////////////////////////

ParityChunkBuffer::ParityChunkBuffer( uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId ) : ChunkBuffer() {
	this->listId = listId;
	this->stripeId = stripeId;
	this->chunkId = chunkId;
}

ParityChunkWrapper &ParityChunkBuffer::getWrapper( uint32_t stripeId, bool needsLock, bool needsUnlock ) {
	if ( needsLock ) LOCK( &this->lock );
	std::unordered_map<uint32_t, ParityChunkWrapper>::iterator it = this->chunks.find( stripeId );
	if ( it == this->chunks.end() ) {
		ParityChunkWrapper wrapper;
		wrapper.chunk = ChunkBuffer::chunkPool->malloc();
		wrapper.chunk->clear();
		wrapper.chunk->isParity = true;
		wrapper.chunk->metadata.set( this->listId, stripeId, this->chunkId );

		ChunkBuffer::map->setChunk( this->listId, stripeId, this->chunkId, wrapper.chunk, true );

		this->chunks[ stripeId ] = wrapper;
		it = this->chunks.find( stripeId );
	}
	ParityChunkWrapper &wrapper = it->second;
	if ( needsUnlock ) UNLOCK( &this->lock );
	return wrapper;
}

bool ParityChunkBuffer::set( char *keyStr, uint8_t keySize, char *valueStr, uint32_t valueSize, uint32_t chunkId, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk ) {
	Key key;
	std::unordered_map<Key, PendingRequest>::iterator it;

	key.set( keySize, keyStr );

	LOCK( &this->lock );

	// Check whether the key is in a sealed chunk
	it = this->pending.find( key );
	if ( it == this->pending.end() ) {
		// Store the key-value pair in a temporary buffer
		KeyValue keyValue;
		std::pair<std::unordered_map<Key, KeyValue>::iterator, bool> ret;

		keyValue.dup( keyStr, keySize, valueStr, valueSize );
		keyValue.deserialize( keyStr, keySize, valueStr, valueSize );

		key.set( keySize, keyStr );

		std::pair<Key, KeyValue> p( key, keyValue );

		ret = this->keys.insert( p );
		if ( ! ret.second ) {
			UNLOCK( &this->lock );
			keyValue.free();
			return false;
		}
	} else {
		// Prepare data delta
		char *data = dataChunk->getData();
		dataChunk->clear();

		key = it->first;
		PendingRequest pendingRequest = it->second;

		this->pending.erase( it );
		key.free();

		switch ( pendingRequest.type ) {
			case PRT_SEAL:
				// fprintf( stderr, "--- PRT_SEAL: Key = %.*s ---\n", keySize, keyStr );
				KeyValue::serialize( data + pendingRequest.req.seal.offset, keyStr, keySize, valueStr, valueSize );
				dataChunk->setSize( pendingRequest.req.seal.offset + KEY_VALUE_METADATA_SIZE + keySize + valueSize );

				// Update parity chunk
				this->update(
					pendingRequest.req.seal.stripeId, chunkId,
					pendingRequest.req.seal.offset,
					KEY_VALUE_METADATA_SIZE + keySize + valueSize,
					dataChunks, dataChunk, parityChunk,
					false, false
				);
				break;
			case PRT_UPDATE:
				fprintf( stderr, "--- TODO: PRT_UPDATE: Key = %.*s ---\n", keySize, keyStr );
				break;
			case PRT_DELETE:
				fprintf( stderr, "--- TODO: PRT_DELETE: Key = %.*s ---\n", keySize, keyStr );
				break;
		}
	}
	UNLOCK( &this->lock );

	return true;
}

bool ParityChunkBuffer::seal( uint32_t stripeId, uint32_t chunkId, uint32_t count, char *sealData, size_t sealDataSize, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk ) {
	char *data = dataChunk->getData();
	dataChunk->clear();

	uint8_t keySize;
	uint32_t valueSize, offset, curPos = 0, numOfKeys = 0;
	char *keyStr, *valueStr;
	Key key;
	KeyValue keyValue;
	std::unordered_map<Key, KeyValue>::iterator it;
	std::unordered_map<Key, PendingRequest>::iterator prtIt;

	LOCK( &this->lock );
	while ( sealDataSize ) {
		// Parse the (key, offset) record
		keySize = sealData[ 0 ];
		offset = ntohl( *( ( uint32_t * )( sealData + 1 ) ) );
		keyStr = sealData + PROTO_CHUNK_SEAL_DATA_SIZE;

		// Find the key-value pair from the temporary buffer
		key.set( keySize, keyStr );
		it = this->keys.find( key );

		prtIt = this->pending.find( key );
		if ( prtIt != this->pending.end() ) {
			printf(
				"prtIt != this->pending.end(); (%u, %u, %u, %u) vs. (%u, %u)\n",
				this->listId,
				stripeId,
				chunkId,
				offset,
				prtIt->second.req.seal.stripeId,
				prtIt->second.req.seal.offset
			);
		}

		if ( it == this->keys.end() ) {
			// Defer the processing of this key
			key.dup();
			PendingRequest pendingRequest;
			pendingRequest.seal( stripeId, offset );

			std::pair<Key, PendingRequest> p( key, pendingRequest );
			std::pair<std::unordered_map<Key, PendingRequest>::iterator, bool> ret;

			ret = this->pending.insert( p );
			if ( ! ret.second ) {
				__ERROR__( "ParityChunkBuffer", "seal", "Key: %.*s (size = %u) cannot be inserted into pending keys map.\n", keySize, keyStr, keySize );
			}
		} else {
			keyValue = it->second;

			// Get the value size
			keyValue.deserialize( keyStr, keySize, valueStr, valueSize );

			// Copy the key-value pair to the temporary data chunk
			memcpy( data + offset, keyValue.data, KEY_VALUE_METADATA_SIZE + keySize + valueSize );

			// Release memory
			this->keys.erase( it );
			keyValue.free();

			curPos = offset + KEY_VALUE_METADATA_SIZE + keySize + valueSize;
		}

		// Update counter
		sealData += PROTO_CHUNK_SEAL_DATA_SIZE + keySize;
		sealDataSize -= PROTO_CHUNK_SEAL_DATA_SIZE + keySize;
		numOfKeys++;
	}
	assert( numOfKeys == count );
	dataChunk->setSize( curPos );
	this->update( stripeId, chunkId, 0, curPos, dataChunks, dataChunk, parityChunk, false, false, true );
	UNLOCK( &this->lock );

	return true;
}

bool ParityChunkBuffer::findValueByKey( char *data, uint8_t size, KeyValue *keyValuePtr, Key *keyPtr ) {
	std::unordered_map<Key, KeyValue>::iterator it;
	Key key;

	if ( keyValuePtr )
		keyValuePtr->clear();
	key.set( size, data );

	LOCK( &this->lock );
	it = this->keys.find( key );
	if ( it == this->keys.end() ) {
		UNLOCK( &this->lock );
		if ( keyPtr ) *keyPtr = key;
		// printf( "findValueByKey() failed: %.*s\n", size, data );
		return false;
	}

	if ( keyPtr ) *keyPtr = it->first;
	if ( keyValuePtr ) *keyValuePtr = it->second;

	UNLOCK( &this->lock );
	return true;
}

bool ParityChunkBuffer::deleteKey( char *keyStr, uint8_t keySize ) {
	std::unordered_map<Key, KeyValue>::iterator it;
	Key key;

	key.set( keySize, keyStr );

	LOCK( &this->lock );

	it = this->keys.find( key );
	if ( it == this->keys.end() ) {
		PendingRequest pendingRequest;
		pendingRequest.del();

		key.dup();

		std::pair<Key, PendingRequest> p( key, pendingRequest );
		std::pair<std::unordered_map<Key, PendingRequest>::iterator, bool> ret;

		ret = this->pending.insert( p );
		if ( ! ret.second ) {
			__ERROR__( "ParityChunkBuffer", "deleteKey", "Key: %.*s (size = %u) cannot be inserted into pending keys map.\n", keySize, keyStr, keySize );
		}
		UNLOCK( &this->lock );
		return false;
	} else {
		KeyValue keyValue = it->second;
		this->keys.erase( it );
		keyValue.free();
	}

	UNLOCK( &this->lock );
	return true;
}

bool ParityChunkBuffer::updateKeyValue( char *keyStr, uint8_t keySize, uint32_t offset, uint32_t length, char *valueUpdate ) {
	std::unordered_map<Key, KeyValue>::iterator it;
	Key key;

	key.set( keySize, keyStr );

	LOCK( &this->lock );
	it = this->keys.find( key );
	if ( it == this->keys.end() ) {
		PendingRequest pendingRequest;
		pendingRequest.update( offset, length, valueUpdate );

		key.dup();

		std::pair<Key, PendingRequest> p( key, pendingRequest );
		std::pair<std::unordered_map<Key, PendingRequest>::iterator, bool> ret;

		ret = this->pending.insert( p );
		if ( ! ret.second ) {
			__ERROR__( "ParityChunkBuffer", "updateKeyValue", "Key: %.*s (size = %u) cannot be inserted into pending keys map.\n", keySize, keyStr, keySize );
		}
		UNLOCK( &this->lock );
		return false;
	} else {
		KeyValue keyValue = it->second;
		char *dst = keyValue.data + PROTO_KEY_VALUE_SIZE + keySize + offset;
		Coding::bitwiseXOR(
			dst,
			dst,
			valueUpdate,
			length
		);
	}
	UNLOCK( &this->lock );
	return true;
}

void ParityChunkBuffer::update( uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t size, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk, bool needsLock, bool needsUnlock, bool isSeal, bool isDelete ) {
	// Prepare the stripe
	for ( uint32_t i = 0; i < ChunkBuffer::dataChunkCount; i++ )
		dataChunks[ i ] = Coding::zeros;
	dataChunks[ chunkId ] = dataChunk;

	parityChunk->clear();

	// Compute parity delta
	ChunkBuffer::coding->encode(
		dataChunks, parityChunk, this->chunkId - ChunkBuffer::dataChunkCount + 1,
		offset + chunkId * ChunkBuffer::capacity,
		offset + chunkId * ChunkBuffer::capacity + size
	);

	if ( needsLock ) LOCK( &this->lock );
	ParityChunkWrapper &wrapper = this->getWrapper( stripeId, false, false );

	LOCK( &wrapper.lock );
	wrapper.chunk->status = CHUNK_STATUS_DIRTY;
	// Update the parity chunk
	char *parity = wrapper.chunk->getData();
	Coding::bitwiseXOR(
		parity,
		parity,
		parityChunk->getData(),
		ChunkBuffer::capacity
	);
	if ( isSeal )
		wrapper.pending[ chunkId ] = false;
	else if ( isDelete )
		wrapper.pending[ chunkId ] = true;
	UNLOCK( &wrapper.lock );
	if ( needsUnlock ) UNLOCK( &this->lock );
}

void ParityChunkBuffer::update( uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t size, char *dataDelta, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk, bool isDelete ) {
	// Prepare data delta
	dataChunk->clear();
	dataChunk->setSize( offset + size );
	memcpy( dataChunk->getData() + offset, dataDelta, size );
	this->update( stripeId, chunkId, offset, size, dataChunks, dataChunk, parityChunk, true, true, false, true );
}

void ParityChunkBuffer::print( FILE *f ) {
	int width = 16;
	int numPending = 0;
	double occupied;

	fprintf(
		f,
		"- %-*s : %s\n"
		"- %-*s : %u\n"
		"- %-*s :\n",
		width, "Role", "Dummy Data chunk buffer",
		width, "Chunk size", ChunkBuffer::capacity,
		width, "Statistics (occupied / total)"
	);
	for (
		std::unordered_map<uint32_t, ParityChunkWrapper>::iterator it = this->chunks.begin();
		it != this->chunks.end();
		it++
	) {
		ParityChunkWrapper &wrapper = it->second;
		uint32_t pending = wrapper.countPending();
		if ( pending ) {
			numPending++;
			occupied = ( double ) wrapper.chunk->getSize() / ChunkBuffer::capacity * 100.0;
			fprintf(
				f,
				"\t%u. [#%u] %u / %u (%5.2lf%%) (pending: %u)\n",
				numPending, wrapper.chunk->metadata.stripeId, wrapper.chunk->getSize(), ChunkBuffer::capacity, occupied,
				pending
			);
			// The pending number does not necessarily equal to the number of data chunks in the dummy data chunk buffer as they may not have been allocated!
		}
	}
}

void ParityChunkBuffer::stop() {}
