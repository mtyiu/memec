#include "parity_chunk_buffer.hh"
#include "../../common/util/debug.hh"

ParityChunkWrapper::ParityChunkWrapper() {
	this->pending = ChunkBuffer::dataChunkCount;
	pthread_mutex_init( &this->lock, 0 );
	this->chunk = 0;
}

///////////////////////////////////////////////////////////////////////////////

ParityChunkBuffer::ParityChunkBuffer( uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId ) : ChunkBuffer( listId, stripeId, chunkId ) {}

ParityChunkWrapper &ParityChunkBuffer::getWrapper( uint32_t stripeId, bool needsLock, bool needsUnlock ) {
	if ( needsLock ) pthread_mutex_lock( &this->lock );
	std::map<uint32_t, ParityChunkWrapper>::iterator it = this->chunks.find( stripeId );
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
	if ( needsUnlock ) pthread_mutex_unlock( &this->lock );
	return wrapper;
}

bool ParityChunkBuffer::set( char *keyStr, uint8_t keySize, char *valueStr, uint32_t valueSize, uint32_t chunkId, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk ) {
	Key key;
	std::map<Key, KeyValueOffset>::iterator it;

	key.set( keySize, keyStr );

	pthread_mutex_lock( &this->lock );

	// Check whether the key is in a sealed chunk
	it = this->pending.find( key );
	if ( it == this->pending.end() ) {
		// Store the key-value pair in a temporary buffer
		KeyValue keyValue;
		std::pair<std::map<Key, KeyValue>::iterator, bool> ret;

		keyValue.dup( keyStr, keySize, valueStr, valueSize );
		keyValue.deserialize( keyStr, keySize, valueStr, valueSize );

		key.set( keySize, keyStr );

		std::pair<Key, KeyValue> p( key, keyValue );

		ret = this->keys.insert( p );
		if ( ! ret.second ) {
			pthread_mutex_unlock( &this->lock );
			keyValue.free();
			return false;
		}
	} else {
		// Prepare data delta
		char *data = dataChunk->getData();
		dataChunk->clear();

		key = it->first;
		KeyValueOffset keyValueOffset = it->second;

		this->pending.erase( it );
		key.free();

		KeyValue::serialize( data + keyValueOffset.offset, keyStr, keySize, valueStr, valueSize );
		dataChunk->setSize( keyValueOffset.offset + KEY_VALUE_METADATA_SIZE + keySize + valueSize );

		// Update parity chunk
		this->update(
			keyValueOffset.stripeId, chunkId,
			keyValueOffset.offset,
			KEY_VALUE_METADATA_SIZE + keySize + valueSize,
			dataChunks, dataChunk, parityChunk,
			false, false
		);
	}
	pthread_mutex_unlock( &this->lock );

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
	std::map<Key, KeyValue>::iterator it;

	pthread_mutex_lock( &this->lock );
	while ( sealDataSize ) {
		// Parse the (key, offset) record
		keySize = sealData[ 0 ];
		offset = ntohl( *( ( uint32_t * )( sealData + 1 ) ) );
		keyStr = sealData + PROTO_CHUNK_SEAL_DATA_SIZE;

		// Find the key-value pair from the temporary buffer
		key.set( keySize, keyStr );
		it = this->keys.find( key );
		if ( it == this->keys.end() ) {
			// Defer the processing of this key
			key.dup();
			KeyValueOffset keyValueOffset;
			keyValueOffset.stripeId = stripeId;
			keyValueOffset.offset = offset;

			std::pair<Key, KeyValueOffset> p( key, keyValueOffset );
			std::pair<std::map<Key, KeyValueOffset>::iterator, bool> ret;

			ret = this->pending.insert( p );
			if ( ! ret.second ) {
				__ERROR__( "ParityChunkBuffer", "seal", "Key: %.*s (size = %u) cannot be inserted into pending keys map.\n", keySize, keyStr, keySize );
			} /* else {
				__ERROR__( "ParityChunkBuffer", "seal", "Key: %.*s (size = %u) at offset %u is not yet received (list ID = %u, stripe ID = %u, chunk ID = %u).\n", keySize, keyStr, keySize, offset, this->listId, stripeId, chunkId );
			} */
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
	this->update( stripeId, chunkId, 0, curPos, dataChunks, dataChunk, parityChunk, false, false );
	pthread_mutex_unlock( &this->lock );

	return true;
}

void ParityChunkBuffer::update( uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t size, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk, bool needsLock, bool needsUnlock ) {
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

	if ( needsLock ) pthread_mutex_lock( &this->lock );
	ParityChunkWrapper &wrapper = this->getWrapper( stripeId, false, false );

	pthread_mutex_lock( &wrapper.lock );
	wrapper.chunk->status = CHUNK_STATUS_DIRTY;
	if ( offset + size > wrapper.chunk->getSize() ) {
		wrapper.chunk->setSize( offset + size );
	}
	// Update the parity chunk
	char *parity = wrapper.chunk->getData();
	Coding::bitwiseXOR(
		parity,
		parity,
		parityChunk->getData(),
		ChunkBuffer::capacity
	);
	pthread_mutex_unlock( &wrapper.lock );
	if ( needsUnlock ) pthread_mutex_unlock( &this->lock );
}

void ParityChunkBuffer::update( uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t size, char *dataDelta, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk ) {
	// Prepare data delta
	dataChunk->clear();
	dataChunk->setSize( offset + size );
	memcpy( dataChunk->getData() + offset, dataDelta, size );
	this->update( stripeId, chunkId, offset, size, dataChunks, dataChunk, parityChunk, true, true );
}

void ParityChunkBuffer::flush( uint32_t stripeId, Chunk *chunk ) {
	// Append a flush event to the event queue
	/*
	IOEvent ioEvent;
	ioEvent.flush( chunk );
	ChunkBuffer::eventQueue->insert( ioEvent );
	*/
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
		std::map<uint32_t, ParityChunkWrapper>::iterator it = this->chunks.begin();
		it != this->chunks.end();
		it++
	) {
		ParityChunkWrapper &wrapper = it->second;
		if ( wrapper.pending ) {
			numPending++;
			occupied = ( double ) wrapper.chunk->getSize() / ChunkBuffer::capacity * 100.0;
			fprintf(
				f,
				"\t%u. [#%u] %u / %u (%5.2lf%%) (pending: %u)\n",
				numPending, wrapper.chunk->metadata.stripeId, wrapper.chunk->getSize(), ChunkBuffer::capacity, occupied,
				wrapper.pending
			);
			// The pending number does not necessarily equal to the number of data chunks in the dummy data chunk buffer as they may not have been allocated!
		}
	}
}

void ParityChunkBuffer::stop() {}
