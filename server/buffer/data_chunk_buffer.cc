#include "data_chunk_buffer.hh"
#include "../main/server.hh"
#include "../worker/worker.hh"

DataChunkBuffer::DataChunkBuffer( uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isReady ) : ChunkBuffer( isReady ) {
	this->count = count;
	this->locks = new LOCK_T[ count ];
	this->chunks = new Chunk*[ count ];
#ifndef REINSERTED_CHUNKS_IS_SET
	this->reInsertedChunks = new Chunk*[ count ];
#endif
	this->sizes = new uint32_t[ count ];

	for ( uint32_t i = 0; i < count; i++ ) {
		LOCK_INIT( this->locks + i );
		this->sizes[ i ] = 0;
		this->chunks[ i ] = ChunkBuffer::chunkPool->alloc();

#ifndef REINSERTED_CHUNKS_IS_SET
		this->reInsertedChunks[ i ] = 0;
#endif
	}

	this->listId = listId;
	this->stripeId = stripeId;
	this->chunkId = chunkId;
	if ( isReady )
		this->init();
}

void DataChunkBuffer::init() {
	uint32_t stripeId;

	for ( uint32_t i = 0; i < this->count; i++ ) {
		stripeId = ChunkBuffer::map->nextStripeID( this->listId, this->stripeId );

		ChunkUtil::set( this->chunks[ i ], this->listId, stripeId, this->chunkId, 0 );

		ChunkBuffer::map->setChunk(
			this->listId, stripeId, this->chunkId,
			this->chunks[ i ],
			false // isParity
		);

		this->stripeId = stripeId + 1;
	}
}

KeyMetadata DataChunkBuffer::set( ServerWorker *worker, char *key, uint8_t keySize, char *value, uint32_t valueSize, uint8_t opcode, uint32_t &timestamp, uint32_t &stripeId, bool *isSealed, Metadata *sealed ) {
	KeyMetadata keyMetadata;
	uint32_t size = PROTO_KEY_VALUE_SIZE + keySize + valueSize, max = 0, tmp;
	int index = -1;
	Chunk *reInsertedChunk = 0, *chunk = 0;
	char *ptr;

	if ( isSealed ) *isSealed = false;

	// Choose one chunk buffer with minimum free space
	LOCK( &this->lock );
	if ( size <= this->reInsertedChunkMaxSpace ) {
		uint32_t space, min = ChunkBuffer::capacity;
		Chunk *c;
		// Choose one from the re-inserted chunks
#ifdef REINSERTED_CHUNKS_IS_SET
		std::unordered_set<Chunk *>::iterator it;
		for ( it = this->reInsertedChunks.begin(); it != this->reInsertedChunks.end(); it++ ) {
			c = *it;
			space = ChunkBuffer::capacity - ChunkUtil::getSize( c );
			if ( space >= size && space < min ) {
				min = space;
				reInsertedChunk = c;
			}
		}
#else
		for ( uint32_t i = 0; i < this->count; i++ ) {
			if ( ( c = this->reInsertedChunks[ i ] ) ) {
				space = ChunkBuffer::capacity - ChunkUtil::getSize( c );
				if ( space >= size && space < min ) {
					min = space;
					reInsertedChunk = c;
				}
			}
		}
#endif
		if ( reInsertedChunk ) {
			// Update reInsertedChunkMaxSpace if the chunk with reInsertedChunkMaxSpace is chosen
			if ( ChunkBuffer::capacity - ChunkUtil::getSize( reInsertedChunk ) == this->reInsertedChunkMaxSpace ) {
				this->reInsertedChunkMaxSpace = 0;

#ifdef REINSERTED_CHUNKS_IS_SET
				for ( it = this->reInsertedChunks.begin(); it != this->reInsertedChunks.end(); it++ ) {
					c = *it;
					space = ChunkBuffer::capacity - ChunkUtil::getSize( c );
					if ( space > this->reInsertedChunkMaxSpace )
						this->reInsertedChunkMaxSpace = space;
				}
#else
				for ( uint32_t i = 0; i < this->count; i++ ) {
					if ( ( c = this->reInsertedChunks[ i ] ) ) {
						space = ChunkBuffer::capacity - ChunkUtil::getSize( c );
						if ( space > this->reInsertedChunkMaxSpace )
							this->reInsertedChunkMaxSpace = space;
					}
				}
#endif
			}
		}
	}
	if ( reInsertedChunk ) {
		chunk = reInsertedChunk;
		index = -1;
	} else {
		// Choose from chunk buffer if no re-inserted chunks can be used
		for ( uint32_t i = 0; i < this->count; i++ ) {
			tmp = this->sizes[ i ] + size;
			if ( tmp <= ChunkBuffer::capacity ) {
				if ( tmp > max ) {
					max = tmp;
					index = i;
				} else if ( tmp == max && index != -1 ) {
					if ( ChunkUtil::getStripeId( this->chunks[ i ] ) < ChunkUtil::getStripeId( this->chunks[ index ] ) )
						index = i;
				}
			}
		}

		if ( index == -1 ) {
			// A chunk is sealed
			if ( isSealed ) *isSealed = true;
			index = this->flush( worker, false, true, sealed );
		}

		// Allocate memory in the selected chunk
		LOCK( this->locks + index );
		chunk = this->chunks[ index ];
	}

	// Set up key metadata
	keyMetadata.listId = this->listId;
	keyMetadata.stripeId = ChunkUtil::getStripeId( chunk );
	keyMetadata.chunkId = this->chunkId;
	keyMetadata.length = size;
	keyMetadata.isParityRemapped = ( opcode == PROTO_OPCODE_DEGRADED_SET );
	// keyMetadata.ptr = ( char * ) chunk;

	// Allocate memory from chunk
	ptr = ChunkUtil::alloc( chunk, size, keyMetadata.offset );
	if ( index != -1 )
		this->sizes[ index ] += size;

	keyMetadata.obj = ptr;

	// Copy data to the buffer
	KeyValue::serialize( ptr, key, keySize, value, valueSize );

	// Flush if the current buffer is full
	if ( ChunkUtil::getSize( chunk ) + PROTO_KEY_VALUE_SIZE + CHUNK_BUFFER_FLUSH_THRESHOLD >= ChunkBuffer::capacity ) {
		if ( index != -1 ) {
			if ( isSealed ) *isSealed = true;
			this->flushAt( worker, index, false, sealed );
		} else {
			__ERROR__( "DataChunkBuffer", "set", "TODO: Fix lastDelPos." );
			// worker->issueSealChunkRequest( chunk, chunk->lastDelPos );
#ifdef REINSERTED_CHUNKS_IS_SET
			this->reInsertedChunks.erase( chunk );
#else
			for ( uint32_t i = 0; i < this->count; i++ ) {
				if ( this->reInsertedChunks[ i ] == chunk ) {
					this->reInsertedChunks[ i ] = 0;
					break;
				}
			}
#endif
		}
	}

	if ( index != -1 )
		UNLOCK( this->locks + index );
	UNLOCK( &this->lock );

	// Update key map
	Key keyObj;
	keyObj.set( keySize, key );
	ChunkBuffer::map->insertKey( keyObj, opcode, timestamp, keyMetadata );
	stripeId = keyMetadata.stripeId;

	return keyMetadata;
}

size_t DataChunkBuffer::seal( ServerWorker *worker ) {
	uint32_t count = 0;
	Chunk *c;
	LOCK( &this->lock );
	for ( uint32_t i = 0; i < this->count; i++ ) {
		this->flushAt( worker, i, false );
		count++;
	}
#ifdef REINSERTED_CHUNKS_IS_SET
	for (
		std::unordered_set<Chunk *>::iterator it = this->reInsertedChunks.begin();
		it != this->reInsertedChunks.end();
		it++
	) {
		c = *it;
		if ( c ) {
			__ERROR__( "DataChunkBuffer", "set", "TODO: Fix lastDelPos." );
			// worker->issueSealChunkRequest( c, c->lastDelPos );
			count++;
		}
	}
#else
	for ( uint32_t i = 0; i < this->count; i++ ) {
		c = this->reInsertedChunks[ i ];
		if ( c ) {
			__ERROR__( "DataChunkBuffer", "set", "TODO: Fix lastDelPos." );
			// worker->issueSealChunkRequest( c, c->lastDelPos );
			count++;
		}
	}
#endif
	UNLOCK( &this->lock );
	return count;
}

bool DataChunkBuffer::reInsert( ServerWorker *worker, Chunk *chunk, uint32_t sizeToBeFreed, bool needsLock, bool needsUnlock ) {
#ifdef REINSERTED_CHUNKS_IS_SET
	std::unordered_set<Chunk *>::iterator it;
	std::pair<std::unordered_set<Chunk *>::iterator, bool> ret;
#else
	bool ret = true;
#endif
	uint32_t space;

	if ( needsLock ) LOCK( &this->lock );

	space = ChunkBuffer::capacity - ChunkUtil::getSize( chunk ) + sizeToBeFreed;

#ifdef REINSERTED_CHUNKS_IS_SET
	ret = this->reInsertedChunks.insert( chunk );
#else
	// Limit the number of re-inserted chunks to be this->count
	Chunk *c;
	uint32_t i, j = this->count, min = space, index = this->count;
	Chunk *selected = chunk;
	bool isFull = true;
	for ( i = 0; i < this->count; i++ ) {
		if ( ( c = this->reInsertedChunks[ i ] ) ) {
			if ( c == chunk ) {
				// No need to insert as the chunk is already re-inserted
				ret = false;
				goto reInsertExit;
			} else if ( ChunkBuffer::capacity - c->getSize() < min ) {
				min = ChunkBuffer::capacity - c->getSize();
				index = i;
				selected = c;
			}
		} else {
			isFull = false;
			j = ( j == this->count ) ? i : j;
		}
	}
	if ( isFull ) {
		if ( selected != chunk ) {
			worker->issueSealChunkRequest( selected, selected->lastDelPos );
			this->reInsertedChunks[ index ] = chunk;
		} else {
			// Ignore the current chunk if it has least free space
			if ( needsUnlock ) UNLOCK( &this->lock );
			return true;
		}
	} else {
		this->reInsertedChunks[ j ] = chunk;
	}

reInsertExit:
#endif
	if ( space > reInsertedChunkMaxSpace )
		reInsertedChunkMaxSpace = space;

	if ( needsUnlock ) UNLOCK( &this->lock );

#ifdef REINSERTED_CHUNKS_IS_SET
	return ret.second;
#else
	return ret;
#endif
}

int DataChunkBuffer::lockChunk( Chunk *chunk, bool keepGlobalLock ) {
	int index = -1;
	LOCK( &this->lock );
	for ( uint32_t i = 0; i < this->count; i++ ) {
		if ( this->chunks[ i ] == chunk ) {
			index = i;
			break;
		}
	}
	if ( index != -1 ) {
		// Found
		LOCK( this->locks + index );
	} else {
		if ( ! keepGlobalLock )
			UNLOCK( &this->lock );
	}
	return index;
}

void DataChunkBuffer::updateAndUnlockChunk( int index ) {
	this->sizes[ index ] = ChunkUtil::getSize( this->chunks[ index ] );
	UNLOCK( this->locks + index );
	UNLOCK( &this->lock );
}

void DataChunkBuffer::unlock( int index ) {
	if ( index != -1 )
		UNLOCK( this->locks + index );
	UNLOCK( &this->lock );
}

uint32_t DataChunkBuffer::flush( ServerWorker *worker, bool lock, bool lockAtIndex, Metadata *sealed ) {
	if ( lock )
		LOCK( &this->lock );

	uint32_t index = 0;
	uint32_t max = this->sizes[ 0 ];

	for ( uint32_t i = 1; i < this->count; i++ ) {
		if ( this->sizes[ i ] > max ) {
			max = this->sizes[ i ];
			index = i;
		} else if ( this->sizes[ i ] == max ) {
			if ( ChunkUtil::getStripeId( this->chunks[ i ] ) < ChunkUtil::getStripeId( this->chunks[ index ] ) )
					index = i;
		}
	}

	if ( lock || lockAtIndex )
		LOCK( this->locks + index );

	this->flushAt( worker, index, false, sealed );

	if ( lock || lockAtIndex )
		UNLOCK( this->locks + index );

	if ( lock )
		UNLOCK( &this->lock );

	return index;
}

Chunk *DataChunkBuffer::flushAt( ServerWorker *worker, int index, bool lock, Metadata *sealed ) {
	if ( lock ) {
		LOCK( &this->lock );
		LOCK( this->locks + index );
	}

	Chunk *chunk = this->chunks[ index ];
	Metadata metadata;
	uint32_t size;

	if ( sealed ) {
		ChunkUtil::get( chunk, metadata.listId, metadata.stripeId, metadata.chunkId, size );
		sealed->set(
			metadata.listId,
			metadata.stripeId,
			metadata.chunkId
		);
	}

	// Get a new chunk
	this->sizes[ index ] = 0;

	metadata.set(
		this->listId,
		ChunkBuffer::map->nextStripeID( this->listId, this->stripeId ),
		this->chunkId
	);
	Chunk *newChunk = ChunkBuffer::chunkPool->alloc( metadata.listId, metadata.stripeId, metadata.chunkId, 0 );

	ChunkBuffer::map->setChunk(
		metadata.listId,
		metadata.stripeId,
		metadata.chunkId,
		newChunk,
		false // isParity
	);
	this->chunks[ index ] = newChunk;
	this->stripeId = metadata.stripeId + 1;

	// Notify the parity servers to seal the chunk
	if ( worker->issueSealChunkRequest( chunk ) ) {
		ChunkUtil::get( chunk, metadata.listId, metadata.stripeId, metadata.chunkId, size );
		ChunkBuffer::map->seal( metadata.listId, metadata.stripeId, metadata.chunkId );
	}

	if ( lock ) {
		UNLOCK( this->locks + index );
		UNLOCK( &this->lock );
	}

	return chunk;
}

void DataChunkBuffer::print( FILE *f ) {
	int width = 16;
	double occupied;
	fprintf(
		f,
		"- %-*s : %s\n"
		"- %-*s : %u\n"
		"- %-*s : %u\n"
		"- %-*s :\n",
		width, "Role", "Data chunk buffer",
		width, "Chunk size", ChunkBuffer::capacity,
		width, "Number of chunks", this->count,
		width, "Statistics (occupied / total)"
	);
	for ( uint32_t i = 0; i < this->count; i++ ) {
		uint32_t size = this->sizes[ i ];
		occupied = ( double ) size / ChunkBuffer::capacity * 100.0;
		fprintf(
			f,
			"\t%u. [#%u] %u / %u (%5.2lf%%)\n",
			( i + 1 ),
			ChunkUtil::getStripeId( this->chunks[ i ] ),
			size, ChunkBuffer::capacity, occupied
		);
	}
}

void DataChunkBuffer::stop() {}

DataChunkBuffer::~DataChunkBuffer() {
	delete[] this->locks;
	delete[] this->chunks;
	delete[] this->sizes;
}
