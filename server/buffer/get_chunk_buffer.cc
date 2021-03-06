#include "get_chunk_buffer.hh"
#include "../main/server.hh"
#include "../../common/util/debug.hh"

GetChunkBuffer::GetChunkBuffer() {
	LOCK_INIT( &this->lock );
}

GetChunkBuffer::~GetChunkBuffer() {
	std::unordered_map<Metadata, GetChunkWrapper>::iterator it;

	LOCK( &this->lock );
	for ( it = this->chunks.begin(); it != this->chunks.end(); it++ ) {
		if ( it->second.chunk ) {
			this->chunkPool.free( it->second.chunk );
			if ( it->second.sealIndicator )
				delete[] it->second.sealIndicator;
		}
		it = this->chunks.erase( it );
	}
	UNLOCK( &this->lock );
}

bool GetChunkBuffer::insert( Metadata metadata, Chunk *chunk, uint8_t sealIndicatorCount, bool *sealIndicator, bool needsLock, bool needsUnlock ) {
	if ( ! chunk )
		return false;
	bool ret = true;
	std::unordered_map<Metadata, GetChunkWrapper>::iterator it;

	if ( needsLock ) LOCK( &this->lock );
	it = this->chunks.find( metadata );
	if ( it == this->chunks.end() ) {
		// Copy the chunk
		Chunk *newChunk;
		GetChunkWrapper wrapper;

		if ( chunk ) {
			newChunk = this->chunkPool.alloc();
			ChunkUtil::dup( newChunk, chunk );
			wrapper.sealIndicatorCount = sealIndicatorCount;
			wrapper.sealIndicator = sealIndicator;
		} else {
			newChunk = 0;
		}

		wrapper.chunk = newChunk;
		wrapper.acked = false;

		std::pair<Metadata, GetChunkWrapper> p( metadata, wrapper );
		this->chunks.insert( p );
	} else if ( ! it->second.acked ) {
		if ( chunk ) {
			if ( ! it->second.chunk ) {
				it->second.chunk = this->chunkPool.alloc();
				ChunkUtil::dup( it->second.chunk, chunk );
				it->second.sealIndicatorCount = sealIndicatorCount;
				it->second.sealIndicator = sealIndicator;
			}
		} else {
			it->second.chunk = 0;
		}
	}
	if ( needsUnlock ) UNLOCK( &this->lock );
	return ret;
}

Chunk *GetChunkBuffer::find( Metadata metadata, bool &exists, uint8_t &sealIndicatorCount, bool *&sealIndicator, bool needsLock, bool needsUnlock ) {
	Chunk *ret;
	std::unordered_map<Metadata, GetChunkWrapper>::iterator it;

	if ( needsLock ) LOCK( &this->lock );
	it = this->chunks.find( metadata );
	if ( it == this->chunks.end() ) {
		ret = 0;
		exists = false;
	} else {
		ret = it->second.chunk;
		sealIndicatorCount = it->second.sealIndicatorCount;
		sealIndicator = it->second.sealIndicator;
		exists = true;
	}
	if ( needsUnlock ) UNLOCK( &this->lock );
	return ret;
}

bool GetChunkBuffer::ack( Metadata metadata, bool needsLock, bool needsUnlock, bool needsFree ) {
	bool ret = true;
	std::unordered_map<Metadata, GetChunkWrapper>::iterator it;

	if ( needsLock ) LOCK( &this->lock );
	it = this->chunks.find( metadata );
	if ( it == this->chunks.end() ) {
		GetChunkWrapper wrapper;
		wrapper.chunk = 0;
		wrapper.sealIndicator = 0;
		wrapper.sealIndicatorCount = 0;
		wrapper.acked = true;
		std::pair<Metadata, GetChunkWrapper> p( metadata, wrapper );
		this->chunks.insert( p );
	} else {
		if ( needsFree ) {
			if ( it->second.chunk )
				this->chunkPool.free( it->second.chunk );
			if ( it->second.sealIndicator )
				delete[] it->second.sealIndicator;
		}
		it->second.chunk = 0;
		it->second.sealIndicator = 0;
		it->second.sealIndicatorCount = 0;
		it->second.acked = true;
	}
	if ( needsUnlock ) UNLOCK( &this->lock );
	return ret;
}

bool GetChunkBuffer::erase( Metadata metadata, bool needsLock, bool needsUnlock ) {
	bool ret = true;
	std::unordered_map<Metadata, GetChunkWrapper>::iterator it;

	if ( needsLock ) LOCK( &this->lock );
	it = this->chunks.find( metadata );
	if ( it == this->chunks.end() ) {
		// Do nothing
	} else {
		if ( it->second.chunk )
			this->chunkPool.free( it->second.chunk );
		if ( it->second.sealIndicator )
			delete[] it->second.sealIndicator;
		this->chunks.erase( it );
	}
	if ( needsUnlock ) UNLOCK( &this->lock );
	return ret;
}

void GetChunkBuffer::unlock() {
	UNLOCK( &this->lock );
}
