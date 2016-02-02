#include "get_chunk_buffer.hh"
#include "../main/slave.hh"
#include "../../common/util/debug.hh"

MemoryPool<Chunk> *GetChunkBuffer::chunkPool;

void GetChunkBuffer::init() {
	GetChunkBuffer::chunkPool = Slave::getInstance()->chunkPool;
}

GetChunkBuffer::GetChunkBuffer() {
	LOCK_INIT( &this->lock );
}

GetChunkBuffer::~GetChunkBuffer() {
	std::unordered_map<Metadata, Chunk *>::iterator it;

	LOCK( &this->lock );
	for ( it = this->chunks.begin(); it != this->chunks.end(); it++ ) {
		if ( it->second ) {
			GetChunkBuffer::chunkPool->free( it->second );
		}
		it = this->chunks.erase( it );
	}
	UNLOCK( &this->lock );
}

bool GetChunkBuffer::insert( Metadata metadata, Chunk *chunk, bool needsLock, bool needsUnlock ) {
	bool ret = true;
	std::unordered_map<Metadata, Chunk *>::iterator it;

	if ( needsLock ) LOCK( &this->lock );
	it = this->chunks.find( metadata );
	if ( it == this->chunks.end() ) {
		// Copy the chunk
		Chunk *newChunk;

		if ( chunk ) {
			newChunk = GetChunkBuffer::chunkPool->malloc();
			newChunk->copy( chunk );
		} else {
			newChunk = 0;
		}

		std::pair<Metadata, Chunk *> p( metadata, newChunk );
		this->chunks.insert( p );
	} else {
		// Do nothing
	}
	if ( needsUnlock ) UNLOCK( &this->lock );
	return ret;
}

Chunk *GetChunkBuffer::find( Metadata metadata, bool needsLock, bool needsUnlock ) {
	Chunk *ret;
	std::unordered_map<Metadata, Chunk *>::iterator it;

	if ( needsLock ) LOCK( &this->lock );
	it = this->chunks.find( metadata );
	if ( it == this->chunks.end() ) {
		ret = 0;
	} else {
		ret = it->second;
	}
	if ( needsUnlock ) UNLOCK( &this->lock );
	return ret;
}

bool GetChunkBuffer::ack( Metadata metadata, bool needsLock, bool needsUnlock ) {
	bool ret = true;
	std::unordered_map<Metadata, Chunk *>::iterator it;

	if ( needsLock ) LOCK( &this->lock );
	it = this->chunks.find( metadata );
	if ( it == this->chunks.end() ) {
		std::pair<Metadata, Chunk *> p( metadata, 0 );
		this->chunks.insert( p );
	} else {
		GetChunkBuffer::chunkPool->free( it->second );
		it->second = 0;
	}
	if ( needsUnlock ) UNLOCK( &this->lock );
	return ret;
}

bool GetChunkBuffer::erase( Metadata metadata, bool needsLock, bool needsUnlock ) {
	bool ret = true;
	std::unordered_map<Metadata, Chunk *>::iterator it;

	if ( needsLock ) LOCK( &this->lock );
	it = this->chunks.find( metadata );
	if ( it == this->chunks.end() ) {
		// Do nothing
	} else {
		if ( it->second )
			GetChunkBuffer::chunkPool->free( it->second );
		this->chunks.erase( it );
	}
	if ( needsUnlock ) UNLOCK( &this->lock );
	return ret;
}
