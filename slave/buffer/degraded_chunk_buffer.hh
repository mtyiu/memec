#ifndef __SLAVE_BUFFER_DEGRADED_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_DEGRADED_CHUNK_BUFFER_HH__

#include "chunk_buffer.hh"

class DegradedMap {
private:
	/**
	 * Store the mapping between keys and chunks
	 * Key |-> (list ID, stripe ID, chunk ID, offset, length)
	 */
	std::unordered_map<Key, KeyMetadata> keys;
	LOCK_T keysLock;
	/**
	 * Store the cached chunks
	 * (list ID, stripe ID, chunk ID) |-> Chunk *
	 */
	std::unordered_map<Metadata, Chunk *> cache;
	LOCK_T cacheLock;

public:
	DegradedMap() {
		LOCK_INIT( &this->keysLock );
		LOCK_INIT( &this->cacheLock );
	}

	bool findValueByKey( char *data, uint8_t size, KeyValue *keyValue, Key *keyPtr = 0, KeyMetadata *keyMetadataPtr = 0, Metadata *metadataPtr = 0, Chunk **chunkPtr = 0 ) {
		std::unordered_map<Key, KeyMetadata>::iterator keysIt;
		std::unordered_map<Metadata, Chunk *>::iterator cacheIt;
		Key key;

		if ( keyValue )
			keyValue->clear();
		key.set( size, data );

		LOCK( &this->keysLock );
		keysIt = this->keys.find( key );
		if ( keysIt == this->keys.end() ) {
			if ( keyPtr ) *keyPtr = key;
			UNLOCK( &this->keysLock );
			return false;
		}

		if ( keyPtr ) *keyPtr = keysIt->first;
		if ( keyMetadataPtr ) *keyMetadataPtr = keysIt->second;
		UNLOCK( &this->keysLock );

		LOCK( &this->cacheLock );
		cacheIt = this->cache.find( keysIt->second );
		if ( cacheIt == this->cache.end() ) {
			UNLOCK( &this->cacheLock );
			return false;
		}

		if ( metadataPtr ) *metadataPtr = cacheIt->first;
		if ( chunkPtr ) *chunkPtr = cacheIt->second;

		Chunk *chunk = cacheIt->second;
		if ( keyValue )
			*keyValue = chunk->getKeyValue( keysIt->second.offset );
		UNLOCK( &this->cacheLock );
		return true;
	}

	Chunk *findChunkById( uint32_t listId, uint32_t stripeId, uint32_t chunkId, Metadata *metadataPtr = 0 ) {
		std::unordered_map<Metadata, Chunk *>::iterator it;
		Metadata metadata;

		metadata.set( listId, stripeId, chunkId );
		if ( metadataPtr ) *metadataPtr = metadata;

		LOCK( &this->cacheLock );
		it = this->cache.find( metadata );
		if ( it == this->cache.end() ) {
			UNLOCK( &this->cacheLock );
			return 0;
		}
		UNLOCK( &this->cacheLock );
		return it->second;
	}

	bool insertKey( Key key, uint8_t opcode, KeyMetadata &keyMetadata ) {
		OpMetadata opMetadata;
		opMetadata.clone( keyMetadata );
		opMetadata.opcode = opcode;

		Key k1;

		k1.dup( key.size, key.data );
		std::pair<Key, KeyMetadata> keyPair( k1, keyMetadata );
		std::pair<std::unordered_map<Key, KeyMetadata>::iterator, bool> keyRet;

		LOCK( &this->keysLock );
		keyRet = this->keys.insert( keyPair );
		if ( ! keyRet.second ) {
			UNLOCK( &this->keysLock );
			return false;
		}
		UNLOCK( &this->keysLock );

		return true;
	}

	void setChunk( uint32_t listId, uint32_t stripeId, uint32_t chunkId, Chunk *chunk, bool isParity = false ) {
		Metadata metadata;
		metadata.set( listId, stripeId, chunkId );

		LOCK( &this->cacheLock );
		this->cache[ metadata ] = chunk;
		UNLOCK( &this->cacheLock );

		if ( ! isParity ) {
			char *ptr = chunk->getData();
			char *keyPtr, *valuePtr;
			uint8_t keySize;
			uint32_t valueSize, offset = 0, size;

			LOCK( &this->keysLock );
			while( ptr < chunk->getData() + Chunk::capacity ) {
				KeyValue::deserialize( ptr, keyPtr, keySize, valuePtr, valueSize );
				if ( keySize == 0 && valueSize == 0 )
					break;

				Key key;
				KeyMetadata keyMetadata;

				size = KEY_VALUE_METADATA_SIZE + keySize + valueSize;

				key.set( keySize, keyPtr );
				keyMetadata.set( listId, stripeId, chunkId );
				keyMetadata.offset = offset;
				keyMetadata.length = size;

				offset += size;

				this->keys[ key ] = keyMetadata;

				ptr += size;
			}
			UNLOCK( &this->keysLock );
		}
	}

	bool deleteKey( Key key, uint8_t opcode, KeyMetadata &keyMetadata, bool needsLock, bool needsUnlock ) {
		Key k;
		std::unordered_map<Key, KeyMetadata>::iterator keysIt;

		if ( needsLock ) LOCK( &this->keysLock );
		keysIt = this->keys.find( key );
		if ( keysIt == this->keys.end() ) {
			if ( needsUnlock ) UNLOCK( &this->keysLock );
			return false;
		} else {
			k = keysIt->first;
			keyMetadata = keysIt->second;
			this->keys.erase( keysIt );
			k.free();
		}
		if ( needsUnlock ) UNLOCK( &this->keysLock );

		return true;
	}

	void getKeysMap( std::unordered_map<Key, KeyMetadata> *&keys, LOCK_T *&lock ) {
		keys = &this->keys;
		lock = &this->keysLock;
	}

	void getCacheMap( std::unordered_map<Metadata, Chunk *> *&cache, LOCK_T *&lock ) {
		cache = &this->cache;
		lock = &this->cacheLock;
	}

	void dump( FILE *f = stdout ) {
		fprintf( f, "List of key-value pairs:\n------------------------\n" );
		if ( ! this->keys.size() ) {
			fprintf( f, "(None)\n" );
		} else {
			for ( std::unordered_map<Key, KeyMetadata>::iterator it = this->keys.begin(); it != this->keys.end(); it++ ) {
				fprintf(
					f, "%.*s --> (list ID: %u, stripe ID: %u, chunk ID: %u, offset: %u, length: %u)\n",
					it->first.size, it->first.data,
					it->second.listId, it->second.stripeId, it->second.chunkId,
					it->second.offset, it->second.length
				);
			}
		}
		fprintf( f, "\n" );

		fprintf( f, "Number of key-value pairs: %lu\n\n", this->keys.size() );

		fprintf( f, "List of chunks in the cache:\n----------------------------\n" );
		if ( ! this->cache.size() ) {
			fprintf( f, "(None)\n" );
		} else {
			for ( std::unordered_map<Metadata, Chunk *>::iterator it = this->cache.begin(); it != this->cache.end(); it++ ) {
				fprintf(
					f, "(list ID: %u, stripe ID: %u, chunk ID: %u) --> %p (type: %s chunk, status: %s, count: %u, size: %u)\n",
					it->first.listId, it->first.stripeId, it->first.chunkId,
					it->second, it->second->isParity ? "parity" : "data",
					( it->second->status == CHUNK_STATUS_EMPTY ? "empty" :
						( it->second->status == CHUNK_STATUS_DIRTY ? "dirty" : "cached" )
					),
					it->second->count, it->second->getSize()
				);
			}
		}
		fprintf( f, "\n" );
	}
};

class DegradedChunkBuffer : public ChunkBuffer {
public:
	DegradedMap map;

	DegradedChunkBuffer();

	void print( FILE *f = stdout );
	void stop();

	~DegradedChunkBuffer();
};

#endif
