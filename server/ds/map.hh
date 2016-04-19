#ifndef __SERVER_MAP_MAP_HH__
#define __SERVER_MAP_MAP_HH__

#include <unordered_map>
#include <unordered_set>
#include "../../common/ds/chunk.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/hash/cuckoo_hash.hh"
#include "../../common/lock/lock.hh"
#include "../../common/protocol/protocol.hh"
#include "../../common/timestamp/timestamp.hh"
#include "../../common/util/debug.hh"

class Map {
private:
	Timestamp *timestamp;
	/**
	 * Store the mapping between keys and chunks
	 * Key |-> (list ID, stripe ID, chunk ID, offset, length, isPartyRemapped)
	 */
	std::unordered_map<Key, KeyMetadata> keys;
	CuckooHash _keys;
	LOCK_T keysLock;
	/**
	 * Store the cached chunks
	 * (list ID, stripe ID, chunk ID) |-> Chunk *
	 */
	std::unordered_map<Metadata, Chunk *> cache;
	LOCK_T cacheLock;

	/**
	 * Store the used stripe IDs
	 * (list ID) |-> std::unordered_set<(stripe ID)>
	 */
	std::unordered_map<uint32_t, std::unordered_set<uint32_t>> stripeIds;
	LOCK_T stripeIdsLock;

	/**
	 * Store the forwarded, reconstructed chunks
	 * (list ID, stripe ID, chunk ID) (src) |-> (list ID, stripe ID, chunk ID) (dst)
	 */
	struct {
		std::unordered_map<Key, Metadata> keys;
		std::unordered_map<Metadata, Metadata> chunks;
		LOCK_T lock;
	} forwarded;

public:
	/**
	 * Store the keys to be synchronized with coordinator
	 * Key |-> (list ID, stripe ID, chunk ID, opcode)
	 */
	std::unordered_map<Key, OpMetadata> ops;
	LOCK_T opsLock;
	/**
	 * Store the metadata of the sealed chunks to be synchronized with coordinator
	 * (list ID, stripe ID, chunk ID)
	 */
	std::unordered_set<Metadata> sealed;
	LOCK_T sealedLock;

	Map() {
		LOCK_INIT( &this->keysLock );
		LOCK_INIT( &this->cacheLock );
		LOCK_INIT( &this->stripeIdsLock );
		LOCK_INIT( &this->forwarded.lock );
		LOCK_INIT( &this->opsLock );
		LOCK_INIT( &this->sealedLock );
	}

	void setTimestamp( Timestamp *timestamp ) {
		this->timestamp = timestamp;
	}

	bool findValueByKey( char *keyStr, uint8_t keySize, KeyValue *keyValuePtr, Key *keyPtr ) {
		Key key;
		KeyValue keyValue;

		if ( keyValuePtr )
			keyValuePtr->clear();
		key.set( keySize, keyStr );

		LOCK( &this->keysLock );
		keyValue.data = this->_keys.find( keyStr, keySize );
		if ( ! keyValue.data ) {
			UNLOCK( &this->keysLock );
			if ( keyPtr ) *keyPtr = key;
			return false;
		}
		key = keyValue.key();
		///// vvvvv Deprecated vvvvv /////
		std::unordered_map<Key, KeyMetadata>::iterator keysIt;
		keysIt = this->keys.find( key );
		if ( keysIt == this->keys.end() ) {
			UNLOCK( &this->keysLock );
			if ( keyPtr ) *keyPtr = key;
			return false;
		}

		if ( keyPtr ) *keyPtr = keysIt->first;
		///// ^^^^^ Deprecated ^^^^^ /////
		UNLOCK( &this->keysLock );

		if ( keyValuePtr )
			keyValuePtr->set( keysIt->second.obj );

		return true;
	}

	bool findValueByKey( char *data, uint8_t size, KeyValue *keyValuePtr, Key *keyPtr, KeyMetadata *keyMetadataPtr, Metadata *metadataPtr, Chunk **chunkPtr, bool needsLock = true, bool needsUnlock = true ) {
		std::unordered_map<Key, KeyMetadata>::iterator keysIt;
		std::unordered_map<Metadata, Chunk *>::iterator cacheIt;
		Key key;

		if ( keyValuePtr )
			keyValuePtr->clear();
		key.set( size, data );

		if ( needsLock ) LOCK( &this->keysLock );
		keysIt = this->keys.find( key );
		if ( keysIt == this->keys.end() ) {
			if ( needsUnlock ) UNLOCK( &this->keysLock );
			if ( keyPtr ) *keyPtr = key;
			return false;
		}

		if ( keyPtr ) *keyPtr = keysIt->first;
		if ( keyMetadataPtr ) *keyMetadataPtr = keysIt->second;
		if ( needsUnlock ) UNLOCK( &this->keysLock );

		if ( needsLock ) LOCK( &this->cacheLock );
		cacheIt = this->cache.find( keysIt->second );
		if ( cacheIt == this->cache.end() ) {
			if ( needsUnlock ) UNLOCK( &this->cacheLock );
			return false;
		}

		if ( metadataPtr ) *metadataPtr = cacheIt->first;
		if ( chunkPtr ) *chunkPtr = cacheIt->second;

		Chunk *chunk = cacheIt->second;
		if ( keyValuePtr )
			*keyValuePtr = chunk->getKeyValue( keysIt->second.offset );
		if ( needsUnlock ) UNLOCK( &this->cacheLock );
		return true;
	}

	Chunk *findChunkById( uint32_t listId, uint32_t stripeId, uint32_t chunkId, Metadata *metadataPtr = 0, bool needsLock = true, bool needsUnlock = true, LOCK_T **lock = 0 ) {
		std::unordered_map<Metadata, Chunk *>::iterator it;
		Metadata metadata;

		metadata.set( listId, stripeId, chunkId );
		if ( metadataPtr ) *metadataPtr = metadata;
		if ( lock ) *lock = &this->cacheLock;

		if ( needsLock ) LOCK( &this->cacheLock );
		it = this->cache.find( metadata );
		if ( it == this->cache.end() ) {
			UNLOCK( &this->cacheLock );
			return 0;
		}
		if ( needsUnlock ) UNLOCK( &this->cacheLock );
		return it->second;
	}

	bool insertKey( Key key, uint8_t opcode, uint32_t &timestamp, KeyMetadata &keyMetadata, bool needsLock = true, bool needsUnlock = true, bool needsUpdateOpMetadata = true ) {
		key.dup();

		std::pair<Key, KeyMetadata> keyPair( key, keyMetadata );
		std::pair<std::unordered_map<Key, KeyMetadata>::iterator, bool> keyRet;

		if ( needsLock ) LOCK( &this->keysLock );

		if ( ! this->_keys.insert( key.data, key.size, keyMetadata.obj ) ) {
			if ( needsUnlock ) UNLOCK( &this->keysLock );
			return false;
		}
		///// vvvvv Deprecated vvvvv /////
		keyRet = this->keys.insert( keyPair );
		if ( ! keyRet.second ) {
			if ( needsUnlock ) UNLOCK( &this->keysLock );
			return false;
		}
		///// ^^^^^ Deprecated ^^^^^ /////
		if ( needsUnlock ) UNLOCK( &this->keysLock );

		return needsUpdateOpMetadata ? this->insertOpMetadata( opcode, timestamp, key, keyMetadata ) : true;
	}

	bool insertOpMetadata( uint8_t opcode, uint32_t &timestamp, Key key, KeyMetadata keyMetadata, bool dup = true ) {
		bool ret;
		std::unordered_map<Key, OpMetadata>::iterator opsIt;
		std::pair<std::unordered_map<Key, OpMetadata>::iterator, bool> opRet;

		LOCK( &this->opsLock );
		opsIt = this->ops.find( key );
		if ( opsIt == this->ops.end() ) {
			OpMetadata opMetadata;
			opMetadata.clone( keyMetadata );
			opMetadata.opcode = opcode;
			opMetadata.timestamp = this->timestamp->nextVal();
			timestamp = opMetadata.timestamp;

			if ( dup ) key.dup();

			std::pair<Key, OpMetadata> opsPair( key, opMetadata );
			std::pair<std::unordered_map<Key, OpMetadata>::iterator, bool> opsRet;

			opsRet = this->ops.insert( opsPair );
			ret = opsRet.second;
		} else if ( opcode == PROTO_OPCODE_DELETE ) {
			Key k = opsIt->first;
			this->ops.erase( opsIt );
			k.free();
			ret = true;
		} else {
			ret = false;
		}
		UNLOCK( &this->opsLock );

		return ret;
	}

	bool seal( uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
		Metadata metadata;
		metadata.set( listId, stripeId, chunkId );

		std::pair<std::unordered_set<Metadata>::iterator, bool> ret;

		LOCK( &this->sealedLock );
		ret = this->sealed.insert( metadata );
		UNLOCK( &this->sealedLock );

		return ret.second;
	}

	void setChunk( uint32_t listId, uint32_t stripeId, uint32_t chunkId, Chunk *chunk, bool isParity = false, bool needsLock = true, bool needsUnlock = true ) {
		Metadata metadata;
		metadata.set( listId, stripeId, chunkId );

		if ( needsLock ) LOCK( &this->cacheLock );
		if ( this->cache.find( metadata ) != this->cache.end() ) {
			__ERROR__( "Map", "setChunk", "This chunk (%u, %u, %u) already exists.", listId, stripeId, chunkId );
		}
		this->cache[ metadata ] = chunk;
		if ( needsUnlock ) UNLOCK( &this->cacheLock );

		if ( needsLock ) LOCK( &this->stripeIdsLock );
		this->stripeIds[ listId ].insert( stripeId );
		if ( needsUnlock ) UNLOCK( &this->stripeIdsLock );

		if ( ! isParity ) {
			char *ptr = chunk->getData();
			char *keyPtr, *valuePtr;
			uint8_t keySize;
			uint32_t valueSize, offset = 0, size;

			if ( needsLock ) LOCK( &this->keysLock );
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
			if ( needsUnlock ) UNLOCK( &this->keysLock );
		}
	}

	uint32_t nextStripeID( uint32_t listId, uint32_t from = 0 ) {
		uint32_t ret = from;
		LOCK( &this->stripeIdsLock );
		std::unordered_set<uint32_t> &sids = this->stripeIds[ listId ];
		std::unordered_set<uint32_t>::iterator end = sids.end();
		while ( sids.find( ret ) != end )
			ret++;
		UNLOCK( &this->stripeIdsLock );
		return ret;
	}

	bool insertForwardedChunk(
		uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId,
		uint32_t dstListId, uint32_t dstStripeId, uint32_t dstChunkId
	) {
		Metadata srcMetadata, dstMetadata;
		srcMetadata.set( srcListId, srcStripeId, srcChunkId );
		dstMetadata.set( dstListId, dstStripeId, dstChunkId );

		std::pair<std::unordered_map<Metadata, Metadata>::iterator, bool> ret;
		std::pair<Metadata, Metadata> p( srcMetadata, dstMetadata );

		LOCK( &this->forwarded.lock );
		ret = this->forwarded.chunks.insert( p );
		UNLOCK( &this->forwarded.lock );

		return ret.second;
	}

	bool findForwardedChunk( uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId, Metadata &dstMetadata ) {
		Metadata srcMetadata;
		srcMetadata.set( srcListId, srcStripeId, srcChunkId );

		std::unordered_map<Metadata, Metadata>::iterator it;
		bool ret = false;

		LOCK( &this->forwarded.lock );
		it = this->forwarded.chunks.find( srcMetadata );
		if ( it != this->forwarded.chunks.end() ) {
			dstMetadata = it->second;
			ret = true;
		}
		UNLOCK( &this->forwarded.lock );

		return ret;
	}

	bool eraseForwardedChunk( uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId ) {
		Metadata srcMetadata;
		srcMetadata.set( srcListId, srcStripeId, srcChunkId );

		std::unordered_map<Metadata, Metadata>::iterator it;
		bool ret;

		LOCK( &this->forwarded.lock );
		ret = this->forwarded.chunks.erase( srcMetadata ) > 0;
		UNLOCK( &this->forwarded.lock );

		return ret;
	}

	bool insertForwardedKey(
		uint8_t keySize, char *keyStr,
		uint32_t dstListId, uint32_t dstChunkId
	) {
		Metadata dstMetadata;
		Key key;

		key.set( keySize, keyStr );
		dstMetadata.set( dstListId, -1, dstChunkId );

		std::pair<std::unordered_map<Key, Metadata>::iterator, bool> ret;

		LOCK( &this->forwarded.lock );
		if ( this->forwarded.keys.find( key ) == this->forwarded.keys.end() )
			key.dup();

		std::pair<Key, Metadata> p( key, dstMetadata );
		ret = this->forwarded.keys.insert( p );
		UNLOCK( &this->forwarded.lock );

		return ret.second;
	}

	bool findForwardedKey( uint8_t keySize, char *keyStr, Metadata &dstMetadata ) {
		Key key;
		key.set( keySize, keyStr );

		std::unordered_map<Key, Metadata>::iterator it;
		bool ret = false;

		LOCK( &this->forwarded.lock );
		it = this->forwarded.keys.find( key );
		if ( it != this->forwarded.keys.end() ) {
			dstMetadata = it->second;
			ret = true;
		}
		UNLOCK( &this->forwarded.lock );

		return ret;
	}

	bool eraseForwardedKey( uint8_t keySize, char *keyStr ) {
		Key key;
		key.set( keySize, keyStr );

		std::unordered_map<Key, Metadata>::iterator it;
		bool ret;

		LOCK( &this->forwarded.lock );
		ret = this->forwarded.keys.erase( key ) > 0;
		UNLOCK( &this->forwarded.lock );

		return ret;
	}

	bool deleteKey( Key key, uint8_t opcode, uint32_t &timestamp, KeyMetadata &keyMetadata, bool needsLock, bool needsUnlock, bool needsUpdateOpMetadata = true ) {
		Key k;
		std::unordered_map<Key, KeyMetadata>::iterator keysIt;
		std::unordered_map<Key, OpMetadata>::iterator opsIt;

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

		return needsUpdateOpMetadata ? this->insertOpMetadata( opcode, timestamp, key, keyMetadata ) : true;
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

#endif
