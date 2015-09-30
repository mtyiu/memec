#ifndef __SLAVE_MAP_MAP_HH__
#define __SLAVE_MAP_MAP_HH__

#include <map>
#include "../../common/ds/chunk.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/protocol/protocol.hh"

class Map {
private:
	/**
	 * Store the mapping between keys and chunks
	 * Key |-> (list ID, stripe ID, chunk ID, offset, length)
	 */
	std::map<Key, KeyMetadata> keys;
	pthread_mutex_t keysLock;
	/**
	 * Store the remapping records
	 * Key |-> (list ID, chunk ID)
	 */
	std::map<Key, RemappingRecord> remap;
	pthread_mutex_t remapLock;
	/**
	 * Store the cached chunks
	 * (list ID, stripe ID, chunk ID) |-> Chunk *
	 */
	std::map<Metadata, Chunk *> cache;
	pthread_mutex_t cacheLock;

public:
	/**
	 * Store the keys to be synchronized with coordinator
	 * Key |-> (list ID, stripe ID, chunk ID, opcode)
	 */
	std::map<Key, OpMetadata> ops;
	pthread_mutex_t opsLock;

	Map() {
		pthread_mutex_init( &this->keysLock, 0 );
		pthread_mutex_init( &this->remapLock, 0 );
		pthread_mutex_init( &this->cacheLock, 0 );
		pthread_mutex_init( &this->opsLock, 0 );
	}

	bool findRemappingRecordByKey( char *data, uint8_t size, RemappingRecord *remappingRecordPtr = 0, Key *keyPtr = 0 ) {
		std::map<Key, RemappingRecord>::iterator it;
		Key key;

		key.set( size, data );

		pthread_mutex_lock( &this->remapLock );
		it = this->remap.find( key );
		if ( it == this->remap.end() ) {
			if ( keyPtr ) *keyPtr = key;
			pthread_mutex_unlock( &this->remapLock );
			return false;
		}

		if ( keyPtr ) *keyPtr = it->first;
		if ( remappingRecordPtr ) *remappingRecordPtr = it->second;
		pthread_mutex_unlock( &this->remapLock );
		return true;
	}

	bool findValueByKey( char *data, uint8_t size, KeyValue *keyValue, Key *keyPtr = 0, KeyMetadata *keyMetadataPtr = 0, Metadata *metadataPtr = 0, Chunk **chunkPtr = 0 ) {
		std::map<Key, KeyMetadata>::iterator keysIt;
		std::map<Metadata, Chunk *>::iterator cacheIt;
		Key key;

		keyValue->clear();
		key.set( size, data );

		pthread_mutex_lock( &this->keysLock );
		keysIt = this->keys.find( key );
		if ( keysIt == this->keys.end() ) {
			if ( keyPtr ) *keyPtr = key;
			pthread_mutex_unlock( &this->keysLock );
			return false;
		}

		if ( keyPtr ) *keyPtr = keysIt->first;
		if ( keyMetadataPtr ) *keyMetadataPtr = keysIt->second;
		pthread_mutex_unlock( &this->keysLock );

		pthread_mutex_lock( &this->cacheLock );
		cacheIt = this->cache.find( keysIt->second );
		if ( cacheIt == this->cache.end() ) {
			pthread_mutex_unlock( &this->cacheLock );
			return false;
		}

		if ( metadataPtr ) *metadataPtr = cacheIt->first;
		if ( chunkPtr ) *chunkPtr = cacheIt->second;

		Chunk *chunk = cacheIt->second;
		*keyValue = chunk->getKeyValue( keysIt->second.offset );
		pthread_mutex_unlock( &this->cacheLock );
		return true;
	}

	Chunk *findChunkById( uint32_t listId, uint32_t stripeId, uint32_t chunkId, Metadata *metadataPtr = 0 ) {
		std::map<Metadata, Chunk *>::iterator it;
		Metadata metadata;

		metadata.set( listId, stripeId, chunkId );
		if ( metadataPtr ) *metadataPtr = metadata;

		pthread_mutex_lock( &this->cacheLock );
		it = this->cache.find( metadata );
		if ( it == this->cache.end() ) {
			pthread_mutex_unlock( &this->cacheLock );
			return 0;
		}
		pthread_mutex_unlock( &this->cacheLock );
		return it->second;
	}

	bool insertKey( Key &key, uint8_t opcode, KeyMetadata &keyMetadata ) {
		key.dup( key.size, key.data );

		std::pair<Key, KeyMetadata> p( key, keyMetadata );
		std::pair<std::map<Key, KeyMetadata>::iterator, bool> ret;

		pthread_mutex_lock( &this->keysLock );
		ret = this->keys.insert( p );
		if ( ! ret.second ) {
			pthread_mutex_unlock( &this->keysLock );
			return false;
		}
		pthread_mutex_unlock( &this->keysLock );

		OpMetadata opMetadata;
		opMetadata.clone( keyMetadata );
		opMetadata.opcode = opcode;
		pthread_mutex_lock( &this->opsLock );
		this->ops[ key ] = opMetadata;
		pthread_mutex_unlock( &this->opsLock );

		return true;
	}

	bool insertRemappingRecord( Key &key, RemappingRecord &remappingRecord ) {
		key.dup( key.size, key.data );

		std::pair<Key, RemappingRecord> p( key, remappingRecord );
		std::pair<std::map<Key, RemappingRecord>::iterator, bool> ret;

		pthread_mutex_lock( &this->remapLock );
		ret = this->remap.insert( p );
		if ( ! ret.second ) {
			pthread_mutex_unlock( &this->remapLock );
			return false;
		}
		pthread_mutex_unlock( &this->remapLock );

		OpMetadata opMetadata;
		opMetadata.listId = remappingRecord.listId;
		opMetadata.chunkId = remappingRecord.chunkId;
		opMetadata.opcode = PROTO_OPCODE_REMAPPING_LOCK;
		pthread_mutex_lock( &this->opsLock );
		this->ops[ key ] = opMetadata;
		pthread_mutex_unlock( &this->opsLock );

		return true;
	}

	void setChunk( uint32_t listId, uint32_t stripeId, uint32_t chunkId, Chunk *chunk, bool isParity = false ) {
		Metadata metadata;
		metadata.set( listId, stripeId, chunkId );

		pthread_mutex_lock( &this->cacheLock );
		this->cache[ metadata ] = chunk;
		pthread_mutex_unlock( &this->cacheLock );

		if ( ! isParity ) {
			char *ptr = chunk->getData();
			char *keyPtr, *valuePtr;
			uint8_t keySize;
			uint32_t valueSize, offset = 0, size;

			pthread_mutex_lock( &this->keysLock );
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
			pthread_mutex_unlock( &this->keysLock );
		}
	}

	void getKeysMap( std::map<Key, KeyMetadata> *&keys, pthread_mutex_t *&lock ) {
		keys = &this->keys;
		lock = &this->keysLock;
	}

	void dump() {
		/*
		fprintf( stdout, "List of key-value pairs:\n------------------------\n" );
		if ( ! this->keys.size() ) {
			fprintf( stdout, "(None)\n" );
		} else {
			for ( std::map<Key, KeyMetadata>::iterator it = this->keys.begin(); it != this->keys.end(); it++ ) {
				fprintf(
					stdout, "%.*s --> (list ID: %u, stripe ID: %u, chunk ID: %u, offset: %u, length: %u)\n",
					it->first.size, it->first.data,
					it->second.listId, it->second.stripeId, it->second.chunkId,
					it->second.offset, it->second.length
				);
			}
		}
		fprintf( stdout, "\n" );
		*/
		fprintf( stdout, "Number of key-value pairs: %lu\n\n", this->keys.size() );

		fprintf( stdout, "List of chunks in the cache:\n----------------------------\n" );
		if ( ! this->cache.size() ) {
			fprintf( stdout, "(None)\n" );
		} else {
			for ( std::map<Metadata, Chunk *>::iterator it = this->cache.begin(); it != this->cache.end(); it++ ) {
				fprintf(
					stdout, "(list ID: %u, stripe ID: %u, chunk ID: %u) --> %p (type: %s chunk, status: %s, count: %u, size: %u)\n",
					it->first.listId, it->first.stripeId, it->first.chunkId,
					it->second, it->second->isParity ? "parity" : "data",
					( it->second->status == CHUNK_STATUS_EMPTY ? "empty" :
						( it->second->status == CHUNK_STATUS_DIRTY ? "dirty" : "cached" )
					),
					it->second->count, it->second->getSize()
				);
			}
		}
		fprintf( stdout, "\n" );
	}
};

#endif
