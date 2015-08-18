#ifndef __SLAVE_MAP_MAP_HH__
#define __SLAVE_MAP_MAP_HH__

#include <map>
#include "../../common/ds/chunk.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/protocol/protocol.hh"

class Map {
public:
	/**
	 * Store the mapping between keys and chunks
	 * Key |-> (list ID, stripe ID, chunk ID, offset, length)
	 */
	std::map<Key, KeyMetadata> keys;
	/**
	 * Store the cached chunks
	 * (list ID, stripe ID, chunk ID) |-> Chunk *
	 */
	std::map<Metadata, Chunk *> cache;
	/**
	 * Store the keys to be synchronized with coordinator
	 * Key |-> (list ID, stripe ID, chunk ID, opcode)
	 */
	std::map<Key, OpMetadata> ops;

	bool findValueByKey( char *data, uint8_t size, KeyValue *keyValue, Key *keyPtr = 0, KeyMetadata *keyMetadataPtr = 0, Metadata *metadataPtr = 0, Chunk **chunkPtr = 0 ) {
		std::map<Key, KeyMetadata>::iterator keysIt;
		std::map<Metadata, Chunk *>::iterator cacheIt;
		Key key;

		keyValue->clear();
		key.set( size, data );
		keysIt = this->keys.find( key );
		if ( keysIt == this->keys.end() ) {
			if ( keyPtr ) *keyPtr = key;
			return false;
		}

		if ( keyPtr ) *keyPtr = keysIt->first;
		if ( keyMetadataPtr ) *keyMetadataPtr = keysIt->second;

		cacheIt = this->cache.find( keysIt->second );
		if ( cacheIt == this->cache.end() ) {
			return false;
		}

		if ( metadataPtr ) *metadataPtr = cacheIt->first;
		if ( chunkPtr ) *chunkPtr = cacheIt->second;

		Chunk *chunk = cacheIt->second;
		*keyValue = chunk->getKeyValue( keysIt->second.offset );
		return true;
	}

	Chunk *findChunkById( uint32_t listId, uint32_t stripeId, uint32_t chunkId, Metadata *metadataPtr = 0 ) {
		std::map<Metadata, Chunk *>::iterator it;
		Metadata metadata;

		metadata.set( listId, stripeId, chunkId );
		if ( metadataPtr ) *metadataPtr = metadata;

		it = this->cache.find( metadata );
		if ( it == this->cache.end() )
			return 0;
		return it->second;
	}

	void insertKey( Key &key, uint8_t opcode, KeyMetadata &keyMetadata ) {
		key.dup( key.size, key.data );
		this->keys[ key ] = keyMetadata;

		OpMetadata opMetadata;
		opMetadata.clone( keyMetadata );
		opMetadata.opcode = opcode;
		this->ops[ key ] = opMetadata;
	}

	void setChunk( uint32_t listId, uint32_t stripeId, uint32_t chunkId, Chunk *chunk, bool isParity = false ) {
		Metadata metadata;
		metadata.set( listId, stripeId, chunkId );
		this->cache[ metadata ] = chunk;
		if ( ! isParity ) {
			char *ptr = chunk->data;
			char *keyPtr, *valuePtr;
			uint8_t keySize;
			uint32_t valueSize, offset = 0, size;

			while( ptr < chunk->data + Chunk::capacity ) {
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
		}
	}
};

#endif
