#ifndef __SLAVE_MAP_MAP_HH__
#define __SLAVE_MAP_MAP_HH__

#include <unordered_map>
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/lock/lock.hh"
#include "../../common/protocol/protocol.hh"

class Map {
private:
	/**
	 * Store the mapping between keys and chunks
	 * Key |-> (list ID, stripe ID, chunk ID)
	 */
	std::unordered_map<Key, Metadata> keys;
	LOCK_T keysLock;
	/**
	 * Store the pending-to-send remapping records
	 * Key |-> (list ID, chunk ID)
	 */
	std::unordered_map<Key, RemappingRecord> remap;
	LOCK_T remapLock;

public:

	Map() {
		LOCK_INIT( &this->keysLock );
		LOCK_INIT( &this->remapLock );
	}

	bool setKey( char *keyStr, uint8_t keySize, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint8_t opcode ) {
		Key key;
		key.dup( keySize, keyStr );

		Metadata metadata;
		metadata.set( listId, stripeId, chunkId );

		std::unordered_map<Key, Metadata>::iterator it;
		std::pair<Key, Metadata> p( key, metadata );
		bool ret = true;

		LOCK( &this->keysLock );
		it = this->keys.find( key );
		if ( it == this->keys.end() && opcode == PROTO_OPCODE_SET ) {
			std::pair<std::unordered_map<Key, Metadata>::iterator, bool> r;
			r = this->keys.insert( p );
			ret = r.second;
		} else if ( it != this->keys.end() && opcode == PROTO_OPCODE_DELETE ) {
			this->keys.erase( it );
		} else {
			ret = false;
		}
		UNLOCK( &this->keysLock );

		return ret;
	}

	bool insertRemappingRecord( Key &key, RemappingRecord &remappingRecord ) {
		key.dup( key.size, key.data );

		std::pair<Key, RemappingRecord> p( key, remappingRecord );
		std::pair<std::unordered_map<Key, RemappingRecord>::iterator, bool> ret;

		LOCK( &this->remapLock );
		ret = this->remap.insert( p );
		if ( ! ret.second ) {
			UNLOCK( &this->remapLock );
			return false;
		}
		UNLOCK( &this->remapLock );

		return true;
	}

	void dump( FILE *f = stdout ) {
		fprintf( f, "List of key-value pairs:\n------------------------\n" );
		if ( ! this->keys.size() ) {
			fprintf( f, "(None)\n" );
		} else {
			for ( std::unordered_map<Key, Metadata>::iterator it = this->keys.begin(); it != this->keys.end(); it++ ) {
				fprintf(
					f, "%.*s --> (list ID: %u, stripe ID: %u, chunk ID: %u)\n",
					it->first.size, it->first.data,
					it->second.listId, it->second.stripeId, it->second.chunkId
				);
			}
			fprintf( f, "Count: %lu\n", this->keys.size() );
		}
		fprintf( f, "\n" );
	}

	void persist( FILE *f ) {
		LOCK( &this->keysLock );
		for ( std::unordered_map<Key, Metadata>::iterator it = this->keys.begin(); it != this->keys.end(); it++ ) {
			Key key = it->first;
			Metadata metadata = it->second;

			fprintf( f, "%.*s\t%u\t%u\t%u\n", key.size, key.data, metadata.listId, metadata.stripeId, metadata.chunkId );
		}
		UNLOCK( &this->keysLock );
	}
};

#endif
