#ifndef __COORDINATOR_DS_REMAPPING_RECORD_MAP_HH__
#define __COORDINATOR_DS_REMAPPING_RECORD_MAP_HH__

#include <unordered_map>
#include <unordered_set>
#include <pthread.h>
#include <assert.h>
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/lock/lock.hh"
#include "../../common/stripe_list/stripe_list.hh"

struct RemappingRecord {
	uint32_t *original;
	uint32_t *remapped;
	uint32_t remappedCount;

	RemappingRecord() {
		this->original = 0;
		this->remapped = 0;
		this->remappedCount = 0;
	}

	void set( uint32_t *original, uint32_t *remapped, uint32_t remappedCount ) {
		this->original = remappedCount ? original : 0;
		this->remapped = remappedCount ? remapped : 0;
		this->remappedCount = remappedCount;
	}

	void dup( uint32_t *original, uint32_t *remapped, uint32_t remappedCount ) {
		this->remappedCount = remappedCount;
		if ( remappedCount ) {
			this->original = new uint32_t[ remappedCount * 2 ];
			this->remapped = new uint32_t[ remappedCount * 2 ];

			for ( uint32_t i = 0; i < remappedCount; i++ ) {
				this->original[ i * 2     ] = original[ i * 2     ];
				this->original[ i * 2 + 1 ] = original[ i * 2 + 1 ];
				this->remapped[ i * 2     ] = remapped[ i * 2     ];
				this->remapped[ i * 2 + 1 ] = remapped[ i * 2 + 1 ];
			}
		}
	}

	void free() {
		if ( this->original ) delete[] this->original;
		if ( this->remapped ) delete[] this->remapped;
		this->original = 0;
		this->remapped = 0;
		this->remappedCount = 0;
	}
};

class RemappingRecordMap {
private:
	std::unordered_map<Key, RemappingRecord> map;
	std::unordered_map<struct sockaddr_in, std::unordered_set<Key>* > slaveToKeyMap;
	LOCK_T lock;

public:
	RemappingRecordMap() {
		this->map.clear();
		LOCK_INIT( &this->lock );
	}

	~RemappingRecordMap() {
		Key key;
		std::unordered_map<Key, RemappingRecord>::iterator it, saveptr;
		for ( it = map.begin(), saveptr = map.begin(); it != map.end(); it = saveptr )
		{
			saveptr++;
			key = it->first;
			key.free();
		}
		for ( auto& it : slaveToKeyMap )
			delete it.second;
	}

	bool insert( Key key, RemappingRecord record, struct sockaddr_in slave ) {
		LOCK( &this->lock );
		std::unordered_map<Key, RemappingRecord>::iterator it = this->map.find( key );
		if ( it != map.end() ) {
			// do not allow overwriting of remapping records without delete
			UNLOCK( &this->lock );
			return false;
		} else {
			Key keyDup;
			keyDup.dup( key.size, key.data );
			map[ keyDup ] = record;
			// add mapping from slave to key ( for erase by slave )
			if ( slaveToKeyMap.count( slave ) < 1 )
				slaveToKeyMap[ slave ] = new std::unordered_set<Key>();
			slaveToKeyMap[ slave ]->insert( keyDup );
		}
		UNLOCK( &this->lock );
		return true;
	}

	bool erase( Key key, RemappingRecord record, bool check, bool needsLock, bool needsUnlock ) {
		Key keyDup;
		bool ret;
		if ( needsLock ) LOCK( &this->lock );
		std::unordered_map<Key, RemappingRecord>::iterator it = map.find( key );
		if ( it != map.end() ) {
			keyDup = it->first;
			it->second.free();
			map.erase( it );
			keyDup.free();
			ret = true;
		} else {
			ret = false;
		}
		if ( needsUnlock ) UNLOCK( &this->lock );
		return ret;
	}

	bool erase( struct sockaddr_in slave ) {
		LOCK( &this->lock );
		std::unordered_map<struct sockaddr_in, std::unordered_set<Key>* >::iterator sit = slaveToKeyMap.find( slave );
		RemappingRecord record;
		if ( sit != slaveToKeyMap.end() ) {
			std::unordered_set<Key>::iterator kit, saveptr;
			std::unordered_set<Key>* keys = sit->second;
			for ( kit = keys->begin(), saveptr = keys->begin(); kit != keys->end(); kit = saveptr ) {
				saveptr++;
				this->erase( *kit , record, false, false, false );
			}
			delete keys;
			slaveToKeyMap.erase( sit );
		}
		UNLOCK( &this->lock );
		return true;
	}

	bool find( Key key, RemappingRecord *record = NULL ) {
		bool ret = false;
		LOCK( &this->lock );
		std::unordered_map<Key, RemappingRecord>::iterator it = map.find( key );
		if ( it != map.end() ) {
			if ( record ) *record = it->second;
			ret = true;
		}
		UNLOCK( &this->lock );
		return ret;
	}

	size_t size() {
		size_t ret;
		LOCK( &this->lock );
		ret = this->map.size();
		UNLOCK( &this->lock );
		return ret;
	}

	void print( FILE *f = stderr, bool listAll = false ) {
		std::unordered_map<Key, RemappingRecord>::iterator it;
		unsigned long count = 0;
		fprintf( f, "%lu remapping records\n", map.size() );
		for ( it = map.begin(); it != map.end() && listAll; it++, count++ ) {
			fprintf(
				f , "#%lu: [%.*s](%d) - ",
				count, it->first.size, it->first.data, it->first.size
			);
			RemappingRecord &remappingRecord = it->second;
			for ( uint32_t i = 0; i < remappingRecord.remappedCount; i++ ) {
				fprintf(
					f, "%s(%u, %u) |-> (%u, %u)",
					i == 0 ? "" : "; ",
					remappingRecord.original[ i * 2     ],
					remappingRecord.original[ i * 2 + 1 ],
					remappingRecord.remapped[ i * 2     ],
					remappingRecord.remapped[ i * 2 + 1 ]
				);
			}
			fprintf( f, ".\n" );
		}
	}
};

#endif
