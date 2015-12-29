#ifndef __COORDINATOR_DS_REMAPPING_RECORD_MAP_HH__
#define __COORDINATOR_DS_REMAPPING_RECORD_MAP_HH__

#include <unordered_map>
#include <unordered_set>
#include <pthread.h>
#include <assert.h>
#include "key.hh"
#include "metadata.hh"
#include "../ds/sockaddr_in.hh"
#include "../lock/lock.hh"
#include "../stripe_list/stripe_list.hh"

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
			if ( record != it->second ) {
				UNLOCK( &this->lock );
				return false;
			}
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
		if ( needsLock ) LOCK( &this->lock );
		std::unordered_map<Key, RemappingRecord>::iterator it = map.find( key );
		if ( it != map.end() ) {
			// do not allow deleting key records if they are different
			if ( check && record != it->second ) {
				if ( needsUnlock ) UNLOCK( &this->lock );
				return false;
			}
			keyDup = it->first;
			map.erase( it );
			keyDup.free();
		}
		if ( needsUnlock ) UNLOCK( &this->lock );
		return true;
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
				f , "%lu: [%.*s](%d) to list %u chunk %u\n",
				count, it->first.size, it->first.data, it->first.size,
				it->second.listId, it->second.chunkId
			);
		}
	}
};

#endif
