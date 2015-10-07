#ifndef __COORDINATOR_DS_REMAPPING_RECORD_MAP_HH__
#define __COORDINATOR_DS_REMAPPING_RECORD_MAP_HH__

#include <map>
#include <pthread.h>
#include "key.hh"
#include "metadata.hh"

class RemappingRecordMap {
private:
	std::map<Key, RemappingRecord> map;
	pthread_mutex_t lock;

public:
	RemappingRecordMap() {
		this->map.clear();
		pthread_mutex_init( &this->lock, 0 );
	}

	bool insert( Key key, RemappingRecord record ) {
		pthread_mutex_lock ( &this->lock );
		std::map<Key, RemappingRecord>::iterator it = map.find( key );
		if ( it != map.end() ) {
			// do not allow overwriting of remapping records without delete
			if ( record != it->second ) {
				pthread_mutex_unlock ( &this->lock );
				return false;
			}
		} else {
			Key keyDup;
			keyDup.dup( key.size, key.data );
			map[ keyDup ] = record;
		}
		pthread_mutex_unlock ( &this->lock );
		return true;
	}

	bool erase( Key key, RemappingRecord record ) {
		pthread_mutex_lock ( &this->lock );
		std::map<Key, RemappingRecord>::iterator it = map.find( key );
		if ( it != map.end() ) {
			// do not allow deleting key records if they are different
			if ( record != it->second ) {
				pthread_mutex_unlock ( &this->lock );
				return false;
			}
			map.erase( it );
		}
		pthread_mutex_unlock ( &this->lock );
		return true;
	}

	RemappingRecord find( Key key ) {
		RemappingRecord ret;
		pthread_mutex_lock( &this->lock );
		std::map<Key, RemappingRecord>::iterator it = map.find( key );
		if ( it != map.end() ) {
			ret = it->second;
			ret.valid = true;
		}
		pthread_mutex_unlock( &this->lock );
		return ret;
	}

	void print( FILE *f = stderr, bool listAll = false ) {
		std::map<Key, RemappingRecord>::iterator it;
		unsigned long count = 0;
		fprintf( f, "%lu remppaing records\n", map.size() );
		for ( it = map.begin(); it != map.end() && listAll; it++, count++ ) {
			fprintf( 
				f , "%lu: [%s](%d) to list %u chunk %u\n", 
				count, it->first.data, it->first.size,
				it->second.listId, it->second.chunkId 
			);
		}
	}
};

#endif
