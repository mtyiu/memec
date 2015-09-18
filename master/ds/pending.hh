#ifndef __MASTER_DS_PENDING_HH__
#define __MASTER_DS_PENDING_HH__

#include <map>
#include <set>
#include <cstring>
#include <pthread.h>
#include <netinet/in.h>
#include "stats.hh"
#include "../../common/ds/pending.hh"

#define GIGA ( 1000 * 1000 * 1000 )

enum PendingType {
	PT_APPLICATION_GET,
	PT_APPLICATION_SET,
	PT_APPLICATION_UPDATE,
	PT_APPLICATION_DEL,
	PT_SLAVE_GET,
	PT_SLAVE_SET,
	PT_SLAVE_UPDATE,
	PT_SLAVE_DEL
};

class Pending {
private:
	bool get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, Key> *&map ) {
		switch( type ) {
			case PT_APPLICATION_GET:
				lock = &this->applications.getLock;
				map = &this->applications.get;
				break;
			case PT_APPLICATION_SET:
				lock = &this->applications.setLock;
				map = &this->applications.set;
				break;
			case PT_APPLICATION_DEL:
				lock = &this->applications.delLock;
				map = &this->applications.del;
				break;
			case PT_SLAVE_GET:
				lock = &this->slaves.getLock;
				map = &this->slaves.get;
				break;
			case PT_SLAVE_SET:
				lock = &this->slaves.setLock;
				map = &this->slaves.set;
				break;
			case PT_SLAVE_DEL:
				lock = &this->slaves.delLock;
				map = &this->slaves.del;
				break;
			default:
				lock = 0;
				map = 0;
				return false;
		}
		return true;
	}

	bool get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, KeyValueUpdate> *&map ) {
		switch( type ) {
			case PT_APPLICATION_UPDATE:
				lock = &this->applications.updateLock;
				map = &this->applications.update;
				break;
			case PT_SLAVE_UPDATE:
				lock = &this->slaves.updateLock;
				map = &this->slaves.update;
				break;
			default:
				lock = 0;
				map = 0;
				return false;
		}
		return true;
	}

public:
	struct {
		std::map<PendingIdentifier, Key> get;
		std::map<PendingIdentifier, Key> set;
		std::map<PendingIdentifier, KeyValueUpdate> update;
		std::map<PendingIdentifier, Key> del;
		pthread_mutex_t getLock;
		pthread_mutex_t setLock;
		pthread_mutex_t updateLock;
		pthread_mutex_t delLock;
	} applications;
	struct {
		std::map<PendingIdentifier, Key> get;
		std::map<PendingIdentifier, Key> set;
		std::map<PendingIdentifier, KeyValueUpdate> update;
		std::map<PendingIdentifier, Key> del;
		pthread_mutex_t getLock;
		pthread_mutex_t setLock;
		pthread_mutex_t updateLock;
		pthread_mutex_t delLock;
	} slaves;
	struct {
		std::map<PendingIdentifier, RequestStartTime> get;
		std::map<PendingIdentifier, RequestStartTime> set;
		pthread_mutex_t getLock;
		pthread_mutex_t setLock;
	} stats;

	Pending() {
		pthread_mutex_init( &this->applications.getLock, 0 );
		pthread_mutex_init( &this->applications.setLock, 0 );
		pthread_mutex_init( &this->applications.updateLock, 0 );
		pthread_mutex_init( &this->applications.delLock, 0 );
		pthread_mutex_init( &this->slaves.getLock, 0 );
		pthread_mutex_init( &this->slaves.setLock, 0 );
		pthread_mutex_init( &this->slaves.updateLock, 0 );
		pthread_mutex_init( &this->slaves.delLock, 0 );
	}

	bool insert( PendingType type, uint32_t id, void *ptr, Key &key, bool needsLock = true, bool needsUnlock = true ) {
		return this->insert( type, id, id, ptr, key, needsLock, needsUnlock );
	}

	bool insert( PendingType type, uint32_t id, uint32_t parentId, void *ptr, Key &key, bool needsLock = true, bool needsUnlock = true ) {
		PendingIdentifier pid( id, parentId, ptr );
		std::pair<PendingIdentifier, Key> p( pid, key );
		std::pair<std::map<PendingIdentifier, Key>::iterator, bool> ret;

		pthread_mutex_t *lock;
		std::map<PendingIdentifier, Key> *map;
		if ( ! this->get( type, lock, map ) )
			return false;

		if ( needsLock ) pthread_mutex_lock( lock );
		ret = map->insert( p );
		if ( needsUnlock ) pthread_mutex_unlock( lock );

		return ret.second;
	}

	bool insert( PendingType type, uint32_t id, void *ptr, KeyValueUpdate &keyValueUpdate, bool needsLock = true, bool needsUnlock = true ) {
		return this->insert( type, id, id, ptr, keyValueUpdate, needsLock, needsUnlock );
	}

	bool insert( PendingType type, uint32_t id, uint32_t parentId, void *ptr, KeyValueUpdate &keyValueUpdate, bool needsLock = true, bool needsUnlock = true ) {
		PendingIdentifier pid( id, parentId, ptr );
		std::pair<PendingIdentifier, KeyValueUpdate> p( pid, keyValueUpdate );
		std::pair<std::map<PendingIdentifier, KeyValueUpdate>::iterator, bool> ret;

		pthread_mutex_t *lock;
		std::map<PendingIdentifier, KeyValueUpdate> *map;
		if ( ! this->get( type, lock, map ) )
			return false;

		if ( needsLock ) pthread_mutex_lock( lock );
		ret = map->insert( p );
		if ( needsUnlock ) pthread_mutex_unlock( lock );

		return ret.second;
	}

	bool recordRequestStartTime( PendingType type, uint32_t id, uint32_t parentId, void *ptr, struct sockaddr_in addr ) {
		RequestStartTime rst;
		rst.addr = addr;
		clock_gettime( CLOCK_REALTIME, &rst.sttime );

		PendingIdentifier pid( id, parentId, ptr );

		std::pair<PendingIdentifier, RequestStartTime> p( pid, rst );
		std::pair<std::map<PendingIdentifier, RequestStartTime>::iterator, bool> ret;

		if ( type == PT_SLAVE_GET ) {
			pthread_mutex_lock( &this->stats.getLock );
			ret = this->stats.get.insert( p );
			pthread_mutex_unlock( &this->stats.getLock );
		} else if ( type == PT_SLAVE_SET ) {
			pthread_mutex_lock( &this->stats.setLock );
			ret = this->stats.set.insert( p );
			pthread_mutex_unlock( &this->stats.setLock );
		} else {
			return false;
		}

		return ret.second;
	}

	bool eraseKey( PendingType type, uint32_t id, void *ptr = 0, PendingIdentifier *pidPtr = 0, Key *keyPtr = 0, bool needsLock = true, bool needsUnlock = true ) {
		PendingIdentifier pid( id, 0, ptr );
		pthread_mutex_t *lock;
		bool ret;

		std::map<PendingIdentifier, Key> *map;
		std::map<PendingIdentifier, Key>::iterator it;
		if ( ! this->get( type, lock, map ) )
			return false;

		if ( needsLock ) pthread_mutex_lock( lock );
		if ( ptr ) {
			it = map->find( pid );
			ret = ( it != map->end() );
		} else {
			it = map->lower_bound( pid );
			ret = ( it != map->end() && it->first.id == id ); // Match request ID
		}
		if ( ret ) {
			if ( pidPtr ) *pidPtr = it->first;
			if ( keyPtr ) *keyPtr = it->second;
			map->erase( it );
		}
		if ( needsUnlock ) pthread_mutex_unlock( lock );

		return ret;
	}

	bool eraseKeyValueUpdate( PendingType type, uint32_t id, void *ptr = 0, PendingIdentifier *pidPtr = 0, KeyValueUpdate *keyValueUpdatePtr = 0, bool needsLock = true, bool needsUnlock = true ) {
		PendingIdentifier pid( id, 0, ptr );
		pthread_mutex_t *lock;
		bool ret;

		std::map<PendingIdentifier, KeyValueUpdate> *map;
		std::map<PendingIdentifier, KeyValueUpdate>::iterator it;
		if ( ! this->get( type, lock, map ) )
			return false;

		if ( needsLock ) pthread_mutex_lock( lock );
		if ( ptr ) {
			it = map->find( pid );
			ret = ( it != map->end() );
		} else {
			it = map->lower_bound( pid );
			ret = ( it != map->end() && it->first.id == id );
		}
		if ( ret ) {
			if ( pidPtr ) *pidPtr = it->first;
			if ( keyValueUpdatePtr ) *keyValueUpdatePtr = it->second;
			map->erase( it );
		}
		if ( needsUnlock ) pthread_mutex_unlock( lock );

		return ret;
	}

	bool eraseRequestStartTime( PendingType type, uint32_t id, void *ptr, double &elapsedTime, PendingIdentifier *pidPtr = 0, RequestStartTime *rstPtr = 0 ) {
		PendingIdentifier pid( id, 0, ptr );
		std::map<PendingIdentifier, RequestStartTime>::iterator it;
		RequestStartTime rst;
		bool ret;

		if ( type == PT_SLAVE_GET ) {
			pthread_mutex_lock( &this->stats.getLock );
			it = this->stats.get.find( pid );
			ret = ( it != this->stats.get.end() );
			if ( ret ) {
				pid = it->first;
				rst = it->second;
				if ( pidPtr ) *pidPtr = pid;
				if ( rstPtr ) *rstPtr = rst;
				this->stats.get.erase( it );
			}
			pthread_mutex_unlock( &this->stats.getLock );
		} else if ( type == PT_SLAVE_SET ) {
			pthread_mutex_lock( &this->stats.setLock );
			it = this->stats.set.find( pid );
			ret = ( it != this->stats.set.end() );
			if ( ret ) {
				pid = it->first;
				rst = it->second;
				if ( pidPtr ) *pidPtr = pid;
				if ( rstPtr ) *rstPtr = rst;
				this->stats.set.erase( it );
			}
			pthread_mutex_unlock( &this->stats.setLock );
		} else {
			return false;
		}

		if ( ret ) {
			struct timespec currentTime;
			clock_gettime( CLOCK_REALTIME, &currentTime );
			elapsedTime = currentTime.tv_sec - rst.sttime.tv_sec + ( currentTime.tv_nsec - rst.sttime.tv_nsec ) / GIGA;
		} else {
			elapsedTime = 0.0;
		}
		return ret;
	}

	uint32_t count( PendingType type, uint32_t id, bool needsLock = true, bool needsUnlock = true ) {
		PendingIdentifier pid( id, 0, 0 );
		pthread_mutex_t *lock;
		uint32_t ret = 0;
		if ( type == PT_APPLICATION_UPDATE || type == PT_SLAVE_UPDATE ) {
			std::map<PendingIdentifier, KeyValueUpdate> *map;
			std::map<PendingIdentifier, KeyValueUpdate>::iterator it;

			if ( ! this->get( type, lock, map ) ) return 0;

			if ( needsLock ) pthread_mutex_lock( lock );
			it = map->lower_bound( pid );
			for ( ret = 0; it != map->end() && it->first.id == id; ret++, it++ );
			if ( needsUnlock ) pthread_mutex_unlock( lock );
		} else {
			std::map<PendingIdentifier, Key> *map;
			std::map<PendingIdentifier, Key>::iterator it;

			if ( ! this->get( type, lock, map ) ) return 0;

			if ( needsLock ) pthread_mutex_lock( lock );
			it = map->lower_bound( pid );
			for ( ret = 0; it != map->end() && it->first.id == id; ret++, it++ );
			if ( needsUnlock ) pthread_mutex_unlock( lock );
		}

		return ret;
	}
};

#endif
