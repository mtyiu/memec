#include "pending.hh"

bool Pending::get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, Key> *&map ) {
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

bool Pending::get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, KeyValueUpdate> *&map ) {
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

bool Pending::get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, RemappingRecord> *&map ) {
	if ( type == PT_SLAVE_REMAPPING_SET ) {
		lock = &this->slaves.remappingSetLock;
		map = &this->slaves.remappingSet;
		return true;
	} else {
		lock = 0;
		map = 0;
		return false;
	}
}

bool Pending::get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, DegradedLockData> *&map ) {
	if ( type == PT_COORDINATOR_DEGRADED_LOCK_DATA ) {
		lock = &this->coordinator.degradedLockDataLock;
		map = &this->coordinator.degradedLockData;
		return true;
	} else {
		lock = 0;
		map = 0;
		return false;
	}
}

Pending::Pending() {
	LOCK_INIT( &this->coordinator.degradedLockDataLock );
	LOCK_INIT( &this->applications.getLock );
	LOCK_INIT( &this->applications.setLock );
	LOCK_INIT( &this->applications.updateLock );
	LOCK_INIT( &this->applications.delLock );
	LOCK_INIT( &this->slaves.getLock );
	LOCK_INIT( &this->slaves.setLock );
	LOCK_INIT( &this->slaves.remappingSetLock );
	LOCK_INIT( &this->slaves.updateLock );
	LOCK_INIT( &this->slaves.delLock );
	LOCK_INIT( &this->stats.getLock );
	LOCK_INIT( &this->stats.setLock );
}

#define DEFINE_PENDING_APPLICATION_INSERT_METHOD( METHOD_NAME, VALUE_TYPE, VALUE_VAR ) \
	bool Pending::METHOD_NAME( PendingType type, uint32_t id, void *ptr, VALUE_TYPE &VALUE_VAR, bool needsLock, bool needsUnlock ) { \
		PendingIdentifier pid( id, id, ptr ); \
		std::pair<PendingIdentifier, VALUE_TYPE> p( pid, VALUE_VAR ); \
		std::unordered_multimap<PendingIdentifier, VALUE_TYPE>::iterator ret; \
 		\
		LOCK_T *lock; \
		std::unordered_multimap<PendingIdentifier, VALUE_TYPE> *map; \
		if ( ! this->get( type, lock, map ) ) \
			return false; \
 		\
		if ( needsLock ) LOCK( lock ); \
		ret = map->insert( p ); \
		if ( needsUnlock ) UNLOCK( lock ); \
 		\
		return true; /* ret.second; */ \
	}

#define DEFINE_PENDING_SLAVE_INSERT_METHOD( METHOD_NAME, VALUE_TYPE, VALUE_VAR ) \
	bool Pending::METHOD_NAME( PendingType type, uint32_t id, uint32_t parentId, void *ptr, VALUE_TYPE &VALUE_VAR, bool needsLock, bool needsUnlock ) { \
		PendingIdentifier pid( id, parentId, ptr ); \
		std::pair<PendingIdentifier, VALUE_TYPE> p( pid, VALUE_VAR ); \
		std::unordered_multimap<PendingIdentifier, VALUE_TYPE>::iterator ret; \
 		\
		LOCK_T *lock; \
		std::unordered_multimap<PendingIdentifier, VALUE_TYPE> *map; \
		if ( ! this->get( type, lock, map ) ) \
			return false; \
 		\
		if ( needsLock ) LOCK( lock ); \
		ret = map->insert( p ); \
		if ( needsUnlock ) UNLOCK( lock ); \
 		\
		return true; /* ret.second; */ \
	}

#define DEFINE_PENDING_COORDINATOR_INSERT_METHOD DEFINE_PENDING_SLAVE_INSERT_METHOD

#define DEFINE_PENDING_ERASE_METHOD( METHOD_NAME, VALUE_TYPE, VALUE_PTR_VAR ) \
	bool Pending::METHOD_NAME( PendingType type, uint32_t id, void *ptr, PendingIdentifier *pidPtr, VALUE_TYPE *VALUE_PTR_VAR, bool needsLock, bool needsUnlock ) { \
		PendingIdentifier pid( id, 0, ptr ); \
		LOCK_T *lock; \
		bool ret; \
		\
		std::unordered_multimap<PendingIdentifier, VALUE_TYPE> *map; \
		std::unordered_multimap<PendingIdentifier, VALUE_TYPE>::iterator it; \
		if ( ! this->get( type, lock, map ) ) \
			return false; \
		\
		if ( needsLock ) LOCK( lock ); \
		if ( ptr ) { \
			it = map->find( pid ); \
			ret = ( it != map->end() ); \
		} else { \
			it = map->find( pid ); \
			ret = ( it != map->end() && it->first.id == id ); \
		} \
		if ( ret ) { \
			if ( pidPtr ) *pidPtr = it->first; \
			if ( VALUE_PTR_VAR ) *VALUE_PTR_VAR = it->second; \
			map->erase( it ); \
		} \
		if ( needsUnlock ) UNLOCK( lock ); \
		\
		return ret; \
	}

DEFINE_PENDING_COORDINATOR_INSERT_METHOD( insertDegradedLockData, DegradedLockData, degradedLockData )

DEFINE_PENDING_APPLICATION_INSERT_METHOD( insertKey, Key, key )
DEFINE_PENDING_APPLICATION_INSERT_METHOD( insertKeyValueUpdate, KeyValueUpdate, keyValueUpdate )

DEFINE_PENDING_SLAVE_INSERT_METHOD( insertKey, Key, key )
DEFINE_PENDING_SLAVE_INSERT_METHOD( insertRemappingRecord, RemappingRecord, remappingRecord )
DEFINE_PENDING_SLAVE_INSERT_METHOD( insertKeyValueUpdate, KeyValueUpdate, keyValueUpdate )

DEFINE_PENDING_ERASE_METHOD( eraseDegradedLockData, DegradedLockData, degradedLockDataPtr )
DEFINE_PENDING_ERASE_METHOD( eraseKey, Key, keyPtr )
DEFINE_PENDING_ERASE_METHOD( eraseRemappingRecord, RemappingRecord, remappingRecordPtr )
DEFINE_PENDING_ERASE_METHOD( eraseKeyValueUpdate, KeyValueUpdate, KeyValueUpdatePtr )

#undef DEFINE_PENDING_APPLICATION_INSERT_METHOD
#undef DEFINE_PENDING_SLAVE_INSERT_METHOD
#undef DEFINE_PENDING_COORDINATOR_INSERT_METHOD
#undef DEFINE_PENDING_ERASE_METHOD

bool Pending::recordRequestStartTime( PendingType type, uint32_t id, uint32_t parentId, void *ptr, struct sockaddr_in addr ) {
	RequestStartTime rst;
	rst.addr = addr;
	clock_gettime( CLOCK_REALTIME, &rst.sttime );

	PendingIdentifier pid( id, parentId, ptr );

	std::pair<PendingIdentifier, RequestStartTime> p( pid, rst );
	std::unordered_multimap<PendingIdentifier, RequestStartTime>::iterator ret;

	if ( type == PT_SLAVE_GET ) {
		LOCK( &this->stats.getLock );
		ret = this->stats.get.insert( p );
		UNLOCK( &this->stats.getLock );
	} else if ( type == PT_SLAVE_SET ) {
		LOCK( &this->stats.setLock );
		ret = this->stats.set.insert( p );
		UNLOCK( &this->stats.setLock );
	} else {
		return false;
	}

	return true; // ret.second;
}

bool Pending::eraseRequestStartTime( PendingType type, uint32_t id, void *ptr, struct timespec &elapsedTime, PendingIdentifier *pidPtr, RequestStartTime *rstPtr ) {
	PendingIdentifier pid( id, 0, ptr );
	std::unordered_multimap<PendingIdentifier, RequestStartTime>::iterator it;
	RequestStartTime rst;
	bool ret;

	if ( type == PT_SLAVE_GET ) {
		LOCK( &this->stats.getLock );
		it = this->stats.get.find( pid );
		ret = ( it != this->stats.get.end() );
		if ( ret ) {
			pid = it->first;
			rst = it->second;
			if ( pidPtr ) *pidPtr = pid;
			if ( rstPtr ) *rstPtr = rst;
			this->stats.get.erase( it );
		}
		UNLOCK( &this->stats.getLock );
	} else if ( type == PT_SLAVE_SET ) {
		LOCK( &this->stats.setLock );
		it = this->stats.set.find( pid );
		ret = ( it != this->stats.set.end() );
		if ( ret ) {
			pid = it->first;
			rst = it->second;
			if ( pidPtr ) *pidPtr = pid;
			if ( rstPtr ) *rstPtr = rst;
			this->stats.set.erase( it );
		}
		UNLOCK( &this->stats.setLock );
	} else {
		return false;
	}

	if ( ret ) {
		struct timespec currentTime;
		clock_gettime( CLOCK_REALTIME, &currentTime );
		elapsedTime.tv_sec = currentTime.tv_sec - rst.sttime.tv_sec;
		//fprintf( stderr, "from %lu.%lu to %lu.%lu\n", rst.sttime.tv_sec, rst.sttime.tv_nsec, currentTime.tv_sec, currentTime.tv_nsec );
		if ( ( long long )currentTime.tv_nsec - rst.sttime.tv_nsec < 0 ) {
			elapsedTime.tv_sec -= 1;
			elapsedTime.tv_nsec = GIGA - rst.sttime.tv_nsec + currentTime.tv_nsec;
		} else {
			elapsedTime.tv_nsec = currentTime.tv_nsec- rst.sttime.tv_nsec;
		}
	} else {
		elapsedTime.tv_sec = 0;
		elapsedTime.tv_nsec = 0;
	}
	return ret;
}

bool Pending::findKey( PendingType type, uint32_t id, void *ptr, Key *keyPtr ) {
	PendingIdentifier pid( id, 0, ptr );
	LOCK_T *lock;
	bool ret;

	std::unordered_multimap<PendingIdentifier, Key> *map;
	std::unordered_multimap<PendingIdentifier, Key>::iterator it;
	if ( ! this->get( type, lock, map ) )
		return false;

	LOCK( lock );
	if ( ptr ) {
		it = map->find( pid );
		ret = ( it != map->end() );
	} else {
		it = map->find( pid );
		ret = ( it != map->end() && it->first.id == id ); // Match request ID
	}
	if ( ret ) {
		if ( keyPtr ) *keyPtr = it->second;
	}
	UNLOCK( lock );

	return ret;
}

bool Pending::findKeyValueUpdate( PendingType type, uint32_t id, void *ptr, KeyValueUpdate *keyValuePtr ) {
	PendingIdentifier pid( id, 0, ptr );
	LOCK_T *lock;
	bool ret;

	std::unordered_multimap<PendingIdentifier, KeyValueUpdate> *map;
	std::unordered_multimap<PendingIdentifier, KeyValueUpdate>::iterator it;
	if ( ! this->get( type, lock, map ) )
		return false;

	LOCK( lock );
	if ( ptr ) {
		it = map->find( pid );
		ret = ( it != map->end() );
	} else {
		it = map->find( pid );
		ret = ( it != map->end() && it->first.id == id ); // Match request ID
	}
	if ( ret ) {
		if ( keyValuePtr ) *keyValuePtr = it->second;
	}
	UNLOCK( lock );

	return ret;
}

uint32_t Pending::count( PendingType type, uint32_t id, bool needsLock, bool needsUnlock ) {
	PendingIdentifier pid( id, 0, 0 );
	LOCK_T *lock;
	uint32_t ret = 0;
	if ( type == PT_APPLICATION_UPDATE || type == PT_SLAVE_UPDATE ) {
		std::unordered_multimap<PendingIdentifier, KeyValueUpdate> *map;
		std::unordered_multimap<PendingIdentifier, KeyValueUpdate>::iterator it;

		if ( ! this->get( type, lock, map ) ) return 0;

		if ( needsLock ) LOCK( lock );
		ret = map->count( pid );
		// it = map->lower_bound( pid );
		// for ( ret = 0; it != map->end() && it->first.id == id; ret++, it++ );
		if ( needsUnlock ) UNLOCK( lock );
	} else if ( type == PT_SLAVE_REMAPPING_SET ) {
		std::unordered_multimap<PendingIdentifier, RemappingRecord> *map;
		std::unordered_multimap<PendingIdentifier, RemappingRecord>::iterator it;

		if ( ! this->get( type, lock, map ) ) return 0;

		if ( needsLock ) LOCK( lock );
		ret = map->count( pid );
		// it = map->lower_bound( pid );
		// for ( ret = 0; it != map->end() && it->first.id == id; ret++, it++ );
		if ( needsUnlock ) UNLOCK( lock );
	} else {
		std::unordered_multimap<PendingIdentifier, Key> *map;
		std::unordered_multimap<PendingIdentifier, Key>::iterator it;

		if ( ! this->get( type, lock, map ) ) return 0;

		if ( needsLock ) LOCK( lock );
		ret = map->count( pid );
		// it = map->lower_bound( pid );
		// for ( ret = 0; it != map->end() && it->first.id == id; ret++, it++ );
		if ( needsUnlock ) UNLOCK( lock );
	}

	return ret;
}
