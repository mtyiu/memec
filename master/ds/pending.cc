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

bool Pending::get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, DegradedLock> *&map ) {
	if ( type == PT_SLAVE_DEGRADED_LOCK ) {
		lock = &this->slaves.degradedLockLock;
		map = &this->slaves.degradedLock;
		return true;
	} else {
		lock = 0;
		map = 0;
		return false;
	}
}

Pending::Pending() {
	LOCK_INIT( &this->applications.getLock );
	LOCK_INIT( &this->applications.setLock );
	LOCK_INIT( &this->applications.updateLock );
	LOCK_INIT( &this->applications.delLock );
	LOCK_INIT( &this->slaves.getLock );
	LOCK_INIT( &this->slaves.setLock );
	LOCK_INIT( &this->slaves.remappingSetLock );
	LOCK_INIT( &this->slaves.updateLock );
	LOCK_INIT( &this->slaves.delLock );
	LOCK_INIT( &this->slaves.degradedLockLock );
	LOCK_INIT( &this->stats.getLock );
	LOCK_INIT( &this->stats.setLock );
}

bool Pending::insertKey( PendingType type, uint32_t id, void *ptr, Key &key, bool needsLock, bool needsUnlock ) {
	return this->insertKey( type, id, id, ptr, key, needsLock, needsUnlock );
}

bool Pending::insertKey( PendingType type, uint32_t id, uint32_t parentId, void *ptr, Key &key, bool needsLock, bool needsUnlock ) {
	PendingIdentifier pid( id, parentId, ptr );
	std::pair<PendingIdentifier, Key> p( pid, key );
	std::unordered_multimap<PendingIdentifier, Key>::iterator ret;

	LOCK_T *lock;
	std::unordered_multimap<PendingIdentifier, Key> *map;
	if ( ! this->get( type, lock, map ) )
		return false;

	if ( needsLock ) LOCK( lock );
	ret = map->insert( p );
	if ( needsUnlock ) UNLOCK( lock );

	return true; // ret.second;
}

bool Pending::insertRemappingRecord( PendingType type, uint32_t id, uint32_t parentId, void *ptr, RemappingRecord &remappingRecord, bool needsLock, bool needsUnlock ) {
	PendingIdentifier pid( id, parentId, ptr );
	std::pair<PendingIdentifier, RemappingRecord> p( pid, remappingRecord );
	std::unordered_multimap<PendingIdentifier, RemappingRecord>::iterator ret;

	LOCK_T *lock;
	std::unordered_multimap<PendingIdentifier, RemappingRecord> *map;
	if ( ! this->get( type, lock, map ) )
		return false;

	if ( needsLock ) LOCK( lock );
	ret = map->insert( p );
	if ( needsUnlock ) UNLOCK( lock );

	return true; // ret.second;
}

bool Pending::insertKeyValueUpdate( PendingType type, uint32_t id, void *ptr, KeyValueUpdate &keyValueUpdate, bool needsLock, bool needsUnlock ) {
	return this->insertKeyValueUpdate( type, id, id, ptr, keyValueUpdate, needsLock, needsUnlock );
}

bool Pending::insertKeyValueUpdate( PendingType type, uint32_t id, uint32_t parentId, void *ptr, KeyValueUpdate &keyValueUpdate, bool needsLock, bool needsUnlock ) {
	PendingIdentifier pid( id, parentId, ptr );
	std::pair<PendingIdentifier, KeyValueUpdate> p( pid, keyValueUpdate );
	std::unordered_multimap<PendingIdentifier, KeyValueUpdate>::iterator ret;

	LOCK_T *lock;
	std::unordered_multimap<PendingIdentifier, KeyValueUpdate> *map;
	if ( ! this->get( type, lock, map ) )
		return false;

	if ( needsLock ) LOCK( lock );
	ret = map->insert( p );
	if ( needsUnlock ) UNLOCK( lock );

	return true; // ret.second;
}

bool Pending::insertDegradedLock( PendingType type, uint32_t id, uint32_t parentId, void *ptr, DegradedLock &degradedLock, bool needsLock, bool needsUnlock ) {
	PendingIdentifier pid( id, parentId, ptr );
	std::pair<PendingIdentifier, DegradedLock> p( pid, degradedLock );
	std::unordered_multimap<PendingIdentifier, DegradedLock>::iterator ret;

	LOCK_T *lock;
	std::unordered_multimap<PendingIdentifier, DegradedLock> *map;
	if ( ! this->get( type, lock, map ) )
		return false;

	if ( needsLock ) LOCK( lock );
	ret = map->insert( p );
	if ( needsUnlock ) UNLOCK( lock );

	return true; // ret.second;
}

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

bool Pending::eraseKey( PendingType type, uint32_t id, void *ptr, PendingIdentifier *pidPtr, Key *keyPtr, bool needsLock, bool needsUnlock ) {
	PendingIdentifier pid( id, 0, ptr );
	LOCK_T *lock;
	bool ret;

	std::unordered_multimap<PendingIdentifier, Key> *map;
	std::unordered_multimap<PendingIdentifier, Key>::iterator it;
	if ( ! this->get( type, lock, map ) )
		return false;

	if ( needsLock ) LOCK( lock );
	if ( ptr ) {
		it = map->find( pid );
		ret = ( it != map->end() );
	} else {
		it = map->find( pid );
		ret = ( it != map->end() && it->first.id == id ); // Match request ID
	}
	if ( ret ) {
		if ( pidPtr ) *pidPtr = it->first;
		if ( keyPtr ) *keyPtr = it->second;
		map->erase( it );
	}
	if ( needsUnlock ) UNLOCK( lock );

	return ret;
}

bool Pending::eraseRemappingRecord( PendingType type, uint32_t id, void *ptr, PendingIdentifier *pidPtr, RemappingRecord *remappingRecordPtr, bool needsLock, bool needsUnlock ) {
	PendingIdentifier pid( id, 0, ptr );
	LOCK_T *lock;
	bool ret;

	std::unordered_multimap<PendingIdentifier, RemappingRecord> *map;
	std::unordered_multimap<PendingIdentifier, RemappingRecord>::iterator it;
	if ( ! this->get( type, lock, map ) )
		return false;

	if ( needsLock ) LOCK( lock );
	if ( ptr ) {
		it = map->find( pid );
		ret = ( it != map->end() );
	} else {
		it = map->find( pid );
		ret = ( it != map->end() && it->first.id == id ); // Match request ID
	}
	if ( ret ) {
		if ( pidPtr ) *pidPtr = it->first;
		if ( remappingRecordPtr ) *remappingRecordPtr = it->second;
		map->erase( it );
	}
	if ( needsUnlock ) UNLOCK( lock );

	return ret;
}

bool Pending::eraseKeyValueUpdate( PendingType type, uint32_t id, void *ptr, PendingIdentifier *pidPtr, KeyValueUpdate *keyValueUpdatePtr, bool needsLock, bool needsUnlock ) {
	PendingIdentifier pid( id, 0, ptr );
	LOCK_T *lock;
	bool ret;

	std::unordered_multimap<PendingIdentifier, KeyValueUpdate> *map;
	std::unordered_multimap<PendingIdentifier, KeyValueUpdate>::iterator it;
	if ( ! this->get( type, lock, map ) )
		return false;

	if ( needsLock ) LOCK( lock );
	if ( ptr ) {
		it = map->find( pid );
		ret = ( it != map->end() );
	} else {
		it = map->find( pid );
		ret = ( it != map->end() && it->first.id == id );
	}
	if ( ret ) {
		if ( pidPtr ) *pidPtr = it->first;
		if ( keyValueUpdatePtr ) *keyValueUpdatePtr = it->second;
		map->erase( it );
	}
	if ( needsUnlock ) UNLOCK( lock );

	return ret;
}

bool Pending::eraseDegradedLock( PendingType type, uint32_t id, void *ptr, PendingIdentifier *pidPtr, DegradedLock *degradedLockPtr, bool needsLock, bool needsUnlock ) {
	PendingIdentifier pid( id, 0, ptr );
	LOCK_T *lock;
	bool ret;

	std::unordered_multimap<PendingIdentifier, DegradedLock> *map;
	std::unordered_multimap<PendingIdentifier, DegradedLock>::iterator it;
	if ( ! this->get( type, lock, map ) )
		return false;

	if ( needsLock ) LOCK( lock );
	if ( ptr ) {
		it = map->find( pid );
		ret = ( it != map->end() );
	} else {
		it = map->find( pid );
		ret = ( it != map->end() && it->first.id == id );
	}
	if ( ret ) {
		if ( pidPtr ) *pidPtr = it->first;
		if ( degradedLockPtr ) *degradedLockPtr = it->second;
		map->erase( it );
	}
	if ( needsUnlock ) UNLOCK( lock );

	return ret;
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
