#include "pending.hh"

bool Pending::get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, Key> *&map ) {
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

bool Pending::get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, KeyValueUpdate> *&map ) {
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

Pending::Pending() {
	pthread_mutex_init( &this->applications.getLock, 0 );
	pthread_mutex_init( &this->applications.setLock, 0 );
	pthread_mutex_init( &this->applications.updateLock, 0 );
	pthread_mutex_init( &this->applications.delLock, 0 );
	pthread_mutex_init( &this->slaves.getLock, 0 );
	pthread_mutex_init( &this->slaves.setLock, 0 );
	pthread_mutex_init( &this->slaves.updateLock, 0 );
	pthread_mutex_init( &this->slaves.delLock, 0 );
}

bool Pending::insertKey( PendingType type, uint32_t id, void *ptr, Key &key, bool needsLock, bool needsUnlock ) {
	return this->insertKey( type, id, id, ptr, key, needsLock, needsUnlock );
}

bool Pending::insertKey( PendingType type, uint32_t id, uint32_t parentId, void *ptr, Key &key, bool needsLock, bool needsUnlock ) {
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

bool Pending::insertKeyValueUpdate( PendingType type, uint32_t id, void *ptr, KeyValueUpdate &keyValueUpdate, bool needsLock, bool needsUnlock ) {
	return this->insertKeyValueUpdate( type, id, id, ptr, keyValueUpdate, needsLock, needsUnlock );
}

bool Pending::insertKeyValueUpdate( PendingType type, uint32_t id, uint32_t parentId, void *ptr, KeyValueUpdate &keyValueUpdate, bool needsLock, bool needsUnlock ) {
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

bool Pending::recordRequestStartTime( PendingType type, uint32_t id, uint32_t parentId, void *ptr, struct sockaddr_in addr ) {
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

bool Pending::eraseKey( PendingType type, uint32_t id, void *ptr, PendingIdentifier *pidPtr, Key *keyPtr, bool needsLock, bool needsUnlock ) {
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

bool Pending::eraseKeyValueUpdate( PendingType type, uint32_t id, void *ptr, PendingIdentifier *pidPtr, KeyValueUpdate *keyValueUpdatePtr, bool needsLock, bool needsUnlock ) {
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

bool Pending::eraseRequestStartTime( PendingType type, uint32_t id, void *ptr, struct timespec &elapsedTime, PendingIdentifier *pidPtr, RequestStartTime *rstPtr ) {
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
