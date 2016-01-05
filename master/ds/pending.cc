#include "pending.hh"

bool Pending::get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, Key> *&map ) {
	switch( type ) {
		case PT_APPLICATION_GET:
			lock = &this->applications.getLock;
			map = &this->applications.get;
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

bool Pending::get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, KeyValue> *&map ) {
	switch( type ) {
		case PT_APPLICATION_SET:
			lock = &this->applications.setLock;
			map = &this->applications.set;
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

bool Pending::get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, RemapList> *&map ) {
	if ( type == PT_KEY_REMAP_LIST ) {
		lock = &this->requests.remapListLock;
		map = &this->requests.remapList;
		return true;
	} else {
		lock = 0;
		map = 0;
		return false;
	}
}

bool Pending::get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, AcknowledgementInfo > *&map ) {
	if ( type == PT_ACK_REVERT_PARITY ) {
		lock = &this->ack.revertLock;
		map = &this->ack.revert;
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
	LOCK_INIT( &this->slaves.updateLock );
	LOCK_INIT( &this->slaves.delLock );
	LOCK_INIT( &this->stats.getLock );
	LOCK_INIT( &this->stats.setLock );
	LOCK_INIT( &this->requests.remapListLock );
	LOCK_INIT( &this->ack.revertLock );
}

#define DEFINE_PENDING_APPLICATION_INSERT_METHOD( METHOD_NAME, VALUE_TYPE, VALUE_VAR ) \
	bool Pending::METHOD_NAME( PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr, VALUE_TYPE &VALUE_VAR, bool needsLock, bool needsUnlock, uint32_t timestamp ) { \
		PendingIdentifier pid( instanceId, instanceId, requestId, requestId, timestamp, ptr ); \
		std::pair<PendingIdentifier, VALUE_TYPE> p( pid, VALUE_VAR ); \
		std::unordered_multimap<PendingIdentifier, VALUE_TYPE>::iterator ret; \
 		\
		LOCK_T *lock; \
		std::unordered_multimap<PendingIdentifier, VALUE_TYPE> *map; \
		if ( ! this->get( type, lock, map ) ) { \
			printf( "Cannot find lock & map.\n" ); \
			return false; \
		} \
 		\
		if ( needsLock ) LOCK( lock ); \
		ret = map->insert( p ); \
		if ( needsUnlock ) UNLOCK( lock ); \
 		\
		return true; /* ret.second; */ \
	}

#define DEFINE_PENDING_ACK_INSERT_METHOD DEFINE_PENDING_APPLICATION_INSERT_METHOD

#define DEFINE_PENDING_SLAVE_INSERT_METHOD( METHOD_NAME, VALUE_TYPE, VALUE_VAR ) \
	bool Pending::METHOD_NAME( PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr, VALUE_TYPE &VALUE_VAR, bool needsLock, bool needsUnlock ) { \
		PendingIdentifier pid( instanceId, parentInstanceId, requestId, parentRequestId, ptr ); \
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
	bool Pending::METHOD_NAME( PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr, PendingIdentifier *pidPtr, VALUE_TYPE *VALUE_PTR_VAR, bool needsLock, bool needsUnlock ) { \
		PendingIdentifier pid( instanceId, 0, requestId, 0, ptr ); \
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
			ret = ( it != map->end() && it->first.instanceId == instanceId && it->first.requestId == requestId ); \
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
DEFINE_PENDING_COORDINATOR_INSERT_METHOD( insertRemapList, RemapList, remapList );

DEFINE_PENDING_APPLICATION_INSERT_METHOD( insertKey, Key, key )
DEFINE_PENDING_APPLICATION_INSERT_METHOD( insertKeyValue, KeyValue, keyValue )
DEFINE_PENDING_APPLICATION_INSERT_METHOD( insertKeyValueUpdate, KeyValueUpdate, keyValueUpdate )

DEFINE_PENDING_SLAVE_INSERT_METHOD( insertKey, Key, key )
DEFINE_PENDING_SLAVE_INSERT_METHOD( insertKeyValueUpdate, KeyValueUpdate, keyValueUpdate )

DEFINE_PENDING_ACK_INSERT_METHOD( insertAck, AcknowledgementInfo, ackInfo )

DEFINE_PENDING_ERASE_METHOD( eraseDegradedLockData, DegradedLockData, degradedLockDataPtr )
DEFINE_PENDING_ERASE_METHOD( eraseRemapList, RemapList, remapList )
DEFINE_PENDING_ERASE_METHOD( eraseAck, AcknowledgementInfo , ackInfoPtr )

#undef DEFINE_PENDING_APPLICATION_INSERT_METHOD
#undef DEFINE_PENDING_SLAVE_INSERT_METHOD
#undef DEFINE_PENDING_COORDINATOR_INSERT_METHOD
#undef DEFINE_PENDING_ERASE_METHOD

bool Pending::recordRequestStartTime( PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr, struct sockaddr_in addr ) {
	RequestStartTime rst;
	rst.addr = addr;
	clock_gettime( CLOCK_REALTIME, &rst.sttime );

	PendingIdentifier pid( instanceId, parentInstanceId, requestId, parentRequestId, ptr );

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

bool Pending::eraseRequestStartTime( PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr, struct timespec &elapsedTime, PendingIdentifier *pidPtr, RequestStartTime *rstPtr ) {
	PendingIdentifier pid( instanceId, 0, requestId, 0, ptr );
	std::unordered_multimap<PendingIdentifier, RequestStartTime>::iterator it, rit;
	RequestStartTime rst;
	bool ret;

#define DO_SEARCH_FOR_ID( _TYPE_ ) \
	do { \
		tie( it, rit ) = this->stats._TYPE_.equal_range( pid ); \
		while( it != rit && ptr && it->first.instanceId == instanceId && it->first.requestId == requestId && it->first.ptr != ptr ) it++; \
		ret = ( it != rit && it->first.instanceId == instanceId && it->first.requestId == requestId && ( ! ptr || it->first.ptr == ptr ) ); \
		if ( ret ) { \
			pid = it->first; \
			rst = it->second; \
			if ( pidPtr ) *pidPtr = pid; \
			if ( rstPtr ) *rstPtr = rst; \
			this->stats._TYPE_.erase( it ); \
		} \
	} while (0)

	if ( type == PT_SLAVE_GET ) {
		LOCK( &this->stats.getLock );
		DO_SEARCH_FOR_ID( get );
		UNLOCK( &this->stats.getLock );
	} else if ( type == PT_SLAVE_SET ) {
		LOCK( &this->stats.setLock );
		DO_SEARCH_FOR_ID( set );
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

#define SEARCH_KEY_RANGE( _MAP_, _LEFT_, _RIGHT_, _PTR_, _CHECK_KEY_, _KEY_PTR_ , _RET_ ) \
	do { \
		/* prevent collision on id, use key and/or ptr as identifier as well */ \
		if ( _PTR_ ) { \
			while ( _LEFT_ != _RIGHT_ && ( _LEFT_->first.ptr != ptr || ( _CHECK_KEY_ && _KEY_PTR_ && strncmp( _KEY_PTR_, _LEFT_->second.data, _LEFT_->second.size ) != 0 ) ) ) \
				_LEFT_++; \
			/* match request id, ptr and/or key */ \
			_RET_ = ( _LEFT_ != _RIGHT_ && _LEFT_->first.ptr == ptr && ( ! _CHECK_KEY_ || ! _KEY_PTR_ || strncmp( _KEY_PTR_, _LEFT_->second.data, _LEFT_->second.size ) == 0 ) ); \
		} else { \
			while ( _LEFT_ != _RIGHT_ && _CHECK_KEY_ && _KEY_PTR_ && strncmp( _KEY_PTR_, _LEFT_->second.data, _LEFT_->second.size ) != 0 ) \
				_LEFT_++; \
			/* match request and/or key */ \
			_RET_ = ( _LEFT_ != _RIGHT_ && ( ! _CHECK_KEY_ || ! _KEY_PTR_ || strncmp( _KEY_PTR_, _LEFT_->second.data, _LEFT_->second.size ) == 0 ) ); \
		} \
	} while ( 0 )

bool Pending::eraseKey( PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr, PendingIdentifier *pidPtr, Key *keyPtr, bool needsLock, bool needsUnlock, bool checkKey, char* checkKeyPtr ) {
	PendingIdentifier pid( instanceId, 0, requestId, 0, ptr );
	LOCK_T *lock;
	bool ret;

	std::unordered_multimap<PendingIdentifier, Key> *map;
	std::unordered_multimap<PendingIdentifier, Key>::iterator lit, rit;
	if ( ! this->get( type, lock, map ) )
		return false;

	if ( needsLock ) LOCK( lock );
	tie( lit, rit ) = map->equal_range( pid );
	SEARCH_KEY_RANGE( map, lit, rit, ptr, checkKey, checkKeyPtr, ret );

	if ( ret ) {
		if ( pidPtr ) *pidPtr = lit->first;
		if ( keyPtr ) *keyPtr = lit->second;
		map->erase( lit );
	}
	if ( needsUnlock ) UNLOCK( lock );

	return ret;
}

bool Pending::eraseKeyValue( PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr, PendingIdentifier *pidPtr, KeyValue *keyValuePtr, bool needsLock, bool needsUnlock, bool checkKey, char* checkKeyPtr ) {
	PendingIdentifier pid( instanceId, 0, requestId, 0, ptr );
	LOCK_T *lock;
	bool ret;

	std::unordered_multimap<PendingIdentifier, KeyValue> *map;
	std::unordered_multimap<PendingIdentifier, KeyValue>::iterator lit, rit;
	if ( ! this->get( type, lock, map ) )
		return false;

	if ( needsLock ) LOCK( lock );
	tie( lit, rit ) = map->equal_range( pid );
	Key key;
	/* prevent collision on id, use key and/or ptr as identifier as well */
	if ( ptr ) {
		while ( lit != rit ) {
			if ( lit->first.ptr == ptr ) {
				if ( ! checkKey || ! checkKeyPtr )
					break;
				key = lit->second.key();
				if ( strncmp( checkKeyPtr, key.data, key.size ) == 0 )
					break;
			}
			lit++;
		}
		/* match request id, ptr and/or key */
		ret = false;
		if ( lit != rit && lit->first.ptr == ptr ) {
			if ( ! checkKey || ! checkKeyPtr )
				ret = true;
			else {
				key = lit->second.key();
				if ( strncmp( checkKeyPtr, key.data, key.size ) == 0 )
					ret = true;
			}
		}
	} else {
		while ( lit != rit ) {
			if ( ! checkKey || ! checkKeyPtr )
				break;
			key = lit->second.key();
			if ( strncmp( checkKeyPtr, key.data, key.size ) == 0 )
				break;

			lit++;
		}
		/* match request and/or key */
		ret = false;
		if ( lit != rit ) {
			if ( ! checkKey || ! checkKeyPtr )
				ret = true;
			else {
				key = lit->second.key();
				if ( strncmp( checkKeyPtr, key.data, key.size ) == 0 )
					ret = true;
			}
		}
	}

	if ( ret ) {
		if ( pidPtr ) *pidPtr = lit->first;
		if ( keyValuePtr ) *keyValuePtr = lit->second;
		map->erase( lit );
	}
	if ( needsUnlock ) UNLOCK( lock );

	return ret;
}

bool Pending::eraseKeyValueUpdate( PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr, PendingIdentifier *pidPtr, KeyValueUpdate *keyValueUpdatePtr,
	bool needsLock, bool needsUnlock, bool checkKey, char* checkKeyPtr
) {
	PendingIdentifier pid( instanceId, 0, requestId, 0, ptr );
	LOCK_T *lock;
	bool ret;

	std::unordered_multimap<PendingIdentifier, KeyValueUpdate> *map;
	std::unordered_multimap<PendingIdentifier, KeyValueUpdate>::iterator lit, rit;
	if ( ! this->get( type, lock, map ) )
		return false;

	if ( needsLock ) LOCK( lock );
	tie( lit, rit ) = map->equal_range( pid );
	SEARCH_KEY_RANGE( map, lit, rit, ptr, checkKey, checkKeyPtr, ret );

	if ( ret ) {
		if ( pidPtr ) *pidPtr = lit->first;
		if ( keyValueUpdatePtr ) *keyValueUpdatePtr = lit->second;
		map->erase( lit );
	}
	if ( needsUnlock ) UNLOCK( lock );

	return ret;
}

bool Pending::eraseAck( PendingType type, uint16_t instanceId, std::vector<PendingIdentifier> *pidPtr, bool needsLock, bool needsUnlock ) {

	PendingIdentifier pid( instanceId, 0, 0, 0, 0 );
	std::unordered_multimap<PendingIdentifier, AcknowledgementInfo> *map;
	std::unordered_multimap<PendingIdentifier, AcknowledgementInfo>::iterator it, saveIt;
	LOCK_T *lock;

	if ( ! this->get( type, lock, map ) )
		return false;
	if ( needsLock ) LOCK( lock );
	for ( it = map->begin(), saveIt = map->begin(); it != map->end(); it = saveIt ) {
		saveIt++;
		if ( it->first.instanceId == instanceId ) {
			if ( pidPtr ) 
				pidPtr->push_back( it->first );
			map->erase( it );
		}
	}
	if ( needsUnlock ) UNLOCK( lock );

	return true;
}

bool Pending::findKeyValue( PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr, KeyValue *keyValuePtr, bool checkKey, char* checkKeyPtr ) {
	PendingIdentifier pid( instanceId, 0, requestId, 0, ptr );
	LOCK_T *lock;
	bool ret;

	std::unordered_multimap<PendingIdentifier, KeyValue> *map;
	std::unordered_multimap<PendingIdentifier, KeyValue>::iterator lit, rit;
	if ( ! this->get( type, lock, map ) )
		return false;

	LOCK( lock );
	tie( lit, rit ) = map->equal_range( pid );
	Key key;
	/* prevent collision on id, use key and/or ptr as identifier as well */
	if ( ptr ) {
		while ( lit != rit ) {
			if ( lit->first.ptr == ptr ) {
				if ( ! checkKey || ! checkKeyPtr )
					break;
				key = lit->second.key();
				if ( strncmp( checkKeyPtr, key.data, key.size ) == 0 )
					break;
			}
			lit++;
		}
		/* match request id, ptr and/or key */
		ret = false;
		if ( lit != rit && lit->first.ptr == ptr ) {
			if ( ! checkKey || ! checkKeyPtr )
				ret = true;
			else {
				key = lit->second.key();
				if ( strncmp( checkKeyPtr, key.data, key.size ) == 0 )
					ret = true;
			}
		}
	} else {
		while ( lit != rit ) {
			if ( ! checkKey || ! checkKeyPtr )
				break;
			key = lit->second.key();
			if ( strncmp( checkKeyPtr, key.data, key.size ) == 0 )
				break;

			lit++;
		}
		/* match request and/or key */
		ret = false;
		if ( lit != rit ) {
			if ( ! checkKey || ! checkKeyPtr )
				ret = true;
			else {
				key = lit->second.key();
				if ( strncmp( checkKeyPtr, key.data, key.size ) == 0 )
					ret = true;
			}
		}
	}

	if ( ret ) {
		if ( keyValuePtr ) *keyValuePtr = lit->second;
	}
	UNLOCK( lock );

	return ret;
}

bool Pending::findKeyValueUpdate( PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr, KeyValueUpdate *keyValuePtr, bool checkKey, char *checkKeyPtr ) {
	PendingIdentifier pid( instanceId, 0, requestId, 0, ptr );
	LOCK_T *lock;
	bool ret;

	std::unordered_multimap<PendingIdentifier, KeyValueUpdate> *map;
	std::unordered_multimap<PendingIdentifier, KeyValueUpdate>::iterator lit, rit;
	if ( ! this->get( type, lock, map ) )
		return false;

	LOCK( lock );
	tie( lit, rit ) = map->equal_range( pid );
	SEARCH_KEY_RANGE( map, lit, rit, ptr, checkKey, checkKeyPtr, ret );

	if ( ret ) {
		if ( keyValuePtr ) *keyValuePtr = lit->second;
	}
	UNLOCK( lock );

	return ret;
}

bool Pending::findRemapList( PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr, RemapList *remapList ) {
	PendingIdentifier pid( instanceId, 0, requestId, 0, ptr );
	LOCK_T *lock;
	bool ret;

	std::unordered_multimap<PendingIdentifier, RemapList> *map;
	std::unordered_multimap<PendingIdentifier, RemapList>::iterator lit;
	if ( ! this->get( type, lock, map ) )
		return false;

	LOCK( lock );
	lit = map->find( pid );
	ret = ( lit != map->end() );

	if ( ret ) {
		if ( remapList ) *remapList = lit->second;
	}
	UNLOCK( lock );

	return ret;
}

#undef SEARCH_KEY_RANGE

uint32_t Pending::count( PendingType type, uint16_t instanceId, uint32_t requestId, bool needsLock, bool needsUnlock ) {
	PendingIdentifier pid( instanceId, 0, requestId, 0, 0 );
	LOCK_T *lock;
	uint32_t ret = 0;
	if ( type == PT_APPLICATION_UPDATE || type == PT_SLAVE_UPDATE ) {
		std::unordered_multimap<PendingIdentifier, KeyValueUpdate> *map;
		std::unordered_multimap<PendingIdentifier, KeyValueUpdate>::iterator it;

		if ( ! this->get( type, lock, map ) ) return 0;

		if ( needsLock ) LOCK( lock );
		ret = map->count( pid );
		// it = map->lower_bound( pid );
		// for ( ret = 0; it != map->end() && it->first.instanceId == instanceId && it->first.requestId == requestId; ret++, it++ );
		if ( needsUnlock ) UNLOCK( lock );
	} else {
		std::unordered_multimap<PendingIdentifier, Key> *map;
		std::unordered_multimap<PendingIdentifier, Key>::iterator it;

		if ( ! this->get( type, lock, map ) ) return 0;

		if ( needsLock ) LOCK( lock );
		ret = map->count( pid );
		// it = map->lower_bound( pid );
		// for ( ret = 0; it != map->end() && it->first.instanceId == instanceId && it->first.requestId == requestId; ret++, it++ );
		if ( needsUnlock ) UNLOCK( lock );
	}

	return ret;
}
