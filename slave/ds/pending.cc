#include "pending.hh"

bool Pending::get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, RemappingRecordKey> *&map ) {
	switch( type ) {
		case PT_MASTER_REMAPPING_SET:
			lock = &this->masters.remappingSetLock;
			map = &this->masters.remappingSet;
			break;
		case PT_SLAVE_PEER_REMAPPING_SET:
			lock = &this->slavePeers.remappingSetLock;
			map = &this->slavePeers.remappingSet;
			break;
		default:
			lock = 0;
			map = 0;
			return false;
	}
	return true;
}

bool Pending::get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, Key> *&map ) {
	switch( type ) {
		case PT_MASTER_GET:
			lock = &this->masters.getLock;
			map = &this->masters.get;
			break;
		case PT_MASTER_DEL:
			lock = &this->masters.delLock;
			map = &this->masters.del;
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
		case PT_MASTER_UPDATE:
			lock = &this->masters.updateLock;
			map = &this->masters.update;
			break;
		default:
			lock = 0;
			map = 0;
			return false;
	}
	return true;
}

bool Pending::get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, DegradedOp> *&map ) {
	switch( type ) {
		case PT_SLAVE_PEER_DEGRADED_OPS:
			lock = &this->slavePeers.degradedOpsLock;
			map = &this->slavePeers.degradedOps;
			break;
		default:
			lock = 0;
			map = 0;
			return false;
	}
	return true;
}

bool Pending::get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, ChunkRequest> *&map ) {
	switch( type ) {
		case PT_SLAVE_PEER_GET_CHUNK:
			lock = &this->slavePeers.getChunkLock;
			map = &this->slavePeers.getChunk;
			break;
		case PT_SLAVE_PEER_SET_CHUNK:
			lock = &this->slavePeers.setChunkLock;
			map = &this->slavePeers.setChunk;
			break;
		default:
			lock = 0;
			map = 0;
			return false;
	}
	return true;
}

bool Pending::get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, ChunkUpdate> *&map ) {
	switch( type ) {
		case PT_SLAVE_PEER_UPDATE_CHUNK:
			lock = &this->slavePeers.updateChunkLock;
			map = &this->slavePeers.updateChunk;
			break;
		case PT_SLAVE_PEER_DEL_CHUNK:
			lock = &this->slavePeers.delChunkLock;
			map = &this->slavePeers.deleteChunk;
			break;
		default:
			lock = 0;
			map = 0;
			return false;
	}
	return true;
}

#define DEFINE_PENDING_MASTER_INSERT_METHOD( METHOD_NAME, VALUE_TYPE, VALUE_VAR ) \
	bool Pending::METHOD_NAME( PendingType type, uint32_t id, void *ptr, VALUE_TYPE &VALUE_VAR, bool needsLock, bool needsUnlock ) { \
		PendingIdentifier pid( id, id, ptr ); \
		std::pair<PendingIdentifier, VALUE_TYPE> p( pid, VALUE_VAR ); \
		std::pair<std::map<PendingIdentifier, VALUE_TYPE>::iterator, bool> ret; \
 		\
		pthread_mutex_t *lock; \
		std::map<PendingIdentifier, VALUE_TYPE> *map; \
		if ( ! this->get( type, lock, map ) ) \
			return false; \
 		\
		if ( needsLock ) pthread_mutex_lock( lock ); \
		ret = map->insert( p ); \
		if ( needsUnlock ) pthread_mutex_unlock( lock ); \
 		\
		return ret.second; \
	}

#define DEFINE_PENDING_SLAVE_PEER_INSERT_METHOD( METHOD_NAME, VALUE_TYPE, VALUE_VAR ) \
	bool Pending::METHOD_NAME( PendingType type, uint32_t id, uint32_t parentId, void *ptr, VALUE_TYPE &VALUE_VAR, bool needsLock, bool needsUnlock ) { \
		PendingIdentifier pid( id, parentId, ptr ); \
		std::pair<PendingIdentifier, VALUE_TYPE> p( pid, VALUE_VAR ); \
		std::pair<std::map<PendingIdentifier, VALUE_TYPE>::iterator, bool> ret; \
 		\
		pthread_mutex_t *lock; \
		std::map<PendingIdentifier, VALUE_TYPE> *map; \
		if ( ! this->get( type, lock, map ) ) \
			return false; \
 		\
		if ( needsLock ) pthread_mutex_lock( lock ); \
		ret = map->insert( p ); \
		if ( needsUnlock ) pthread_mutex_unlock( lock ); \
 		\
		return ret.second; \
	}

#define DEFINE_PENDING_ERASE_METHOD( METHOD_NAME, VALUE_TYPE, VALUE_PTR_VAR ) \
	bool Pending::METHOD_NAME( PendingType type, uint32_t id, void *ptr, PendingIdentifier *pidPtr, VALUE_TYPE *VALUE_PTR_VAR, bool needsLock, bool needsUnlock ) { \
		PendingIdentifier pid( id, 0, ptr ); \
		pthread_mutex_t *lock; \
		bool ret; \
		\
		std::map<PendingIdentifier, VALUE_TYPE> *map; \
		std::map<PendingIdentifier, VALUE_TYPE>::iterator it; \
		if ( ! this->get( type, lock, map ) ) \
			return false; \
		\
		if ( needsLock ) pthread_mutex_lock( lock ); \
		if ( ptr ) { \
			it = map->find( pid ); \
			ret = ( it != map->end() ); \
		} else { \
			it = map->lower_bound( pid ); \
			ret = ( it != map->end() && it->first.id == id ); \
		} \
		if ( ret ) { \
			if ( pidPtr ) *pidPtr = it->first; \
			if ( VALUE_PTR_VAR ) *VALUE_PTR_VAR = it->second; \
			map->erase( it ); \
		} \
		if ( needsUnlock ) pthread_mutex_unlock( lock ); \
		\
		return ret; \
	}

DEFINE_PENDING_MASTER_INSERT_METHOD( insertRemappingRecordKey, RemappingRecordKey, remappingRecordKey )
DEFINE_PENDING_MASTER_INSERT_METHOD( insertKey, Key, key )
DEFINE_PENDING_MASTER_INSERT_METHOD( insertKeyValueUpdate, KeyValueUpdate, keyValueUpdate )

DEFINE_PENDING_SLAVE_PEER_INSERT_METHOD( insertRemappingRecordKey, RemappingRecordKey, remappingRecordKey )
DEFINE_PENDING_SLAVE_PEER_INSERT_METHOD( insertDegradedOp, DegradedOp, degradedOp )
DEFINE_PENDING_SLAVE_PEER_INSERT_METHOD( insertChunkRequest, ChunkRequest, chunkRequest )
DEFINE_PENDING_SLAVE_PEER_INSERT_METHOD( insertChunkUpdate, ChunkUpdate, chunkUpdate )

DEFINE_PENDING_ERASE_METHOD( eraseKey, Key, keyPtr )
DEFINE_PENDING_ERASE_METHOD( eraseKeyValueUpdate, KeyValueUpdate, keyValueUpdatePtr )
DEFINE_PENDING_ERASE_METHOD( eraseDegradedOp, DegradedOp, degradedOpPtr )
DEFINE_PENDING_ERASE_METHOD( eraseChunkRequest, ChunkRequest, chunkRequestPtr )
DEFINE_PENDING_ERASE_METHOD( eraseChunkUpdate, ChunkUpdate, chunkUpdatePtr )

#undef DEFINE_PENDING_MASTER_INSERT_METHOD
#undef DEFINE_PENDING_SLAVE_PEER_INSERT_METHOD
#undef DEFINE_PENDING_ERASE_METHOD

bool Pending::findChunkRequest( PendingType type, uint32_t id, void *ptr, std::map<PendingIdentifier, ChunkRequest>::iterator &it, bool needsLock, bool needsUnlock ) {
	PendingIdentifier pid( id, 0, ptr );
	pthread_mutex_t *lock;
	bool ret;

	std::map<PendingIdentifier, ChunkRequest> *map;
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
	if ( needsUnlock ) pthread_mutex_unlock( lock );

	return ret;
}

uint32_t Pending::count( PendingType type, uint32_t id, bool needsLock, bool needsUnlock ) {
	PendingIdentifier pid( id, 0, 0 );
	pthread_mutex_t *lock;
	uint32_t ret = 0;
	if ( type == PT_SLAVE_PEER_DEGRADED_OPS ) {
		std::map<PendingIdentifier, DegradedOp> *map;
		std::map<PendingIdentifier, DegradedOp>::iterator it;

		if ( ! this->get( type, lock, map ) ) return 0;

		if ( needsLock ) pthread_mutex_lock( lock );
		it = map->lower_bound( pid );
		for ( ret = 0; it != map->end() && it->first.id == id; ret++, it++ );
		if ( needsUnlock ) pthread_mutex_unlock( lock );
	} else if ( type == PT_SLAVE_PEER_GET_CHUNK || type == PT_SLAVE_PEER_SET_CHUNK ) {
		std::map<PendingIdentifier, ChunkRequest> *map;
		std::map<PendingIdentifier, ChunkRequest>::iterator it;

		if ( ! this->get( type, lock, map ) ) return 0;

		if ( needsLock ) pthread_mutex_lock( lock );
		it = map->lower_bound( pid );
		for ( ret = 0; it != map->end() && it->first.id == id; ret++, it++ );
		if ( needsUnlock ) pthread_mutex_unlock( lock );
	} else if ( type == PT_SLAVE_PEER_UPDATE_CHUNK || type == PT_SLAVE_PEER_DEL_CHUNK ) {
		std::map<PendingIdentifier, ChunkUpdate> *map;
		std::map<PendingIdentifier, ChunkUpdate>::iterator it;

		if ( ! this->get( type, lock, map ) ) return 0;

		if ( needsLock ) pthread_mutex_lock( lock );
		it = map->lower_bound( pid );
		for ( ret = 0; it != map->end() && it->first.id == id; ret++, it++ );
		if ( needsUnlock ) pthread_mutex_unlock( lock );
	}

	return ret;
}
