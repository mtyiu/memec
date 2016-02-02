#include "pending.hh"
#include "../../common/util/debug.hh"

bool Pending::get( PendingType type, LOCK_T *&lock, std::unordered_set<PendingIdentifier> *&map ) {
	switch( type ) {
		case PT_SLAVE_PEER_FORWARD_KEYS:
			lock = &this->slavePeers.forwardKeysLock;
			map = &this->slavePeers.forwardKeys;
			break;
		default:
			lock = 0;
			map = 0;
			return false;
	}
	return true;
}

bool Pending::get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, Key> *&map ) {
	switch( type ) {
		case PT_MASTER_GET:
			lock = &this->masters.getLock;
			map = &this->masters.get;
			break;
		case PT_MASTER_DEL:
			lock = &this->masters.delLock;
			map = &this->masters.del;
			break;
		case PT_SLAVE_PEER_GET:
			lock = &this->slavePeers.getLock;
			map = &this->slavePeers.get;
			break;
		case PT_SLAVE_PEER_DEL:
			lock = &this->slavePeers.delLock;
			map = &this->slavePeers.del;
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
		case PT_SLAVE_PEER_SET:
			lock = &this->slavePeers.setLock;
			map = &this->slavePeers.set;
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
		case PT_MASTER_UPDATE:
			lock = &this->masters.updateLock;
			map = &this->masters.update;
			break;
		case PT_SLAVE_PEER_UPDATE:
			lock = &this->slavePeers.updateLock;
			map = &this->slavePeers.update;
			break;
		default:
			lock = 0;
			map = 0;
			return false;
	}
	return true;
}

bool Pending::get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, DegradedOp> *&map ) {
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

bool Pending::get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, ChunkRequest> *&map ) {
	switch( type ) {
		case PT_SLAVE_PEER_GET_CHUNK:
			lock = &this->slavePeers.getChunkLock;
			map = &this->slavePeers.getChunk;
			break;
		case PT_SLAVE_PEER_SET_CHUNK:
			lock = &this->slavePeers.setChunkLock;
			map = &this->slavePeers.setChunk;
			break;
		case PT_SLAVE_PEER_FORWARD_PARITY_CHUNK:
			lock = &this->slavePeers.forwardParityChunkLock;
			map = &this->slavePeers.forwardParityChunk;
			break;
		default:
			lock = 0;
			map = 0;
			return false;
	}
	return true;
}

bool Pending::get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, ChunkUpdate> *&map ) {
	switch( type ) {
		case PT_SLAVE_PEER_UPDATE_CHUNK:
			lock = &this->slavePeers.updateChunkLock;
			map = &this->slavePeers.updateChunk;
			break;
		case PT_SLAVE_PEER_DEL_CHUNK:
			lock = &this->slavePeers.deleteChunkLock;
			map = &this->slavePeers.deleteChunk;
			break;
		default:
			lock = 0;
			map = 0;
			return false;
	}
	return true;
}

bool Pending::get( PendingType type, LOCK_T *&lock, std::unordered_map<struct sockaddr_in, std::set<PendingData>* > *&map ) {
	switch( type ) {
		case PT_SLAVE_PEER_PARITY:
			lock = &this->slavePeers.remappedDataLock;
			map = &this->slavePeers.remappedData;
			break;
		default:
			lock = 0;
			map = 0;
			return false;
	}
	return true;
}
#define DEFINE_PENDING_MASTER_INSERT_METHOD( METHOD_NAME, VALUE_TYPE, VALUE_VAR ) \
	bool Pending::METHOD_NAME( PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr, VALUE_TYPE &VALUE_VAR, bool needsLock, bool needsUnlock ) { \
		PendingIdentifier pid( instanceId, instanceId, requestId, requestId, ptr ); \
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

#define DEFINE_PENDING_SLAVE_PEER_INSERT_METHOD( METHOD_NAME, VALUE_TYPE, VALUE_VAR ) \
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

DEFINE_PENDING_MASTER_INSERT_METHOD( insertKey, Key, key )
DEFINE_PENDING_MASTER_INSERT_METHOD( insertKeyValueUpdate, KeyValueUpdate, keyValueUpdate )

DEFINE_PENDING_SLAVE_PEER_INSERT_METHOD( insertKey, Key, key )
DEFINE_PENDING_SLAVE_PEER_INSERT_METHOD( insertKeyValue, KeyValue, keyValue )
DEFINE_PENDING_SLAVE_PEER_INSERT_METHOD( insertKeyValueUpdate, KeyValueUpdate, keyValueUpdate )
DEFINE_PENDING_SLAVE_PEER_INSERT_METHOD( insertDegradedOp, DegradedOp, degradedOp )
DEFINE_PENDING_SLAVE_PEER_INSERT_METHOD( insertChunkRequest, ChunkRequest, chunkRequest )
DEFINE_PENDING_SLAVE_PEER_INSERT_METHOD( insertChunkUpdate, ChunkUpdate, chunkUpdate )

DEFINE_PENDING_ERASE_METHOD( eraseKey, Key, keyPtr )
DEFINE_PENDING_ERASE_METHOD( eraseKeyValue, KeyValue, keyValuePtr )
DEFINE_PENDING_ERASE_METHOD( eraseKeyValueUpdate, KeyValueUpdate, keyValueUpdatePtr )
DEFINE_PENDING_ERASE_METHOD( eraseDegradedOp, DegradedOp, degradedOpPtr )
DEFINE_PENDING_ERASE_METHOD( eraseChunkRequest, ChunkRequest, chunkRequestPtr )
DEFINE_PENDING_ERASE_METHOD( eraseChunkUpdate, ChunkUpdate, chunkUpdatePtr )

#undef DEFINE_PENDING_MASTER_INSERT_METHOD
#undef DEFINE_PENDING_SLAVE_PEER_INSERT_METHOD
#undef DEFINE_PENDING_ERASE_METHOD

void Pending::insertReleaseDegradedLock( uint16_t instanceId, uint32_t requestId, CoordinatorSocket *socket, uint32_t count ) {
	PendingIdentifier pid( instanceId, instanceId, requestId, requestId, socket );
	std::unordered_map<PendingIdentifier, PendingDegradedLock>::iterator it;

	LOCK( &this->coordinators.releaseDegradedLockLock );
	it = this->coordinators.releaseDegradedLock.find( pid );
	if ( it == this->coordinators.releaseDegradedLock.end() ) {
		PendingDegradedLock d;
		d.count = count;
		d.total = count;

		this->coordinators.releaseDegradedLock[ pid ] = d;
	} else {
		it->second.count += count;
		it->second.total += count;
	}
	UNLOCK( &this->coordinators.releaseDegradedLockLock );
}

bool Pending::insertReconstruction( uint16_t instanceId, uint32_t requestId, CoordinatorSocket *socket, uint32_t listId, uint32_t chunkId, std::unordered_set<uint32_t> &stripeIds, std::unordered_set<Key> &unsealedKeys ) {
	PendingIdentifier pid( instanceId, instanceId, requestId, requestId, socket );
	PendingReconstruction r;
	r.set( listId, chunkId, stripeIds, unsealedKeys );
	std::unordered_map<PendingIdentifier, PendingReconstruction>::iterator it;
	bool ret;

	LOCK( &this->coordinators.reconstructionLock );
	it = this->coordinators.reconstruction.find( pid );
	if ( it == this->coordinators.reconstruction.end() ) {
		std::pair<PendingIdentifier, PendingReconstruction> p( pid, r );
		std::pair<std::unordered_map<PendingIdentifier, PendingReconstruction>::iterator, bool> r;
		r = this->coordinators.reconstruction.insert( p );
		ret = r.second;

		printf( "(%u, %u): Number of pending chunks = %lu; number of unsealed keys: %lu\n", listId, chunkId, stripeIds.size(), unsealedKeys.size() );
	} else {
		PendingReconstruction &reconstruction = it->second;
		if ( reconstruction.listId == listId && reconstruction.chunkId == chunkId ) {
			if ( stripeIds.size() ) reconstruction.merge( stripeIds );
			if ( unsealedKeys.size() ) reconstruction.merge( unsealedKeys );
			ret = true;
		} else {
			ret = false;
		}

		printf( "(%u, %u): Number of pending chunks = %lu; number of unsealed keys: %lu\n", listId, chunkId, reconstruction.chunks.stripeIds.size(), reconstruction.unsealed.keys.size() );
	}
	UNLOCK( &this->coordinators.reconstructionLock );

	return ret;
}

bool Pending::insertRecovery( uint16_t instanceId, uint32_t requestId, CoordinatorSocket *socket, uint32_t addr, uint16_t port, uint32_t chunkCount, uint32_t *metadataBuf, uint32_t unsealedCount, char *keysBuf ) {
	PendingIdentifier pid( instanceId, instanceId, requestId, requestId, socket );
	PendingRecovery r( addr, port );

	std::unordered_map<PendingIdentifier, PendingRecovery>::iterator it;
	bool ret;

	LOCK( &this->coordinators.recoveryLock );
	it = this->coordinators.recovery.find( pid );
	if ( it == this->coordinators.recovery.end() ) {
		r.insertMetadata( chunkCount, metadataBuf );
		r.insertKeys( unsealedCount, keysBuf );

		std::pair<PendingIdentifier, PendingRecovery> p( pid, r );
		std::pair<std::unordered_map<PendingIdentifier, PendingRecovery>::iterator, bool> r;
		r = this->coordinators.recovery.insert( p );
		ret = r.second;
	} else {
		it->second.insertMetadata( chunkCount, metadataBuf );
		it->second.insertKeys( unsealedCount, keysBuf );
		ret = true;
	}
	UNLOCK( &this->coordinators.recoveryLock );

	return ret;
}

bool Pending::insertRemapData( struct sockaddr_in target, uint32_t listId, uint32_t chunkId, Key key, Value value ) {
	LOCK( &this->slavePeers.remappedDataLock );
	if ( this->slavePeers.remappedData.count( target ) == 0 ) {
		this->slavePeers.remappedData[ target ] = new std::set<PendingData>();
	}
	PendingData pendingData;
	pendingData.set( listId, chunkId, key, value );
	// check for duplicated insert?
	this->slavePeers.remappedData[ target ]->insert( pendingData );
	UNLOCK( &this->slavePeers.remappedDataLock );
	return true;
}

bool Pending::insertRemapDataRequest( uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, uint32_t requestCount, SlavePeerSocket *target ) {
	bool ret = false;
	PendingIdentifier pid( instanceId, parentInstanceId, requestId, parentRequestId, target );
	LOCK( &this->slavePeers.remappedDataRequestLock );
	if ( this->slavePeers.remappedDataRequest.count( pid ) == 0 ){
		ret = true;
		this->slavePeers.remappedDataRequest[ pid ] = requestCount;
	}
	UNLOCK( &this->slavePeers.remappedDataRequestLock );
	return ret;
}

bool Pending::insert(
	PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
	bool needsLock, bool needsUnlock
) {
	PendingIdentifier pid( instanceId, parentInstanceId, requestId, parentRequestId, ptr );
	std::pair<std::unordered_set<PendingIdentifier>::iterator, bool> ret;
	LOCK_T *lock;
	std::unordered_set<PendingIdentifier> *set;

	if ( ! this->get( type, lock, set ) )
		return false;

	if ( needsLock ) LOCK( lock );
	ret = set->insert( pid );
	if ( needsUnlock ) UNLOCK( lock );

	return true;
}

bool Pending::eraseReleaseDegradedLock( uint16_t instanceId, uint32_t requestId, uint32_t count, uint32_t &remaining, uint32_t &total, PendingIdentifier *pidPtr ) {
	PendingIdentifier pid( instanceId, instanceId, requestId, requestId, 0 );
	std::unordered_map<PendingIdentifier, PendingDegradedLock>::iterator it;

	LOCK( &this->coordinators.releaseDegradedLockLock );
	it = this->coordinators.releaseDegradedLock.find( pid );
	if ( it == this->coordinators.releaseDegradedLock.end() ) {
		UNLOCK( &this->coordinators.releaseDegradedLockLock );
		return false;
	}
	if ( pidPtr ) *pidPtr = it->first;
	it->second.count -= count;
	remaining = it->second.count;
	total = it->second.total;
	UNLOCK( &this->coordinators.releaseDegradedLockLock );

	return true;
}

bool Pending::eraseRemapData( struct sockaddr_in target, std::set<PendingData> **pendingData ) {
	bool found = false;
	LOCK( &this->slavePeers.remappedDataLock );
	if ( this->slavePeers.remappedData.count( target ) > 0 ) {
		*pendingData = this->slavePeers.remappedData[ target ];
		this->slavePeers.remappedData.erase( target );
		found = true;
	} else {
		pendingData = 0;
	}
	UNLOCK( &this->slavePeers.remappedDataLock );
	return found;
}

bool Pending::decrementRemapDataRequest( uint16_t instanceId, uint32_t requestId, PendingIdentifier *pidPtr, uint32_t *requestCount ) {
	PendingIdentifier pid( instanceId, 0, requestId, 0, 0 );

	LOCK( &this->slavePeers.remappedDataRequestLock );
	auto it  = this->slavePeers.remappedDataRequest.find( pid );
	bool ret = ( it != this->slavePeers.remappedDataRequest.end() );
	// decrement remaining request count, remove counter if it reaches 0
	if ( ret ) {
		it->second--;
		if ( requestCount ) *requestCount = it->second;
		if ( it->second == 0 ) this->slavePeers.remappedDataRequest.erase( pid );
	}
	UNLOCK( &this->slavePeers.remappedDataRequestLock );

	if ( ret ) {
		if ( pidPtr ) *pidPtr = it->first;
	}

	return ret;
}

bool Pending::eraseReconstruction( uint16_t instanceId, uint32_t requestId, CoordinatorSocket *&socket, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t &remainingChunks, uint32_t &remainingKeys, uint32_t &totalChunks, uint32_t &totalKeys, PendingIdentifier *pidPtr ) {
	PendingIdentifier pid( instanceId, instanceId, requestId, requestId, 0 );
	std::unordered_map<PendingIdentifier, PendingReconstruction>::iterator it;
	bool ret;

	LOCK( &this->coordinators.reconstructionLock );
	it = this->coordinators.reconstruction.find( pid );
	if ( it == this->coordinators.reconstruction.end() ) {
		UNLOCK( &this->coordinators.reconstructionLock );
		return false;
	}
	// Check whether the list ID and chunk ID match
	if ( listId == it->second.listId && chunkId == it->second.chunkId ) {
		size_t count;
		if ( pidPtr ) *pidPtr = it->first;
		socket = ( CoordinatorSocket * ) it->first.ptr;
		count = it->second.chunks.stripeIds.erase( stripeId );
		ret = count > 0;
		remainingChunks = it->second.chunks.stripeIds.size();
		remainingKeys = it->second.unsealed.keys.size();
		totalChunks = it->second.chunks.total;
		totalKeys = it->second.unsealed.total;

		if ( remainingChunks == 0 && remainingKeys == 0 )
			this->coordinators.reconstruction.erase( it );
	} else {
		socket = 0;
		ret = false;
	}
	UNLOCK( &this->coordinators.reconstructionLock );

	return ret;
}

bool Pending::eraseReconstruction( uint16_t instanceId, uint32_t requestId, CoordinatorSocket *&socket, uint32_t listId, uint32_t chunkId, uint8_t keySize, char *keyStr, uint32_t &remainingChunks, uint32_t &remainingKeys, uint32_t &totalChunks, uint32_t &totalKeys, PendingIdentifier *pidPtr ) {
	PendingIdentifier pid( instanceId, instanceId, requestId, requestId, 0 );
	std::unordered_map<PendingIdentifier, PendingReconstruction>::iterator it;
	bool ret;

	LOCK( &this->coordinators.reconstructionLock );
	it = this->coordinators.reconstruction.find( pid );
	if ( it == this->coordinators.reconstruction.end() ) {
		UNLOCK( &this->coordinators.reconstructionLock );
		return false;
	}
	// Check whether the list ID and chunk ID match
	if ( listId == it->second.listId && chunkId == it->second.chunkId ) {
		size_t count;
		Key key;
		key.set( keySize, keyStr );

		if ( pidPtr ) *pidPtr = it->first;
		socket = ( CoordinatorSocket * ) it->first.ptr;
		count = it->second.unsealed.keys.erase( key );
		ret = count > 0;
		remainingChunks = it->second.chunks.stripeIds.size();
		remainingKeys = it->second.unsealed.keys.size();
		totalChunks = it->second.chunks.total;
		totalKeys = it->second.unsealed.total;

		if ( remainingChunks == 0 && remainingKeys == 0 )
			this->coordinators.reconstruction.erase( it );
	} else {
		socket = 0;
		ret = false;
	}
	UNLOCK( &this->coordinators.reconstructionLock );

	return ret;
}

bool Pending::eraseRecovery( uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint16_t &instanceId, uint32_t &requestId, CoordinatorSocket *&socket, uint32_t &addr, uint16_t &port, uint32_t &remainingChunks, uint32_t &remainingKeys, uint32_t &totalChunks, uint32_t &totalKeys ) {
	std::unordered_map<PendingIdentifier, PendingRecovery>::iterator it;
	Metadata metadata;
	bool ret = false;

	metadata.set( listId, stripeId, chunkId );

	LOCK( &this->coordinators.recoveryLock );
	for ( it = this->coordinators.recovery.begin(); it != this->coordinators.recovery.end(); it++ ) {
		const PendingIdentifier &pid = it->first;
		PendingRecovery &recovery = it->second;

		if ( recovery.chunks.metadata.erase( metadata ) > 0 ) {
			instanceId = pid.instanceId;
			requestId = pid.requestId;
			socket = ( CoordinatorSocket * ) pid.ptr;
			addr = recovery.addr;
			port = recovery.port;
			remainingChunks = recovery.chunks.metadata.size();
			totalChunks = recovery.chunks.total;
			remainingKeys = recovery.unsealed.keys.size();
			totalKeys = recovery.unsealed.total;

			if ( remainingChunks == 0 && remainingKeys == 0 )
				this->coordinators.recovery.erase( it );

			ret = true;
			break;
		}
	}
	UNLOCK( &this->coordinators.recoveryLock );

	return ret;
}

bool Pending::eraseRecovery( uint8_t keySize, char *keyStr, uint16_t &instanceId, uint32_t &requestId, CoordinatorSocket *&socket, uint32_t &addr, uint16_t &port, uint32_t &remainingChunks, uint32_t &remainingKeys, uint32_t &totalChunks, uint32_t &totalKeys ) {
	std::unordered_map<PendingIdentifier, PendingRecovery>::iterator it;
	Key key;
	bool ret = false;

	key.set( keySize, keyStr );

	LOCK( &this->coordinators.recoveryLock );
	for ( it = this->coordinators.recovery.begin(); it != this->coordinators.recovery.end(); it++ ) {
		const PendingIdentifier &pid = it->first;
		PendingRecovery &recovery = it->second;

		if ( recovery.unsealed.keys.erase( key ) > 0 ) {
			instanceId = pid.instanceId;
			requestId = pid.requestId;
			socket = ( CoordinatorSocket * ) pid.ptr;
			addr = recovery.addr;
			port = recovery.port;
			remainingChunks = recovery.chunks.metadata.size();
			totalChunks = recovery.chunks.total;
			remainingKeys = recovery.unsealed.keys.size();
			totalKeys = recovery.unsealed.total;

			if ( remainingChunks == 0 && remainingKeys == 0 )
				this->coordinators.recovery.erase( it );

			ret = true;
			break;
		}
	}
	UNLOCK( &this->coordinators.recoveryLock );

	return ret;
}

bool Pending::erase(
	PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr,
	PendingIdentifier *pidPtr,
	bool needsLock, bool needsUnlock
) {
	PendingIdentifier pid( instanceId, 0, requestId, 0, ptr );
	LOCK_T *lock;
	bool ret;

	std::unordered_set<PendingIdentifier> *set;
	std::unordered_set<PendingIdentifier>::iterator it;
	if ( ! this->get( type, lock, set ) )
		return false;

	if ( needsLock ) LOCK( lock );
	if ( ptr ) {
		it = set->find( pid );
		ret = ( it != set->end() );
	} else {
		it = set->find( pid );
		ret = ( it != set->end() && it->instanceId == instanceId && it->requestId == requestId );
	}
	if ( ret ) {
		if ( pidPtr ) *pidPtr = *it;
		set->erase( it );
	}
	if ( needsUnlock ) UNLOCK( lock );

	return ret;
}

bool Pending::findReconstruction( uint16_t instanceId, uint32_t requestId, uint32_t stripeId, uint32_t &listId, uint32_t &chunkId ) {
	PendingIdentifier pid( instanceId, instanceId, requestId, requestId, 0 );
	std::unordered_map<PendingIdentifier, PendingReconstruction>::iterator it;
	size_t count;

	LOCK( &this->coordinators.reconstructionLock );
	it = this->coordinators.reconstruction.find( pid );
	if ( it == this->coordinators.reconstruction.end() ) {
		UNLOCK( &this->coordinators.reconstructionLock );
		return 0;
	}
	listId = it->second.listId;
	chunkId = it->second.chunkId;
	count = it->second.chunks.stripeIds.count( stripeId );
	UNLOCK( &this->coordinators.reconstructionLock );

	return count > 0;
}

bool Pending::findReconstruction( uint16_t instanceId, uint32_t requestId, uint8_t keySize, char *keyStr, uint32_t &listId, uint32_t &chunkId ) {
	PendingIdentifier pid( instanceId, instanceId, requestId, requestId, 0 );
	std::unordered_map<PendingIdentifier, PendingReconstruction>::iterator it;
	Key key;
	size_t count;

	key.set( keySize, keyStr );

	LOCK( &this->coordinators.reconstructionLock );
	it = this->coordinators.reconstruction.find( pid );
	if ( it == this->coordinators.reconstruction.end() ) {
		UNLOCK( &this->coordinators.reconstructionLock );
		return 0;
	}
	listId = it->second.listId;
	chunkId = it->second.chunkId;
	count = it->second.unsealed.keys.count( key );
	UNLOCK( &this->coordinators.reconstructionLock );

	return count > 0;
}

bool Pending::findChunkRequest( PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr, std::unordered_multimap<PendingIdentifier, ChunkRequest>::iterator &it, bool needsLock, bool needsUnlock ) {
	PendingIdentifier pid( instanceId, 0, requestId, 0, ptr );
	LOCK_T *lock;
	bool ret;

	std::unordered_multimap<PendingIdentifier, ChunkRequest> *map;
	if ( ! this->get( type, lock, map ) )
		return false;

	if ( needsLock ) LOCK( lock );
	if ( ptr ) {
		it = map->find( pid );
		ret = ( it != map->end() );
	} else {
		it = map->find( pid );
		ret = ( it != map->end() && it->first.instanceId == instanceId && it->first.requestId == requestId );
	}
	if ( needsUnlock ) UNLOCK( lock );

	return ret;
}

uint32_t Pending::count( PendingType type, uint16_t instanceId, uint32_t requestId, bool needsLock, bool needsUnlock ) {
	PendingIdentifier pid( instanceId, 0, requestId, 0, 0 );
	LOCK_T *lock;
	uint32_t ret = 0;
	if ( type == PT_SLAVE_PEER_DEGRADED_OPS ) {
		std::unordered_multimap<PendingIdentifier, DegradedOp> *map;
		std::unordered_multimap<PendingIdentifier, DegradedOp>::iterator it;

		if ( ! this->get( type, lock, map ) ) return 0;

		if ( needsLock ) LOCK( lock );
		ret = map->count( pid );
		// it = map->count( pid );
		// for ( ret = 0; it != map->end() && it->first.instanceId == instanceId && it->first.requestId == requestId; ret++, it++ );
		if ( needsUnlock ) UNLOCK( lock );
	} else if ( type == PT_SLAVE_PEER_SET ) {
		std::unordered_multimap<PendingIdentifier, KeyValue> *map;
		std::unordered_multimap<PendingIdentifier, KeyValue>::iterator it;

		if ( ! this->get( type, lock, map ) ) return 0;

		if ( needsLock ) LOCK( lock );
		ret = map->count( pid );
		// it = map->count( pid );
		// for ( ret = 0; it != map->end() && it->first.instanceId == instanceId && it->first.requestId == requestId; ret++, it++ );
		if ( needsUnlock ) UNLOCK( lock );
	} else if ( type == PT_SLAVE_PEER_UPDATE ) {
		std::unordered_multimap<PendingIdentifier, KeyValueUpdate> *map;
		std::unordered_multimap<PendingIdentifier, KeyValueUpdate>::iterator it;

		if ( ! this->get( type, lock, map ) ) return 0;

		if ( needsLock ) LOCK( lock );
		ret = map->count( pid );
		// it = map->count( pid );
		// for ( ret = 0; it != map->end() && it->first.instanceId == instanceId && it->first.requestId == requestId; ret++, it++ );
		if ( needsUnlock ) UNLOCK( lock );
	} else if ( type == PT_SLAVE_PEER_DEL ) {
		std::unordered_multimap<PendingIdentifier, Key> *map;
		std::unordered_multimap<PendingIdentifier, Key>::iterator it;

		if ( ! this->get( type, lock, map ) ) return 0;

		if ( needsLock ) LOCK( lock );
		ret = map->count( pid );
		// it = map->count( pid );
		// for ( ret = 0; it != map->end() && it->first.instanceId == instanceId && it->first.requestId == requestId; ret++, it++ );
		if ( needsUnlock ) UNLOCK( lock );
	} else if ( type == PT_SLAVE_PEER_GET_CHUNK || type == PT_SLAVE_PEER_SET_CHUNK ) {
		std::unordered_multimap<PendingIdentifier, ChunkRequest> *map;
		std::unordered_multimap<PendingIdentifier, ChunkRequest>::iterator it;

		if ( ! this->get( type, lock, map ) ) return 0;

		if ( needsLock ) LOCK( lock );
		ret = map->count( pid );
		// it = map->count( pid );
		// for ( ret = 0; it != map->end() && it->first.instanceId == instanceId && it->first.requestId == requestId; ret++, it++ );
		if ( needsUnlock ) UNLOCK( lock );
	} else if ( type == PT_SLAVE_PEER_UPDATE_CHUNK || type == PT_SLAVE_PEER_DEL_CHUNK ) {
		std::unordered_multimap<PendingIdentifier, ChunkUpdate> *map;
		std::unordered_multimap<PendingIdentifier, ChunkUpdate>::iterator it;

		if ( ! this->get( type, lock, map ) ) return 0;

		if ( needsLock ) LOCK( lock );
		ret = map->count( pid );
		// it = map->lower_bound( pid );
		// for ( ret = 0; it != map->end() && it->first.instanceId == instanceId && it->first.requestId == requestId; ret++, it++ );
		if ( needsUnlock ) UNLOCK( lock );
	} else {
		__ERROR__( "Pending", "count", "The count function is not implemented for this type." );
		return 0;
	}

	return ret;
}
