#ifndef __COORDINATOR_DS_PENDING_HH__
#define __COORDINATOR_DS_PENDING_HH__

#include <map>
#include <set>
#include <unordered_set>
#include <unordered_map>
#include "../socket/slave_socket.hh"
#include "../../common/lock/lock.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/ds/pending.hh"
#include "../../common/util/debug.hh"
#include "../../common/util/time.hh"

class PendingReconstruction {
public:
	uint32_t listId;
	uint32_t chunkId;
	uint32_t remaining;
	uint32_t total;
	std::unordered_set<uint32_t> stripeIds;
	pthread_mutex_t lock;
	pthread_cond_t cond;

	PendingReconstruction() {
		pthread_mutex_init( &this->lock, 0 );
		pthread_cond_init( &this->cond, 0 );
	}

	void set( uint32_t listId, uint32_t chunkId, std::unordered_set<uint32_t> &stripeIds ) {
		this->listId = listId;
		this->chunkId = chunkId;
		this->stripeIds = stripeIds;
		this->total = ( uint32_t ) this->stripeIds.size();
		this->remaining = this->total;
	}
};

class PendingRecovery {
public:
	uint32_t addr;
	uint16_t port;
	uint32_t remaining;
	uint32_t total;
	struct timespec startTime;
	SlaveSocket *socket;

	PendingRecovery( uint32_t addr, uint16_t port, uint32_t total, struct timespec startTime, SlaveSocket *socket ) {
		this->addr = addr;
		this->port = port;
		this->remaining = total;
		this->total = total;
		this->startTime = startTime;
		this->socket = socket;
	}
};

struct PendingRemapSync {
	uint32_t count;
	pthread_mutex_t *lock;
	pthread_cond_t *cond;
	bool *done;
};

struct PendingDegradedLock {
	uint32_t count;
	pthread_mutex_t *lock;
	pthread_cond_t *cond;
	bool *done;
};

struct PendingTransition {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	uint32_t pending;

	PendingTransition() {
		pthread_mutex_init( &this->lock, 0 );
		pthread_cond_init( &this->cond, 0 );
		this->pending = 0;
	}
};

class Pending {
private:
	/*
	 * syncMetaRequests: ( id, indicator whether sync is completed )
	 */
	std::unordered_map<uint32_t, bool *> syncMetaRequests;
	LOCK_T syncMetaLock;

	std::unordered_map<uint32_t, PendingDegradedLock> releaseDegradedLock;
	LOCK_T releaseDegradedLockLock;

	/*
	 * State transition: (normal -> intermediate) or (degraded -> coordinated normal)
	 * (Slave instance ID) |-> PendingTransition
	 */
	struct {
		LOCK_T intermediateLock;
		LOCK_T coordinatedLock;
		std::unordered_map<uint16_t, PendingTransition> intermediate;
		std::unordered_map<uint16_t, PendingTransition> coordinated;
	} transition;

	/*
	 * syncRemappingRecordCounters: ( packet id, counter for a sync operation )
	 * syncRemappingRecordCountersReverse: ( counter for a sync operation, set of packet ids associated )
	 * syncRemappingRecordIndicators: ( counter for a sync operations, indicator whether the op is completed )
	 * counter = map( master, no. of remaining packets to ack )
	 */
	std::map<uint32_t, std::map<struct sockaddr_in, uint32_t>* > syncRemappingRecordCounters;
	std::map<std::map<struct sockaddr_in, uint32_t>*, std::set<uint32_t> > syncRemappingRecordCountersReverse;
	std::map<std::map<struct sockaddr_in, uint32_t>*, bool*> syncRemappingRecordIndicators;
	LOCK_T syncRemappingRecordLock;

	std::unordered_map<PendingIdentifier, PendingReconstruction> reconstruction;
	LOCK_T reconstructionLock;

	std::map<struct sockaddr_in, Key> syncRemappedData;
	LOCK_T syncRemappedDataLock;

	std::unordered_map<uint32_t, PendingRemapSync> syncRemappedDataRequest;
	LOCK_T syncRemappedDataRequestLock;

	std::unordered_map<PendingIdentifier, PendingRecovery> recovery;
	LOCK_T recoveryLock;

public:
	Pending() {
		LOCK_INIT( &this->syncMetaLock );
		LOCK_INIT( &this->releaseDegradedLockLock );
		LOCK_INIT( &this->transition.intermediateLock );
		LOCK_INIT( &this->transition.coordinatedLock );
		LOCK_INIT( &this->syncRemappingRecordLock );
		LOCK_INIT( &this->reconstructionLock );
		LOCK_INIT( &this->recoveryLock );
		LOCK_INIT( &this->syncRemappedDataLock );
		LOCK_INIT( &this->syncRemappedDataRequestLock );
	}

	~Pending() {}

	bool insertReconstruction( uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, std::unordered_set<uint32_t> &stripeIds, pthread_mutex_t *&lock, pthread_cond_t *&cond ) {
		PendingIdentifier pid( instanceId, instanceId, requestId, requestId, 0 );
		PendingReconstruction r;

		r.set( listId, chunkId, stripeIds );

		std::pair<PendingIdentifier, PendingReconstruction> p( pid, r );
		std::pair<std::unordered_map<PendingIdentifier, PendingReconstruction>::iterator, bool> ret;

		LOCK( &this->reconstructionLock );
		ret = this->reconstruction.insert( p );
		lock = &( ret.first )->second.lock;
		cond = &( ret.first )->second.cond;
		UNLOCK( &this->reconstructionLock );

		return ret.second;
	}

	std::unordered_set<uint32_t> *findReconstruction( uint16_t instanceId, uint32_t requestId, uint32_t &listId, uint32_t &chunkId ) {
		PendingIdentifier pid( instanceId, instanceId, requestId, requestId, 0 );
		std::unordered_map<PendingIdentifier, PendingReconstruction>::iterator it;

		LOCK( &this->reconstructionLock );
		it = this->reconstruction.find( pid );
		if ( it == this->reconstruction.end() ) {
			UNLOCK( &this->reconstructionLock );
			return 0;
		}
		listId = it->second.listId;
		chunkId = it->second.chunkId;
		UNLOCK( &this->reconstructionLock );

		return &( it->second.stripeIds );
	}

	bool eraseReconstruction( uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, uint32_t numStripes, uint32_t &remaining ) {
		PendingIdentifier pid( instanceId, instanceId, requestId, requestId, 0 );
		std::unordered_map<PendingIdentifier, PendingReconstruction>::iterator it;
		bool ret;

		LOCK( &this->reconstructionLock );
		it = this->reconstruction.find( pid );
		if ( it == this->reconstruction.end() ) {
			UNLOCK( &this->reconstructionLock );
			return false;
		}
		ret = ( listId == it->second.listId && chunkId == it->second.chunkId && it->second.remaining >= numStripes );
		if ( ret ) {
			pthread_cond_signal( &it->second.cond );
			it->second.remaining -= numStripes;
			remaining = it->second.remaining;
			if ( it->second.remaining == 0 )
				this->reconstruction.erase( it );
		} else {
			printf(
				"(%u, %u, %u) vs. (%u, %u, %u)\n",
				listId, chunkId, numStripes,
				it->second.listId, it->second.chunkId, it->second.remaining
			);
		}
		UNLOCK( &this->reconstructionLock );

		return ret;
	}

	bool insertRecovery( uint16_t instanceId, uint32_t requestId, uint32_t addr, uint16_t port, uint32_t total, struct timespec startTime, SlaveSocket *socket ) {
		PendingIdentifier pid( instanceId, instanceId, requestId, requestId, 0 );
		PendingRecovery r( addr, port, total, startTime, socket );

		std::pair<PendingIdentifier, PendingRecovery> p( pid, r );
		std::pair<std::unordered_map<PendingIdentifier, PendingRecovery>::iterator, bool> ret;

		LOCK( &this->recoveryLock );
		ret = this->recovery.insert( p );
		UNLOCK( &this->recoveryLock );

		return ret.second;
	}

	bool eraseRecovery( uint16_t instanceId, uint32_t requestId, uint32_t addr, uint16_t port, uint32_t numReconstructed, SlaveSocket *socket, uint32_t &remaining, uint32_t &total, double &elapsedTime ) {
		PendingIdentifier pid( instanceId, instanceId, requestId, requestId, 0 );
		std::unordered_map<PendingIdentifier, PendingRecovery>::iterator it;
		bool ret;

		LOCK( &this->recoveryLock );
		it = this->recovery.find( pid );
		if ( it == this->recovery.end() ) {
			UNLOCK( &this->recoveryLock );
			return false;
		}
		ret = ( addr == it->second.addr && port == it->second.port && it->second.remaining >= numReconstructed && socket == it->second.socket );
		if ( ret ) {
			it->second.remaining -= numReconstructed;
			remaining = it->second.remaining;
			total = it->second.total;
			elapsedTime = get_elapsed_time( it->second.startTime );
			if ( it->second.remaining == 0 )
				this->recovery.erase( it );
		} else {
			printf(
				"(%u, %u, %u, %p) vs. (%u, %u, %u, %p)\n",
				addr, port, numReconstructed, socket,
				it->second.addr,
				it->second.port,
				it->second.remaining,
				it->second.socket
			);
		}
		UNLOCK( &this->recoveryLock );

		return ret;
	}

	void addReleaseDegradedLock( uint32_t id, uint32_t count, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done ) {
		std::unordered_map<uint32_t, PendingDegradedLock>::iterator it;

		LOCK( &this->releaseDegradedLockLock );
		it = this->releaseDegradedLock.find( id );
		if ( it == this->releaseDegradedLock.end() ) {
			PendingDegradedLock v;
			v.count = count;
			v.lock = lock;
			v.cond = cond;
			v.done = done;

			this->releaseDegradedLock[ id ] = v;
		} else {
			it->second.count += count;
		}
		UNLOCK( &this->releaseDegradedLockLock );
	}

	void removeReleaseDegradedLock( uint32_t id, uint32_t count, pthread_mutex_t *&lock, pthread_cond_t *&cond, bool *&done ) {
		std::unordered_map<uint32_t, PendingDegradedLock>::iterator it;

		lock = 0;
		cond = 0;
		done = 0;

		LOCK( &this->releaseDegradedLockLock );
		it = this->releaseDegradedLock.find( id );
		if ( it != this->releaseDegradedLock.end() ) {
			it->second.count -= count;
			if ( it->second.count == 0 ) {
				lock = it->second.lock;
				cond = it->second.cond;
				done = it->second.done;
				this->releaseDegradedLock.erase( it );
			}
		} else {
			__ERROR__( "Pending", "removeReleaseDegradedLock", "ID: %u Not found.\n", id );
		}
		UNLOCK( &this->releaseDegradedLockLock );
	}

	bool addPendingTransition( uint16_t instanceId, bool isDegraded, uint32_t pending ) {
		pthread_mutex_t *lock = isDegraded ? &this->transition.intermediateLock : &this->transition.coordinatedLock;
		std::unordered_map<uint16_t, PendingTransition>::iterator it;
		std::unordered_map<uint16_t, PendingTransition> &map = isDegraded ? this->transition.intermediate : this->transition.coordinated;
		bool ret = true;

		pthread_mutex_lock( lock );
		it = map.find( instanceId );
		if ( it == map.end() ) {
			map[ instanceId ] = PendingTransition();
			PendingTransition &transition = map[ instanceId ];
			transition.pending = pending;
		} else {
			__ERROR__( "Pending", "addPendingTransition", "This slave (instance ID = %u) is already under transition. This transition is ignored.", instanceId );
			ret = false;
		}
		pthread_mutex_unlock( lock );

		return ret;
	}

	PendingTransition *findPendingTransition( uint16_t instanceId, bool isDegraded ) {
		pthread_mutex_t *lock = isDegraded ? &this->transition.intermediateLock : &this->transition.coordinatedLock;
		std::unordered_map<uint16_t, PendingTransition>::iterator it;
		std::unordered_map<uint16_t, PendingTransition> &map = isDegraded ? this->transition.intermediate : this->transition.coordinated;
		PendingTransition *ret = 0;

		pthread_mutex_lock( lock );
		it = map.find( instanceId );
		if ( it != map.end() )
			ret = &( it->second );
		pthread_mutex_unlock( lock );

		return ret;
	}

	bool erasePendingTransition( uint16_t instanceId, bool isDegraded ) {
		pthread_mutex_t *lock = isDegraded ? &this->transition.intermediateLock : &this->transition.coordinatedLock;
		std::unordered_map<uint16_t, PendingTransition> &map = isDegraded ? this->transition.intermediate : this->transition.coordinated;
		bool ret;

		pthread_mutex_lock( lock );
		ret = map.erase( instanceId ) > 0;
		pthread_mutex_unlock( lock );

		return ret;
	}

	void addSyncMetaReq( uint32_t id, bool* sync ) {
		LOCK( &this->syncMetaLock );
		syncMetaRequests[ id ] = sync;
		UNLOCK( &this->syncMetaLock );
	}

	bool *removeSyncMetaReq( uint32_t id ) {
		bool *sync = NULL;
		LOCK( &this->syncMetaLock );
		if ( syncMetaRequests.count( id ) > 0 ) {
			sync = this->syncMetaRequests[ id ];
			this->syncMetaRequests.erase( id );
		}
		UNLOCK( &this->syncMetaLock );
		return sync;
	}

	// indicator is optional
	bool addRemappingRecords( uint32_t id, std::map<struct sockaddr_in, uint32_t> *map, bool* indicator = 0 ) {
		LOCK( &this->syncRemappingRecordLock );
		if ( this->syncRemappingRecordCounters.count( id ) > 0 ) {
			UNLOCK( &this->syncRemappingRecordLock );
			return false;
		}
		this->syncRemappingRecordCounters[ id ] = map;
		this->syncRemappingRecordIndicators[ map ] = indicator;
		if ( this->syncRemappingRecordCountersReverse.count( map ) < 1 )
			this->syncRemappingRecordCountersReverse[ map ] = std::set<uint32_t>();

		this->syncRemappingRecordCountersReverse[ map ].insert( id );
		UNLOCK( &this->syncRemappingRecordLock );
		return true;
	}

	// decrement the counter for a packet acked by a master
	bool decrementRemappingRecords( uint32_t id, struct sockaddr_in addr, bool lock = true, bool unlock = true ) {
		bool ret = false;
		if ( lock ) LOCK( &this->syncRemappingRecordLock );
		// check if the master needs to ack this packet
		if ( this->syncRemappingRecordCounters.count( id ) > 0 &&
			this->syncRemappingRecordCounters[ id ]->count( addr ) )
		{
			uint32_t &count = this->syncRemappingRecordCounters[ id ]->at( addr );
			count--;
			// if the master acked all packets, remove this master
			if ( count <= 0 ) {
				this->syncRemappingRecordCounters[ id ]->erase( addr );
			}
			ret = true;
		}
		if ( unlock ) UNLOCK( &this->syncRemappingRecordLock );
		return ret;
	}

	std::pair<std::map<struct sockaddr_in, uint32_t> *, bool*>
		checkAndRemoveRemappingRecords( uint32_t id, uint32_t target = 0, bool lock = true, bool unlock = true )
	{
		std::map<struct sockaddr_in, uint32_t> *map = NULL;
		bool *indicator = NULL;
		if ( lock ) LOCK( &this->syncRemappingRecordLock );
		// check if the packet exists, and the counter has "target" number of master remains
		if ( this->syncRemappingRecordCounters.count( id ) > 0 &&
			this->syncRemappingRecordCounters[ id ]->size() == target )
		{
			map = this->syncRemappingRecordCounters[ id ];
			indicator = this->syncRemappingRecordIndicators[ map ];
			std::set<uint32_t> idSet = this->syncRemappingRecordCountersReverse[ map ];
			// remove all the id assocaited with the counter
			for ( uint32_t id : idSet )
				this->syncRemappingRecordCounters.erase( id );
			// remove the counter
			this->syncRemappingRecordCountersReverse.erase( map );
			// flip the indicator if it exists
			if ( indicator )
				*indicator = ! *indicator;
		}
		if ( unlock ) UNLOCK( &this->syncRemappingRecordLock );
		return std::pair<std::map<struct sockaddr_in, uint32_t> *, bool *>( map, indicator );
	}

	std::map<struct sockaddr_in, uint32_t> *removeRemappingRecords( uint32_t id ) {
		std::map<struct sockaddr_in, uint32_t> *map = NULL;
		LOCK( &this->syncRemappingRecordLock );
		if ( this->syncRemappingRecordCounters.count( id ) > 0 ) {
			map = this->syncRemappingRecordCounters[ id ];
			this->syncRemappingRecordCounters.erase( id );
		}
		UNLOCK( &this->syncRemappingRecordLock );
		return map;
	}

	bool insertRemappedDataRequest( uint32_t id, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done, uint32_t count ) {
		PendingRemapSync pendingRemapSync;
		bool ret = false;

		pendingRemapSync.lock = lock;
		pendingRemapSync.cond = cond;
		pendingRemapSync.done = done;
		pendingRemapSync.count = count;

		LOCK( &this->syncRemappedDataRequestLock );
		if ( this->syncRemappedDataRequest.count( id ) == 0 ) {
			this->syncRemappedDataRequest[ id ] = pendingRemapSync;
			ret = true;
		}
		UNLOCK( &this->syncRemappedDataRequestLock );
		return ret;
	}

	bool decrementRemappedDataRequest( uint32_t id, struct sockaddr_in target, pthread_mutex_t *&lock, pthread_cond_t *&cond, bool *&done, bool &isCompleted ) {
		LOCK( &this->syncRemappedDataRequestLock );
		std::unordered_map<uint32_t, PendingRemapSync>::iterator it = this->syncRemappedDataRequest.find( id );
		bool ret = ( it != this->syncRemappedDataRequest.end() );

		isCompleted = false;

		if ( it != this->syncRemappedDataRequest.end() ) {
			PendingRemapSync &pendingRemapSync = it->second;
			pendingRemapSync.count--;
			if ( pendingRemapSync.count ) {
				lock = 0;
				cond = 0;
				done = 0;
			} else {
				lock = pendingRemapSync.lock;
				cond = pendingRemapSync.cond;
				done = pendingRemapSync.done;
				isCompleted = true;
				this->syncRemappedDataRequest.erase( it );
			}
		}
		UNLOCK( &this->syncRemappedDataRequestLock );
		return ret;
	}

	void printSyncMetaRequests( FILE *f = stderr, bool list = false ) {
		if ( list )
			for ( auto it : this->syncMetaRequests ) {
				fprintf( f, "id=%u", it.first );
			}
		fprintf( f, "total=%lu\n", this->syncMetaRequests.size() );
	}

	void printSyncRemappingRecords( FILE *f = stderr, bool list = false ) {
		if ( list )
			for ( auto it : this->syncRemappingRecordCounters ) {
				fprintf( f, "id=%u of size=%lu", it.first, it.second->size() );
			}
		fprintf( f, "total=%lu\n", this->syncRemappingRecordCounters.size() );
	}

	void print( FILE *f = stderr, bool list = false ) {
		printSyncMetaRequests( f, list );
		printSyncRemappingRecords( f, list );
	}
};

#endif
