#ifndef __COORDINATOR_DS_PENDING_HH__
#define __COORDINATOR_DS_PENDING_HH__

#include <map>
#include <set>
#include <unordered_set>
#include <unordered_map>
#include "../../common/lock/lock.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/ds/pending.hh"
#include "../../common/util/debug.hh"

class PendingRecovery {
public:
	uint32_t listId;
	uint32_t chunkId;
	std::unordered_set<uint32_t> stripeIds;

	void set( uint32_t listId, uint32_t chunkId, std::unordered_set<uint32_t> &stripeIds ) {
		this->listId = listId;
		this->chunkId = chunkId;
		this->stripeIds = stripeIds;
	}
};

class Pending {
private:
	/*
	 * syncMetaRequests: ( id, indicator whether sync is completed )
	 */
	std::unordered_map<uint32_t, bool*> syncMetaRequests;
	LOCK_T syncMetaLock;

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

	std::unordered_map<PendingIdentifier, PendingRecovery> recovery;
	LOCK_T recoveryLock;

public:
	Pending() {
		LOCK_INIT( &this->syncMetaLock );
		LOCK_INIT( &this->syncRemappingRecordLock );
		LOCK_INIT( &this->recoveryLock );
	}

	~Pending() {}

	bool insertRecovery( uint32_t id, uint32_t listId, uint32_t chunkId, std::unordered_set<uint32_t> &stripeIds ) {
		PendingIdentifier pid( id, id, 0 );
		PendingRecovery r;

		r.set( listId, chunkId, stripeIds );

		std::pair<PendingIdentifier, PendingRecovery> p( pid, r );
		std::pair<std::unordered_map<PendingIdentifier, PendingRecovery>::iterator, bool> ret;

		LOCK( &this->recoveryLock );
		ret = this->recovery.insert( p );
		UNLOCK( &this->recoveryLock );

		return ret.second;
	}

	std::unordered_set<uint32_t> *findRecovery( uint32_t id, uint32_t &listId, uint32_t &chunkId ) {
		PendingIdentifier pid( id, id, 0 );
		std::unordered_map<PendingIdentifier, PendingRecovery>::iterator it;

		LOCK( &this->recoveryLock );
		it = this->recovery.find( pid );
		if ( it == this->recovery.end() ) {
			UNLOCK( &this->recoveryLock );
			return 0;
		}
		listId = it->second.listId;
		chunkId = it->second.chunkId;
		UNLOCK( &this->recoveryLock );

		return &( it->second.stripeIds );
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
