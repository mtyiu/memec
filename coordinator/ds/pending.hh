#ifndef __COORDINATOR_DS_PENDING_HH__
#define __COORDINATOR_DS_PENDING_HH__

#include <unordered_map>
#include "../../common/util/debug.hh"
#include "../../common/lock/lock.hh"

class Pending {
private:
	std::unordered_map<int, bool*> syncMetaRequests;
	LOCK_T syncMetaLock;

public:
	Pending() {
		LOCK_INIT( &this->syncMetaLock );
	}
	~Pending() {}
	
	void addSyncMetaReq( int id, bool* sync ) {
		LOCK( &this->syncMetaLock );
		syncMetaRequests[ id ] = sync;
		UNLOCK( &this->syncMetaLock );
	}

	bool* removeSyncMetaReq( int id ) {
		bool *sync = NULL;
		LOCK( &this->syncMetaLock );
		if ( syncMetaRequests.count( id ) > 0 ) {
			sync = this->syncMetaRequests[ id ];
			this->syncMetaRequests.erase( id );
		}
		UNLOCK( &this->syncMetaLock );
		return sync;
	}
};

#endif 
