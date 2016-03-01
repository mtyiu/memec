#ifndef __MASTER_REMAP_REMAP_MSG_HANDLER_HH__
#define __MASTER_REMAP_REMAP_MSG_HANDLER_HH__

#include <unordered_map>
#include <unordered_set>
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/lock/lock.hh"
#include "../../common/remap/remap_msg_handler.hh"
#include "../../common/remap/remap_group.hh"

class StateTransitInfo {
public:
	struct {
		struct {
			uint32_t value;
			LOCK_T lock;
		} parityRevert;
		struct {
			std::unordered_set<uint32_t> requestIds;
			LOCK_T lock;
			bool completed;
		} pendingNormalRequests;
	} counter;

	StateTransitInfo() {
		LOCK_INIT( &counter.parityRevert.lock );
		LOCK_INIT( &counter.pendingNormalRequests.lock );
		counter.parityRevert.value = 0;
		counter.pendingNormalRequests.completed = false;
	}

	bool setParityRevertCounterVal( uint32_t value ) {
		LOCK( &counter.parityRevert.lock );
		counter.parityRevert.value = value;
		UNLOCK( &counter.parityRevert.lock );
		return true;
	}

	uint32_t getParityRevertCounterVal( bool needsLock = true, bool needsUnlock = true ) {
		uint32_t ret = 0;
		if ( needsLock ) LOCK( &counter.parityRevert.lock );
		ret = counter.parityRevert.value;
		if ( needsUnlock ) UNLOCK( &counter.parityRevert.lock );
		return ret;
	}

	// increment or decrement number of parity revert requests
	// return the number of pending parity revert requests after addition or removal
	uint32_t incrementParityRevertCounter( uint32_t inc = 1 ) {
		LOCK( &counter.parityRevert.lock );
		counter.parityRevert.value += inc;
		return getParityRevertCounterVal( false, true );
	}

	uint32_t decrementParityRevertCounter( uint32_t dec = 1 ) {
		LOCK( &counter.parityRevert.lock );
		counter.parityRevert.value -= dec;
		return getParityRevertCounterVal( false, true );
	}

	uint32_t getPendingRequestCount( bool needsLock = true, bool needsUnlock = true ) {
		uint32_t ret = 0;
		if ( needsLock ) LOCK( &counter.pendingNormalRequests.lock );
		ret = counter.pendingNormalRequests.requestIds.size();
		if ( needsUnlock ) UNLOCK( &counter.pendingNormalRequests.lock );
		return ret;
	}

	// add or remove pending request by id (assigned by master)
	// return the number of pending requests after addition or removal
	uint32_t addPendingRequest( uint32_t requestId, bool needsLock = true, bool needsUnlock = true ) {
		if ( needsLock ) LOCK( &counter.pendingNormalRequests.lock );
		counter.pendingNormalRequests.requestIds.insert( requestId );
		return getPendingRequestCount( false, needsUnlock );

	}

	uint32_t removePendingRequest( uint32_t requestId, bool needsLock = true, bool needsUnlock = true ) {
		if ( needsLock ) LOCK( &counter.pendingNormalRequests.lock );
		counter.pendingNormalRequests.requestIds.erase( requestId );
		return getPendingRequestCount( false, needsUnlock );
	}

	bool isCompleted() {
		return counter.pendingNormalRequests.completed;
	}

	// set the progress to be completed
	bool setCompleted( bool force = false, bool needsLock = true, bool needsUnlock = true ) {
		bool &completed = counter.pendingNormalRequests.completed;
		if ( needsLock ) LOCK( &counter.pendingNormalRequests.lock );
		completed = force || counter.pendingNormalRequests.requestIds.empty();
		if ( needsUnlock ) UNLOCK( &counter.pendingNormalRequests.lock );
		return completed;
	}

	// set the progress to be yet completed
	bool unsetCompleted( bool force = false, bool needsLock = true, bool needsUnlock = true ) {
		bool &completed = counter.pendingNormalRequests.completed;
		if ( needsLock ) LOCK( &counter.pendingNormalRequests.lock );
		/* not empty = yet completed */
		completed = ! force && ! counter.pendingNormalRequests.requestIds.empty();
		if ( needsUnlock ) UNLOCK( &counter.pendingNormalRequests.lock );
		return completed;
	}
};

class MasterRemapMsgHandler : public RemapMsgHandler {
private:
	bool isListening;

	/* background thread to check ack condition regularly */
	pthread_t acker;
	uint32_t bgAckInterval;

	/* lock on the list of alive servers connected */
	LOCK_T aliveSlavesLock;

	/* parse a message and set state of servers accordingly */
	void setState( char* msg, int len );

	// threaded background process
	static void *readMessages( void *argv );
	static void *ackTransitThread( void *argv );

	/* return if master need to ack coordinator for server */
	bool checkAckForSlave( struct sockaddr_in server );

	/* send a list of states of servers */
	int sendStateToCoordinator( std::vector<struct sockaddr_in> servers );
	int sendStateToCoordinator( struct sockaddr_in server );

public:
	std::unordered_map<struct sockaddr_in, StateTransitInfo> stateTransitInfo;

	MasterRemapMsgHandler();
	~MasterRemapMsgHandler();

	bool init( const int ip, const int port, const char *user = NULL );
	void quit();

	bool start();
	bool stop();

	bool addAliveSlave( struct sockaddr_in server );
	bool removeAliveSlave( struct sockaddr_in server );

	bool useCoordinatedFlow( const struct sockaddr_in &server );
	bool allowRemapping( const struct sockaddr_in &server );
	bool acceptNormalResponse( const struct sockaddr_in &server );

	// ack specific server if necessary,
	// empty server will trigger full search on possible servers to ack
	bool ackTransit( struct sockaddr_in *server = NULL );
	bool ackTransit( struct sockaddr_in server );
};

#endif
