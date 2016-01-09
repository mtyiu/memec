#ifndef __MASTER_REMAP_REMAP_MSG_HANDLER_HH__
#define __MASTER_REMAP_REMAP_MSG_HANDLER_HH__

#include <unordered_map>
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
	} counter;

	StateTransitInfo() {
		LOCK_INIT( &counter.parityRevert.lock );
		counter.parityRevert.value = 0;
	}

	bool setParityRevertCounterVal( uint32_t value ) {
		LOCK( &counter.parityRevert.lock );
		counter.parityRevert.value = value;
		UNLOCK( &counter.parityRevert.lock );
		return true;
	}

	uint32_t getParityRevertCounterVal() {
		return counter.parityRevert.value;
	}

	uint32_t incrementParityRevertCounter( uint32_t inc = 1 ) {
		uint32_t ret = 0;
		LOCK( &counter.parityRevert.lock );
		counter.parityRevert.value += inc;
		ret = counter.parityRevert.value;
		UNLOCK( &counter.parityRevert.lock );
		return ret;
	}

	uint32_t decrementParityRevertCounter( uint32_t dec = 1 ) {
		uint32_t ret = 0;
		LOCK( &counter.parityRevert.lock );
		counter.parityRevert.value -= dec;
		ret = counter.parityRevert.value;
		UNLOCK( &counter.parityRevert.lock );
		return ret;
	}

};

class MasterRemapMsgHandler : public RemapMsgHandler {
private:
	bool isListening;

	/* background thread to check ack condition regularly */
	pthread_t acker;
	uint32_t bgAckInterval;

	/* lock on the list of alive slaves connected */
	LOCK_T aliveSlavesLock;

	/* parse a message and set state of slaves accordingly */
	void setState( char* msg, int len );

	// threaded background process
	static void *readMessages( void *argv );
	static void *ackTransitThread( void *argv );

	/* return if master need to ack coordinator for slave */
	bool checkAckForSlave( struct sockaddr_in slave );

	/* send a list of states of slaves */
	bool sendStateToCoordinator( std::vector<struct sockaddr_in> slaves );
	bool sendStateToCoordinator( struct sockaddr_in slave );

public:
	std::unordered_map<struct sockaddr_in, StateTransitInfo> stateTransitInfo;

	MasterRemapMsgHandler();
	~MasterRemapMsgHandler();

	bool init( const int ip, const int port, const char *user = NULL );
	void quit();

	bool start();
	bool stop();

	bool addAliveSlave( struct sockaddr_in slave );
	bool removeAliveSlave( struct sockaddr_in slave );

	bool useCoordinatedFlow( const struct sockaddr_in &slave );
	bool allowRemapping( const struct sockaddr_in &slave );
	bool acceptNormalResponse( const struct sockaddr_in &slave );

	// ack specific slave if necessary,
	// empty slave will trigger full search on possible slaves to ack
	bool ackTransit( struct sockaddr_in *slave = NULL );
	bool ackTransit( struct sockaddr_in slave );
};

#endif
