#ifndef __COORDINATOR_REMAP_REMAP_MSG_HANDLER_HH__
#define __COORDINATOR_REMAP_REMAP_MSG_HANDLER_HH__

#include <pthread.h>
#include <string>
#include <set>
#include <map>
#include <vector>
#include "remap_worker.hh"
#include "../event/remap_state_event.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/event/event_queue.hh"
#include "../../common/event/event_type.hh"
#include "../../common/lock/lock.hh"
#include "../../common/remap/remap_msg_handler.hh"
#include "../../common/remap/remap_state.hh"
#include "../../common/remap/remap_group.hh"

class CoordinatorRemapMsgHandler : public RemapMsgHandler {
private:
	CoordinatorRemapMsgHandler();
	// Do not implement
	CoordinatorRemapMsgHandler( CoordinatorRemapMsgHandler const & );
	void operator=( CoordinatorRemapMsgHandler const & );

	~CoordinatorRemapMsgHandler();

	/* set of alive masters connected */
	std::set<std::string> aliveMasters;
	LOCK_T mastersLock;

	/* ack map for each slave, identify by slave addr, and contains the set of acked masters */
	std::map<struct sockaddr_in, std::set<std::string>* > ackMasters;
	LOCK_T mastersAckLock;

	bool isListening;

	CoordinatorRemapWorker *workers;
	std::set<struct sockaddr_in> aliveSlaves;
	std::set<struct sockaddr_in> crashedSlaves;
	LOCK_T aliveSlavesLock;

	/* handle master join or leave */
	bool isMasterLeft( int service, char *msg, char *subject );
	bool isMasterJoin( int service, char *msg, char *subject );

	static void *readMessages( void *argv );
	bool updateState( char *subject, char *msg, int len );

	/* manage the set of alive masters connected */
	void addAliveMaster( char *name );
	void removeAliveMaster( char *name );

	/* insert the same event for one or more slaves */
	bool insertRepeatedEvents ( RemapStateEvent event, std::vector<struct sockaddr_in> *slaves );

public:
	EventQueue<RemapStateEvent> *eventQueue;
	std::map<struct sockaddr_in, pthread_cond_t> ackSignal;
	pthread_mutex_t ackSignalLock; // dummy lock for pthread_cond_wait()

	static CoordinatorRemapMsgHandler *getInstance() {
		static CoordinatorRemapMsgHandler crmh;
		return &crmh;
	}

	bool init( const int ip, const int port, const char *user = NULL );
	void quit();

	bool start();
	bool stop();

	// batch transit (start)
	bool transitToDegraded( std::vector<struct sockaddr_in> *slaves );
	bool transitToNormal( std::vector<struct sockaddr_in> *slaves );
	// clean up before transition ends
	bool transitToDegradedEnd( const struct sockaddr_in &slave );
	bool transitToNormalEnd( const struct sockaddr_in &slave );

	// manage master ack
	bool resetMasterAck( struct sockaddr_in slave );
	bool isAllMasterAcked( struct sockaddr_in slave );

	// notify master of slaves' state
	bool sendStateToMasters( std::vector<struct sockaddr_in> slaves );
	bool sendStateToMasters( struct sockaddr_in slave );

	// keep track of the alive slaves
	bool addAliveSlave( struct sockaddr_in slave );
	bool addCrashedSlave( struct sockaddr_in slave );
	bool removeAliveSlave( struct sockaddr_in slave );

	// check slave state
	bool isInTransition( const struct sockaddr_in &slave );
	bool allowRemapping( const struct sockaddr_in &slave );
	bool reachMaximumRemapped( uint32_t maximum );

};

#endif
