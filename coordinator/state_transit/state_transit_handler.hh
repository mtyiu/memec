#ifndef __COORDINATOR_STATE_TRANSIT_STATE_TRANSIT_MSG_HANDLER_HH__
#define __COORDINATOR_STATE_TRANSIT_STATE_TRANSIT_MSG_HANDLER_HH__

#include <pthread.h>
#include <string>
#include <set>
#include <map>
#include <vector>
#include "state_transit_worker.hh"
#include "../event/state_transit_event.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/event/event_queue.hh"
#include "../../common/event/event_type.hh"
#include "../../common/lock/lock.hh"
#include "../../common/state_transit/state_transit_handler.hh"
#include "../../common/state_transit/state_transit_state.hh"
#include "../../common/state_transit/state_transit_group.hh"
#include "../../common/util/time.hh"

class CoordinatorStateTransitHandler : public StateTransitHandler {
private:
	CoordinatorStateTransitHandler();
	// Do not implement
	CoordinatorStateTransitHandler( CoordinatorStateTransitHandler const & );
	void operator=( CoordinatorStateTransitHandler const & );

	~CoordinatorStateTransitHandler();

	/* set of alive clients connected */
	std::set<std::string> aliveClients;
	LOCK_T clientsLock;

	/* ack map for each server, identify by server addr, and contains the set of acked clients */
	std::map<struct sockaddr_in, std::set<std::string>* > ackClients;
	LOCK_T clientsAckLock;

	bool isListening;

	CoordinatorStateTransitWorker *workers;
	std::set<struct sockaddr_in> aliveServers;
	std::set<struct sockaddr_in> crashedServers;
	std::set<struct sockaddr_in> failedServers;
	std::vector<struct sockaddr_in> updatedServers;
	LOCK_T aliveServersLock;
	LOCK_T failedServersLock;
	LOCK_T updatedServersLock;

	/* handle client join or leave */
	bool isClientJoin( int service, char *msg, char *subject );
	bool isClientLeft( int service, char *msg, char *subject );
	bool isServerJoin( int service, char *msg, char *subject );
	bool isServerLeft( int service, char *msg, char *subject );

	static void *readMessages( void *argv );
	bool updateState( char *subject, char *msg, int len );

	/* manage the set of alive clients connected */
	void addAliveClient( char *name );
	void removeAliveClient( char *name );

	/* insert the same event for one or more servers */
	bool insertRepeatedEvents ( StateTransitEvent event, std::vector<struct sockaddr_in> *servers );

public:
	EventQueue<StateTransitEvent> *eventQueue;
	std::map<struct sockaddr_in, pthread_cond_t> ackSignal;
	pthread_mutex_t ackSignalLock; // dummy lock for pthread_cond_wait()

	struct timespec transitStartTime;

	static CoordinatorStateTransitHandler *getInstance() {
		static CoordinatorStateTransitHandler csth;
		return &csth;
	}

	bool init( const int ip, const int port, const char *user = NULL );
	void quit();

	bool start();
	bool stop();

	// batch transit (start)
	bool transitToDegraded( std::vector<struct sockaddr_in> *servers, bool forced = false );
	bool transitToNormal( std::vector<struct sockaddr_in> *servers, bool froced = false );
	// clean up before transition ends
	bool transitToDegradedEnd( const struct sockaddr_in &server );
	bool transitToNormalEnd( const struct sockaddr_in &server );

	// manage client ack
	bool resetClientAck( struct sockaddr_in server );
	bool isAllClientAcked( struct sockaddr_in server );

	// notify clients of servers' state
	int sendStateToClients( std::vector<struct sockaddr_in> servers );
	int sendStateToClients( struct sockaddr_in server );
	// notify both clients and servers
	int broadcastState( std::vector<struct sockaddr_in> servers );
	int broadcastState( struct sockaddr_in server );

	// keep track of alive servers
	bool addAliveServer( struct sockaddr_in server );
	bool removeAliveServer( struct sockaddr_in server );

	// keep track of crashed servers
	bool addCrashedServer( struct sockaddr_in server );

	// keep track of failed servers
	uint32_t addFailedServer( struct sockaddr_in server );
	uint32_t removeFailedServer( struct sockaddr_in server );
	uint32_t getFailedServerCount( bool needsLock = false, bool needsUnlock = false );

	uint32_t eraseUpdatedServers( std::vector<struct sockaddr_in> * ret = 0 );

	// check server state
	bool isInTransition( const struct sockaddr_in &server );
	bool allowRemapping( const struct sockaddr_in &server );
	bool reachMaximumRemapped( uint32_t maximum );

};

#endif
