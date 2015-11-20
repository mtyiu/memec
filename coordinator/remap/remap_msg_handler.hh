#ifndef __COORDINATOR_REMAP_REMAP_MSG_HANDLER_HH__
#define __COORDINATOR_REMAP_REMAP_MSG_HANDLER_HH__

#include <pthread.h>
#include <string>
#include <set>
#include <map>
#include <vector>
#include "remap_worker.hh"
#include "../event/remap_status_event.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/event/event_queue.hh"
#include "../../common/event/event_type.hh"
#include "../../common/lock/lock.hh"
#include "../../common/remap/remap_msg_handler.hh"
#include "../../common/remap/remap_status.hh"
#include "../../common/remap/remap_group.hh"

class CoordinatorRemapMsgHandler : public RemapMsgHandler {
private:
	CoordinatorRemapMsgHandler();
	// Do not implement
	CoordinatorRemapMsgHandler( CoordinatorRemapMsgHandler const & );
	void operator=( CoordinatorRemapMsgHandler const & );

	~CoordinatorRemapMsgHandler();

	std::set<std::string> aliveMasters;
	LOCK_T mastersLock;

	std::map<struct sockaddr_in, std::set<std::string>* > ackMasters; // slave, set of acked masters
	LOCK_T mastersAckLock;
	
	pthread_t reader;
	bool isListening;

	CoordinatorRemapWorker *workers;
	std::set<struct sockaddr_in> aliveSlaves;
	LOCK_T aliveSlavesLock;

	bool isMasterLeft( int service, char *msg, char *subject );
	bool isMasterJoin( int service, char *msg, char *subject );

	static void *readMessages( void *argv );
	bool updateStatus( char *subject, char *msg, int len );

	void addAliveMaster( char *name );
	void removeAliveMaster( char *name );

	bool insertRepeatedEvents ( RemapStatusEvent event, std::vector<struct sockaddr_in> *slaves );

public:
	EventQueue<RemapStatusEvent> *eventQueue;
	std::map<struct sockaddr_in, pthread_cond_t> ackSignal;
	LOCK_T ackSignalLock; // dummy lock for pthread_cond_wait()

	static CoordinatorRemapMsgHandler *getInstance() {
		static CoordinatorRemapMsgHandler crmh;
		return &crmh;
	}

	bool init( const int ip, const int port, const char *user = NULL );
	void quit();

	bool start();
	bool stop();

	void* read( void * );

	// batch start and stop (to trigger individual remap)
	bool startRemap( std::vector<struct sockaddr_in> *slaves );
	bool stopRemap( std::vector<struct sockaddr_in> *slaves );

	bool startRemapEnd( const struct sockaddr_in &slave );
	bool stopRemapEnd( const struct sockaddr_in &slave );

	bool resetMasterAck( struct sockaddr_in slave );
	bool isAllMasterAcked( struct sockaddr_in slave );

	bool sendStatusToMasters( std::vector<struct sockaddr_in> &slaves );
	bool sendStatusToMasters( struct sockaddr_in slave );

	// keep trace of the alive slaves 
	bool addAliveSlave( struct sockaddr_in slave );
	bool removeAliveSlave( struct sockaddr_in slave );


};

#endif
