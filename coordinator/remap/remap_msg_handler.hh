#ifndef __COORDINATOR_REMAP_REMAP_MSG_HANDLER_HH__
#define __COORDINATOR_REMAP_REMAP_MSG_HANDLER_HH__

#include <set>
#include <string>
#include <pthread.h>
#include "../../common/remap/remap_msg_handler.hh"
#include "../../common/remap/remap_status.hh"
#include "../../common/remap/remap_group.hh"

#define RETRY_LIMIT	 3

class CoordinatorRemapMsgHandler : public RemapMsgHandler {
private:
	std::set<std::string> aliveMasters;
	std::set<std::string> ackMasters;
	pthread_rwlock_t mastersLock;

	pthread_t reader;
	bool isListening;

	bool isMasterLeft( int service, char *msg, char *subject );
	bool isMasterJoin( int service, char *msg, char *subject );

	bool sendMessageToMasters( RemapStatus to = REMAP_UNDEFINED );
	static void *readMessages( void *argv );
	bool updateStatus( char *subject, char *msg, int len );

	void addAliveMaster( char *name );
	void removeAliveMaster( char *name );

	bool resetMasterAck();
	bool isAllMasterAcked();

public:
	CoordinatorRemapMsgHandler();
	~CoordinatorRemapMsgHandler();

	bool init( const int ip, const int port, const char *user = NULL );
	void quit();

	bool start();
	bool stop();

	void* read( void * );

	bool startRemap();
	bool stopRemap();
	bool isRemapStarted() {
		switch ( this->status ) {
			case REMAP_PREPARE_START:
			case REMAP_START:
			case REMAP_PREPARE_END:
				return true;
			default:
				return false;
		}
		return false;
	}
	bool isRemapStopped() {
		switch ( this->status ) {
			case REMAP_NONE:
			case REMAP_END:
			case REMAP_UNDEFINED:
				return true;
			default:
				return false;
		}
		return true;
	}
};

#endif
