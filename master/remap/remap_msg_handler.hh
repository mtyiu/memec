#ifndef __MASTER_REMAP_REMAP_MSG_HANDLER_HH__
#define __MASTER_REMAP_REMAP_MSG_HANDLER_HH__

#include "../../common/ds/sockaddr_in.hh"
#include "../../common/lock/lock.hh"
#include "../../common/remap/remap_msg_handler.hh"
#include "../../common/remap/remap_group.hh"

class MasterRemapMsgHandler : public RemapMsgHandler {
private:
	bool isListening;

	pthread_t acker;
	LOCK_T aliveSlavesLock;

	void setStatus( char* msg, int len );

	// threaded background process
	static void *readMessages( void *argv );
	static void *ackRemapLoop( void *argv );

	bool addAliveSlave( struct sockaddr_in slave );
	bool removeAliveSlave( struct sockaddr_in slave );

	/* return if master need to ack coordinator for slave */
	bool ackRemapForSlave( struct sockaddr_in slave ); 

	bool sendStatusToCoordinator( std::vector<struct sockaddr_in> slaves );
	bool sendStatusToCoordinator( struct sockaddr_in slave );
	
public:

	MasterRemapMsgHandler();
	~MasterRemapMsgHandler();

	bool init( const int ip, const int port, const char *user = NULL );
	void quit();

	bool start();
	bool stop();

	bool useRemappingFlow( struct sockaddr_in slave );
	bool allowRemapping( struct sockaddr_in slave );

	// ack specific slave if necessary, 
	// empty slave will trigger full search on possible slaves to ack
	bool ackRemap( struct sockaddr_in *slave = NULL );
	bool ackRemap( struct sockaddr_in slave );
};

#endif
