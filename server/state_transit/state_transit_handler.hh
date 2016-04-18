#ifndef __SERVER_STATE_TRANSIT_STATE_TRANSIT_MSG_HANDLER_HH__
#define __SERVER_STATE_TRANSIT_STATE_TRANSIT_MSG_HANDLER_HH__

#include <unordered_map>
#include <unordered_set>
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/lock/lock.hh"
#include "../../common/state_transit/state_transit_handler.hh"
#include "../../common/state_transit/state_transit_group.hh"

class ServerStateTransitHandler : public StateTransitHandler {
private:
	bool isListening;

	/* lock on the list of alive servers connected */
	LOCK_T aliveServersLock;

	/* parse a message and set state of servers accordingly */
	void setState( char* msg, int len );

	// threaded background process
	static void *readMessages( void *argv );

public:
	ServerStateTransitHandler();
	~ServerStateTransitHandler();

	bool init( const int ip, const int port, const char *user = NULL );
	void quit();

	bool start();
	bool stop();

	bool addAliveServer( struct sockaddr_in server );
	bool removeAliveServer( struct sockaddr_in server );

	bool useCoordinatedFlow( const struct sockaddr_in &server, bool needsLock = false, bool needsUnlock = false );
	bool allowRemapping( const struct sockaddr_in &server );
	bool acceptNormalResponse( const struct sockaddr_in &server );
};

#endif
