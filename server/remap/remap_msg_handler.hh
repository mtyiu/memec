#ifndef __SERVER_REMAP_REMAP_MSG_HANDLER_HH__
#define __SERVER_REMAP_REMAP_MSG_HANDLER_HH__

#include <unordered_map>
#include <unordered_set>
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/lock/lock.hh"
#include "../../common/remap/remap_msg_handler.hh"
#include "../../common/remap/remap_group.hh"

class SlaveRemapMsgHandler : public RemapMsgHandler {
private:
	bool isListening;

	/* lock on the list of alive servers connected */
	LOCK_T aliveSlavesLock;

	/* parse a message and set state of servers accordingly */
	void setState( char* msg, int len );

	// threaded background process
	static void *readMessages( void *argv );

public:
	SlaveRemapMsgHandler();
	~SlaveRemapMsgHandler();

	bool init( const int ip, const int port, const char *user = NULL );
	void quit();

	bool start();
	bool stop();

	bool addAliveSlave( struct sockaddr_in server );
	bool removeAliveSlave( struct sockaddr_in server );

	bool useCoordinatedFlow( const struct sockaddr_in &server );
	bool allowRemapping( const struct sockaddr_in &server );
	bool acceptNormalResponse( const struct sockaddr_in &server );
};

#endif
