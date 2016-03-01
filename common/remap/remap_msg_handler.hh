#ifndef __COMMON_REMAP_REMAP_MSG_HANDLER_HH__
#define __COMMON_REMAP_REMAP_MSG_HANDLER_HH__

#include <cstdio>
#include <map>
#include <vector>
#include <stdint.h>
#include <sys/types.h>
#include <pthread.h>
#include <sp.h>
#include "remap_state.hh"
#include "../lock/lock.hh"
#include "../ds/sockaddr_in.hh"

#define MAX_MESSLEN         ( 4096 )         // max length of messages
#define MAX_SPREAD_NAME     ( 1024 )		 // max length of a group name
#define MAX_GROUP_NUM       ( 128 )          // max number of members in a group
#define GROUP_NAME          "memec"          // default group
#define MSG_TYPE            FIFO_MESS        // default message type

class RemapMsgHandler {
protected:
	mailbox mbox;
	char privateGroup[ MAX_GROUP_NAME ];
	char spread[ MAX_SPREAD_NAME ];
	char user[ MAX_SPREAD_NAME ];
	char *group ;

	pthread_t reader;
	uint32_t msgCount;

	bool isConnected;

	const static uint32_t serverStateRecordSize = 4 + 2 + 1; // sizeof( IP, port, state ) = 7

	// send a vector of server state
	int sendState ( std::vector<struct sockaddr_in> &servers, int numGroup, const char targetGroup[][ MAX_GROUP_NAME ] );

	inline void increMsgCount() {
		this->msgCount++;
	}

	inline void decreMsgCount() {
		this->msgCount--;
	}

	static inline bool isRegularMessage( int service ) {
		return ( service & REGULAR_MESS );
	}

	static inline bool isMemberJoin( int service ) {
		return ( service & CAUSED_BY_JOIN );
	}

	static inline bool isMemberLeave( int service ) {
		return ( ( service & CAUSED_BY_LEAVE ) ||
				( service & CAUSED_BY_DISCONNECT ) );
	}

public:
	std::map<struct sockaddr_in, RemapState> serversState;
	std::map<struct sockaddr_in, LOCK_T> serversStateLock;

	RemapMsgHandler();
	virtual ~RemapMsgHandler();

	inline bool getIsConnected () {
		return this->isConnected;
	}

	inline RemapState getState( struct sockaddr_in server ) {
		RemapState state = REMAP_UNDEFINED;
		if ( this->serversState.count( server ) )
			state = this->serversState[ server ];
		return state;
	}

	bool init( const char *spread = NULL, const char *user = NULL );
	void quit();

	void listAliveSlaves();

	virtual bool start() = 0;
	virtual bool stop() = 0;

	virtual bool addAliveSlave( struct sockaddr_in server ) = 0;
	virtual bool removeAliveSlave( struct sockaddr_in server ) = 0;

	bool isRemapStarted( const struct sockaddr_in server ) {
		if ( this->serversState.count( server ) == 0 )
			return false;
		switch ( this->serversState[ server ] ) {
			case REMAP_INTERMEDIATE:
			case REMAP_COORDINATED:
			case REMAP_DEGRADED:
				return true;
			default:
				return false;
		}
		return false;
	}

	bool isRemapStopped( const struct sockaddr_in server ) {
		if ( this->serversState.count( server ) == 0 )
			return false;
		switch ( this->serversState[ server ] ) {
			case REMAP_NORMAL:
			case REMAP_UNDEFINED:
				return true;
			default:
				return false;
		}
		return true;
	}

};

#endif
