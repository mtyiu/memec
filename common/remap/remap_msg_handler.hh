#ifndef __COMMON_REMAP_REMAP_MSG_HANDLER_HH__
#define __COMMON_REMAP_REMAP_MSG_HANDLER_HH__

#include <cstdio>
#include <map>
#include <vector>
#include <stdint.h>
#include <sys/types.h>
#include <pthread.h>
#include <sp.h>
#include "remap_status.hh"
#include "../lock/lock.hh"
#include "../ds/sockaddr_in.hh"

#define MAX_MESSLEN	 4096
#define MAX_SPREAD_NAME 1024
#define MAX_GROUP_NUM   10
#define GROUP_NAME	  "plio"
#define MSG_TYPE		FIFO_MESS

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

	const static uint32_t slaveStatusRecordSize = 4 + 2 + 1; // sizeof( IP, port, status ) = 7

	// send a vector of slave status
	bool sendStatus ( std::vector<struct sockaddr_in> slaves, const char *targetGroup );

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
	std::map<struct sockaddr_in, RemapStatus> slavesStatus;
	std::map<struct sockaddr_in, LOCK_T> slavesStatusLock;

	RemapMsgHandler();
	virtual ~RemapMsgHandler();

	inline bool getIsConnected () {
		return this->isConnected;
	}

	inline RemapStatus getStatus( struct sockaddr_in slave ) {
		RemapStatus status = REMAP_UNDEFINED;
		if ( this->slavesStatus.count( slave ) )
			status = this->slavesStatus[ slave ];
		return status;
	}

	bool init( const char *spread = NULL, const char *user = NULL );
	void quit();

	void listAliveSlaves();

	virtual bool start() = 0;
	virtual bool stop() = 0;

	virtual bool addAliveSlave( struct sockaddr_in slave ) = 0;
	virtual bool removeAliveSlave( struct sockaddr_in slave ) = 0;

	bool isRemapStarted( const struct sockaddr_in slave ) {
		if ( this->slavesStatus.count( slave ) == 0 ) 
			return false;
		switch ( this->slavesStatus[ slave ] ) {
			case REMAP_PREPARE_START:
			case REMAP_START:
			case REMAP_PREPARE_END:
				return true;
			default:
				return false;
		}
		return false;
	}
	bool isRemapStopped( const struct sockaddr_in slave ) {
		if ( this->slavesStatus.count( slave ) == 0 ) 
			return false;
		switch ( this->slavesStatus[ slave ] ) {
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
