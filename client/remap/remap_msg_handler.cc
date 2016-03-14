#include <cstdlib>
#include <arpa/inet.h>
#include <pthread.h>
#include <string.h>
#include "../../common/remap/remap_group.hh"
#include "../../common/util/debug.hh"
#include "../main/client.hh"
#include "remap_msg_handler.hh"

ClientRemapMsgHandler::ClientRemapMsgHandler() :
		RemapMsgHandler() {
	this->group = ( char* )CLIENT_GROUP;
	LOCK_INIT( &this->aliveServersLock );
}

ClientRemapMsgHandler::~ClientRemapMsgHandler() {
}

bool ClientRemapMsgHandler::init( const int ip, const int port, const char *user ) {
	char addrbuf[ 32 ], ipstr[ INET_ADDRSTRLEN ];
	struct in_addr addr;
	memset( addrbuf, 0, 32 );
	addr.s_addr = ip;
	inet_ntop( AF_INET, &addr, ipstr, INET_ADDRSTRLEN );
	sprintf( addrbuf, "%u@%s", ntohs( port ), ipstr );
	return RemapMsgHandler::init( addrbuf , user ) ;
}

void ClientRemapMsgHandler::quit() {
	RemapMsgHandler::quit();
	if ( this->isListening ) {
		this->stop();
	}
	pthread_join( this->reader, NULL );
	this->isListening = true;
	this->reader = -1;
}

bool ClientRemapMsgHandler::start() {
	if ( ! this->isConnected )
		return false;

	this->isListening = true;
	// read message using a background thread
	if ( pthread_create( &this->reader, NULL, ClientRemapMsgHandler::readMessages, this ) < 0 ){
		__ERROR__( "ClientRemapMsgHandler", "start", "Client FAILED to start reading remapping messages\n" );
		return false;
	}
	this->bgAckInterval = Client::getInstance()->config.client.states.ackTimeout;
	if ( this->bgAckInterval > 0 && pthread_create( &this->acker, NULL, ClientRemapMsgHandler::ackTransitThread, this ) < 0 ){
		__ERROR__( "ClientRemapMsgHandler", "start", "Client FAILED to start background ack. service.\n" );
	}

	return true;
}

bool ClientRemapMsgHandler::stop() {
	int ret = 0;
	if ( ! this->isConnected || ! this->isListening )
		return false;

	// stop reading messages
	this->isListening = false;
	// avoid blocking call from blocking the stop action
	ret = pthread_cancel( this->reader );

	return ( ret == 0 );
}
void *ClientRemapMsgHandler::readMessages( void *argv ) {
	ClientRemapMsgHandler *myself = ( ClientRemapMsgHandler* ) argv;
	int ret = 0;

	int service, groups, endian;
	int16 msgType;
	char sender[ MAX_GROUP_NAME ], msg[ MAX_MESSLEN ];
	char targetGroups[ MAX_GROUP_NUM ][ MAX_GROUP_NAME ];

	// handler messages
	while ( myself->isListening ) {
		ret = SP_receive( myself->mbox, &service, sender, MAX_GROUP_NUM, &groups, targetGroups, &msgType, &endian, MAX_MESSLEN, msg );
		if ( ret > 0 && myself->isRegularMessage( service ) ) {
			// change state accordingly
			myself->setState( msg, ret );
			myself->increMsgCount();
		} else if ( ret < 0 ) {
			__ERROR__ ( "ClientRemapMsgHandler", "readMessages", "Failed to read message %d\n", ret );
		}
	}

	pthread_exit( ( void * ) &myself->msgCount );
	return ( void* ) &myself->msgCount;
}

void *ClientRemapMsgHandler::ackTransitThread( void *argv ) {

	ClientRemapMsgHandler *myself = ( ClientRemapMsgHandler* ) argv;

	while ( myself->bgAckInterval > 0 && myself->isListening ) {
		sleep( myself->bgAckInterval );
		myself->ackTransit();
	}

	pthread_exit(0);
	return NULL;
}

void ClientRemapMsgHandler::setState( char* msg , int len ) {
	RemapState signal;
	uint8_t serverCount = ( uint8_t ) msg[0];
	struct sockaddr_in server;
	int ofs = 1;
	uint32_t recordSize = this->serverStateRecordSize;

	for ( uint8_t i = 0; i < serverCount; i++ ) {
		server.sin_addr.s_addr = (*( ( uint32_t * )( msg + ofs ) ) );
		server.sin_port = *( ( uint16_t * )( msg + ofs + 4 ) );
		signal = ( RemapState ) msg[ ofs + 6 ];
		ofs += recordSize;

		char buf[ INET_ADDRSTRLEN ];
		inet_ntop( AF_INET, &server.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
		if ( this->serversState.count( server ) == 0 ) {
			__ERROR__( "ClientRemapMsgHandler", "setState" , "Server %s:%hu not found\n", buf, ntohs( server.sin_port ) );
			continue;
		}

		Client *client = Client::getInstance();
		LOCK_T &lock = client->sockets.servers.lock;
		std::vector<ServerSocket *> &servers = client->sockets.servers.values;
		ServerSocket *target = 0;

		LOCK( &lock );
		for ( size_t i = 0, count = servers.size(); i < count; i++ ) {
			if ( servers[ i ]->equal( server.sin_addr.s_addr, server.sin_port ) ) {
				target = servers[ i ];
				break;
			}
		}
		UNLOCK( &lock );

		if ( ! target ) {
			__ERROR__( "ClientRemapMsgHandler", "setState" , "ServerSocket for %s:%hu not found\n", buf, ntohs( server.sin_port ) );
			continue;
		}

		LOCK( &this->serversStateLock[ server ] );
		RemapState state = this->serversState[ server ];
		switch ( signal ) {
			case REMAP_NORMAL:
				__DEBUG__( BLUE, "ClientRemapMsgHandler", "setState", "REMAP_NORMAL %s:%hu", buf, ntohs( server.sin_port ) );
				break;
			case REMAP_INTERMEDIATE:
				__INFO__( BLUE, "ClientRemapMsgHandler", "setState", "REMAP_INTERMEDIATE %s:%hu", buf, ntohs( server.sin_port ) );
				if ( state == REMAP_WAIT_DEGRADED )
					signal = state;
				else {
					// Insert a new event
					ServerEvent event;
					event.syncMetadata( target );
					Client::getInstance()->eventQueue.insert( event );
					// revert parity deltas
					this->stateTransitInfo[ server ].unsetCompleted( true );
					Client::getInstance()->revertDelta(
						0, target, 0,
						&this->stateTransitInfo[ server ].counter.parityRevert.lock,
						&this->stateTransitInfo[ server ].counter.parityRevert.value,
						true
					);
					// scan for normal requests to be completed
					ClientWorker::gatherPendingNormalRequests( target );
				}
				break;
			case REMAP_COORDINATED:
				__INFO__( BLUE, "ClientRemapMsgHandler", "setState", "REMAP_COORDINATED %s:%hu", buf, ntohs( server.sin_port ) );
				if ( state == REMAP_WAIT_NORMAL )
					signal = state;
				break;
			case REMAP_DEGRADED:
				__INFO__( BLUE, "ClientRemapMsgHandler", "setState", "REMAP_DEGRADED %s:%hu", buf, ntohs( server.sin_port ) );
				if ( state == REMAP_INTERMEDIATE )
					__ERROR__( "ClientRemapMsgHandler", "setState", "Not yet ready for transition to DEGRADED!\n" );
				break;
			default:
				__INFO__( BLUE, "ClientRemapMsgHandler", "setState", "Unknown %d %s:%hu", signal, buf, ntohs( server.sin_port ) );
				UNLOCK( &this->serversStateLock[ server ] );
				return;
		}
		this->serversState[ server ] = signal;
		state = this->serversState[ server ];
		UNLOCK( &this->serversStateLock[ server ] );

		// actions/cleanup after state change
		switch ( state ) {
			case REMAP_INTERMEDIATE:
				// clean up pending items associated with this server
				// TODO handle the case when insert happened after cleanup ( useCoordinatedFlow returns false > erase > add )
				// ClientWorker::removePending( target );
				this->ackTransit();
				break;
			case REMAP_COORDINATED:
				// check if the change can be immediately acked
				this->ackTransit( &server );
				break;
			case REMAP_DEGRADED:
				// start replaying the requests
				// ClientWorker::replayRequestPrepare( target );
				// ClientWorker::replayRequest( target );
				break;
			default:
				break;
		}
	}

}

bool ClientRemapMsgHandler::addAliveServer( struct sockaddr_in server ) {
	LOCK( &this->aliveServersLock );
	if ( this->serversState.count( server ) >= 1 ) {
		UNLOCK( &this->aliveServersLock );
		return false;
	}
	this->serversState[ server ] = REMAP_NORMAL;
	this->stateTransitInfo[ server ] = StateTransitInfo();
	UNLOCK( &this->aliveServersLock );
	return true;
}

bool ClientRemapMsgHandler::removeAliveServer( struct sockaddr_in server ) {
	LOCK( &this->aliveServersLock );
	if ( this->serversState.count( server ) < 1 ) {
		UNLOCK( &this->aliveServersLock );
		return false;
	}
	this->serversState.erase( server );
	this->stateTransitInfo.erase( server );
	UNLOCK( &this->aliveServersLock );
	return true;
}

bool ClientRemapMsgHandler::useCoordinatedFlow( const struct sockaddr_in &server ) {
	if ( this->serversState.count( server ) == 0 )
		return false;
	return this->serversState[ server ] != REMAP_NORMAL;
	/*
	switch ( this->serversState[ server ] ) {
		case REMAP_INTERMEDIATE:
		case REMAP_DEGRADED:
		case REMAP_COORDINATED:
			return true;
		default:
			break;
	}
	return false;
	*/
}

bool ClientRemapMsgHandler::allowRemapping( const struct sockaddr_in &server ) {
	if ( this->serversState.count( server ) == 0 )
		return false;

	switch ( this->serversState[ server ] ) {
		case REMAP_INTERMEDIATE:
		case REMAP_WAIT_DEGRADED:
		case REMAP_DEGRADED:
			return true;
		default:
			break;
	}

	return false;
}

bool ClientRemapMsgHandler::acceptNormalResponse( const struct sockaddr_in &server ) {
	if ( this->serversState.count( server ) == 0 )
		return true;

	switch( this->serversState[ server ] ) {
		case REMAP_UNDEFINED:
		case REMAP_NORMAL:
		case REMAP_INTERMEDIATE:
		case REMAP_COORDINATED:
		case REMAP_WAIT_DEGRADED:
		case REMAP_WAIT_NORMAL:
			return true;
		case REMAP_DEGRADED:
		default:
			return false;
	}
}

int ClientRemapMsgHandler::sendStateToCoordinator( std::vector<struct sockaddr_in> servers ) {
	char group[ 1 ][ MAX_GROUP_NAME ];
	uint32_t recordSize = this->serverStateRecordSize;
	if ( servers.size() == 0 ) {
		// TODO send all server state
		//servers = std::vector<struct sockaddr_in>( this->aliveServers.begin(), this->aliveServers.end() );
		return false;
	} else if ( servers.size() > 255 || servers.size() * recordSize + 1 > MAX_MESSLEN ) {
		fprintf( stderr, "Too much servers to include in message" );
		return false;
	}
	strcpy( group[ 0 ], COORD_GROUP );
	return sendState( servers, 1, group );
}

int ClientRemapMsgHandler::sendStateToCoordinator( struct sockaddr_in server ) {
	std::vector<struct sockaddr_in> servers;
	servers.push_back( server );
	return sendStateToCoordinator( servers );
}

bool ClientRemapMsgHandler::ackTransit( struct sockaddr_in server ) {
	return ackTransit( &server );
}

// Call after all metadata is synchonized
bool ClientRemapMsgHandler::ackTransit( struct sockaddr_in *server ) {
	LOCK( &this->aliveServersLock );
	if ( server ) {
		// specific server
		if ( this->serversStateLock.count( *server ) == 0 ) {
			UNLOCK( &this->aliveServersLock );
			return false;
		}
		if ( this->checkAckForServer( *server ) )
			sendStateToCoordinator( *server );
	} else {
		// check all servers
		std::vector<struct sockaddr_in> serversToAck;
		for ( auto s : this->serversState ) {
			if ( this->checkAckForServer( s.first ) )
				serversToAck.push_back( s.first );
		}
		if ( ! serversToAck.empty() )
			sendStateToCoordinator( serversToAck );
	}
	UNLOCK( &this->aliveServersLock );

	return true;
}

bool ClientRemapMsgHandler::checkAckForServer( struct sockaddr_in server ) {
	LOCK( &this->serversStateLock[ server ] );
	RemapState state = this->serversState[ server ];
	char buf[ INET_ADDRSTRLEN ];
	inet_ntop( AF_INET, &server.sin_addr.s_addr, buf, INET_ADDRSTRLEN );

	if (
		( state == REMAP_NORMAL ) ||
	    ( state == REMAP_INTERMEDIATE &&
			(
				false /* yet sync all meta */ ||
				this->stateTransitInfo[ server ].getParityRevertCounterVal() > 0 /* yet undo all parity */ ||
				! this->stateTransitInfo[ server ].counter.pendingNormalRequests.completed /* yet complete all normal requests */
			)
		) ||
	    ( state == REMAP_COORDINATED && false ) ) {
		UNLOCK( &this->serversStateLock[ server ] );
		return false;
	}

	switch ( state ) {
		case REMAP_INTERMEDIATE:
			state = REMAP_WAIT_DEGRADED;
			break;
		case REMAP_COORDINATED:
			state = REMAP_WAIT_NORMAL;
			break;
		default:
			UNLOCK( &this->serversStateLock[ server ] );
			return false;
	}
	this->serversState[ server ] = state;
	UNLOCK( &this->serversStateLock[ server ] );

	return true;
}
