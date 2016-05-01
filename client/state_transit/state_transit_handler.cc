#include <cstdlib>
#include <arpa/inet.h>
#include <pthread.h>
#include <string.h>
#include "../../common/state_transit/state_transit_group.hh"
#include "../../common/util/debug.hh"
#include "../main/client.hh"
#include "state_transit_handler.hh"

ClientStateTransitHandler::ClientStateTransitHandler() :
		StateTransitHandler() {
	this->group = ( char* )CLIENT_GROUP;
	LOCK_INIT( &this->aliveServersLock );
}

ClientStateTransitHandler::~ClientStateTransitHandler() {
}

bool ClientStateTransitHandler::init( const int ip, const int port, const char *user ) {
	char addrbuf[ 32 ], ipstr[ INET_ADDRSTRLEN ];
	struct in_addr addr;
	memset( addrbuf, 0, 32 );
	addr.s_addr = ip;
	inet_ntop( AF_INET, &addr, ipstr, INET_ADDRSTRLEN );
	sprintf( addrbuf, "%u@%s", ntohs( port ), ipstr );
	return StateTransitHandler::init( addrbuf , user ) ;
}

void ClientStateTransitHandler::quit() {
	StateTransitHandler::quit();
	if ( this->isListening ) {
		this->stop();
	}
	pthread_join( this->reader, NULL );
	this->isListening = true;
	this->reader = -1;
}

bool ClientStateTransitHandler::start() {
	if ( ! this->isConnected )
		return false;

	this->isListening = true;
	// read message using a background thread
	if ( pthread_create( &this->reader, NULL, ClientStateTransitHandler::readMessages, this ) < 0 ){
		__ERROR__( "ClientStateTransitHandler", "start", "Client FAILED to start reading state transition messages\n" );
		return false;
	}
	this->bgAckInterval = Client::getInstance()->config.client.states.ackTimeout;
	if ( this->bgAckInterval > 0 && pthread_create( &this->acker, NULL, ClientStateTransitHandler::ackTransitThread, this ) < 0 ){
		__ERROR__( "ClientStateTransitHandler", "start", "Client FAILED to start background ack. service.\n" );
	}

	return true;
}

bool ClientStateTransitHandler::stop() {
	int ret = 0;
	if ( ! this->isConnected || ! this->isListening )
		return false;

	// stop reading messages
	this->isListening = false;
	// avoid blocking call from blocking the stop action
	ret = pthread_cancel( this->reader );

	return ( ret == 0 );
}
void *ClientStateTransitHandler::readMessages( void *argv ) {
	ClientStateTransitHandler *myself = ( ClientStateTransitHandler* ) argv;
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
			__ERROR__ ( "ClientStateTransitHandler", "readMessages", "Failed to read message %d\n", ret );
		}
	}

	pthread_exit( ( void * ) &myself->msgCount );
	return ( void* ) &myself->msgCount;
}

void *ClientStateTransitHandler::ackTransitThread( void *argv ) {

	ClientStateTransitHandler *myself = ( ClientStateTransitHandler* ) argv;

	while ( myself->bgAckInterval > 0 && myself->isListening ) {
		sleep( myself->bgAckInterval );
		myself->ackTransit();
	}

	pthread_exit(0);
	return NULL;
}

void ClientStateTransitHandler::setState( char* msg , int len ) {
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
			__ERROR__( "ClientStateTransitHandler", "setState" , "Server %s:%hu not found\n", buf, ntohs( server.sin_port ) );
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
			__ERROR__( "ClientStateTransitHandler", "setState" , "ServerSocket for %s:%hu not found\n", buf, ntohs( server.sin_port ) );
			continue;
		}

		LOCK( &this->serversStateLock[ server ] );
		RemapState state = this->serversState[ server ];
		switch ( signal ) {
			case STATE_NORMAL:
				__DEBUG__( BLUE, "ClientStateTransitHandler", "setState", "STATE_NORMAL %s:%hu", buf, ntohs( server.sin_port ) );
				break;
			case STATE_INTERMEDIATE:
				__INFO__( BLUE, "ClientStateTransitHandler", "setState", "STATE_INTERMEDIATE %s:%hu", buf, ntohs( server.sin_port ) );
				if ( state == STATE_WAIT_DEGRADED )
					signal = state;
				else {
					// Insert a new event to sync metadata
					ServerEvent event;
					event.syncMetadata( target );
					Client::getInstance()->eventQueue.insert( event );
					// mark need to wait for pending requests first
					this->stateTransitInfo[ server ].unsetCompleted( true );
					// scan for normal requests to be completed
					ClientWorker::gatherPendingNormalRequests( target );
					// revert parity deltas
					Client::getInstance()->revertDelta(
						0, target, 0,
						&this->stateTransitInfo[ server ].counter.parityRevert.lock,
						&this->stateTransitInfo[ server ].counter.parityRevert.value,
						true
					);
				}
				break;
			case STATE_COORDINATED:
				__INFO__( BLUE, "ClientStateTransitHandler", "setState", "STATE_COORDINATED %s:%hu", buf, ntohs( server.sin_port ) );
				if ( state == STATE_WAIT_NORMAL )
					signal = state;
				break;
			case STATE_DEGRADED:
				__INFO__( BLUE, "ClientStateTransitHandler", "setState", "STATE_DEGRADED %s:%hu", buf, ntohs( server.sin_port ) );
				if ( state == STATE_INTERMEDIATE )
					__ERROR__( "ClientStateTransitHandler", "setState", "Not yet ready for transition to DEGRADED!" );
				break;
			default:
				__INFO__( BLUE, "ClientStateTransitHandler", "setState", "Unknown %d %s:%hu", signal, buf, ntohs( server.sin_port ) );
				UNLOCK( &this->serversStateLock[ server ] );
				return;
		}
		this->serversState[ server ] = signal;
		state = this->serversState[ server ];
		UNLOCK( &this->serversStateLock[ server ] );

		// actions/cleanup after state change
		switch ( state ) {
			case STATE_INTERMEDIATE:
				// clean up pending items associated with this server
				// TODO handle the case when insert happened after cleanup ( useCoordinatedFlow returns false > erase > add )
				ClientWorker::removePending( target );
				this->ackTransit();
				break;
			case STATE_COORDINATED:
				// check if the change can be immediately acked
				this->ackTransit( &server );
				break;
			case STATE_DEGRADED:
				// start replaying the requests
				ClientWorker::replayRequestPrepare( target );
				ClientWorker::replayRequest( target );
				break;
			default:
				break;
		}
	}

}

bool ClientStateTransitHandler::addAliveServer( struct sockaddr_in server ) {
	LOCK( &this->aliveServersLock );
	if ( this->serversState.count( server ) >= 1 ) {
		UNLOCK( &this->aliveServersLock );
		return false;
	}
	this->serversState[ server ] = STATE_NORMAL;
	this->stateTransitInfo[ server ] = StateTransitInfo();
	UNLOCK( &this->aliveServersLock );
	return true;
}

bool ClientStateTransitHandler::removeAliveServer( struct sockaddr_in server ) {
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

bool ClientStateTransitHandler::useCoordinatedFlow( const struct sockaddr_in &server, bool needsLock, bool needsUnlock ) {
	if ( this->serversState.count( server ) == 0 )
		return false;
	if ( needsLock ) LOCK( &this->serversStateLock[ server ] );
	bool ret = this->serversState[ server ] != STATE_NORMAL;
	if ( needsUnlock ) UNLOCK( &this->serversStateLock[ server ] );
	return ret;
}

bool ClientStateTransitHandler::allowRemapping( const struct sockaddr_in &server ) {
	if ( this->serversState.count( server ) == 0 )
		return false;

	switch ( this->serversState[ server ] ) {
		case STATE_INTERMEDIATE:
		case STATE_WAIT_DEGRADED:
		case STATE_DEGRADED:
			return true;
		default:
			break;
	}

	return false;
}

bool ClientStateTransitHandler::acceptNormalResponse( const struct sockaddr_in &server ) {
	if ( this->serversState.count( server ) == 0 )
		return true;

	switch( this->serversState[ server ] ) {
		case STATE_UNDEFINED:
		case STATE_NORMAL:
		case STATE_COORDINATED:
		case STATE_WAIT_NORMAL:
		case STATE_INTERMEDIATE:
			return true;
		case STATE_WAIT_DEGRADED:
		case STATE_DEGRADED:
		default:
			return false;
	}
}

int ClientStateTransitHandler::sendStateToCoordinator( std::vector<struct sockaddr_in> servers ) {
	char group[ 1 ][ MAX_GROUP_NAME ];
	uint32_t recordSize = this->serverStateRecordSize;
	if ( servers.size() == 0 ) {
		return false;
	} else if ( servers.size() > 255 || servers.size() * recordSize + 1 > MAX_MESSLEN ) {
		fprintf( stderr, "Too much servers to include in message" );
		return false;
	}
	strcpy( group[ 0 ], COORD_GROUP );
	return sendState( servers, 1, group );
}

int ClientStateTransitHandler::sendStateToCoordinator( struct sockaddr_in server ) {
	std::vector<struct sockaddr_in> servers;
	servers.push_back( server );
	return sendStateToCoordinator( servers );
}

bool ClientStateTransitHandler::ackTransit( struct sockaddr_in server ) {
	return ackTransit( &server );
}

// Call after all metadata is synchonized
bool ClientStateTransitHandler::ackTransit( struct sockaddr_in *server ) {
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

bool ClientStateTransitHandler::checkAckForServer( struct sockaddr_in server ) {
	LOCK( &this->serversStateLock[ server ] );
	RemapState state = this->serversState[ server ];
	char buf[ INET_ADDRSTRLEN ];
	inet_ntop( AF_INET, &server.sin_addr.s_addr, buf, INET_ADDRSTRLEN );

	if (
		( state == STATE_NORMAL ) ||
	    ( state == STATE_INTERMEDIATE &&
			(
				false /* yet sync all meta */ ||
				this->stateTransitInfo[ server ].getParityRevertCounterVal() > 0 /* yet undo all parity */ ||
				! this->stateTransitInfo[ server ].counter.pendingNormalRequests.completed /* yet complete all normal requests */
			)
		) ||
	    ( state == STATE_COORDINATED && false /* no operations are needed before completing transition */) ) {
		if ( state == STATE_INTERMEDIATE ) {
			// fprintf(
			// 	stderr,
			// 	"addr: %s:%hu, count: %u, completed? %s (pending = %lu)\n",
			// 	buf, ntohs( server.sin_port ),
			// 	this->stateTransitInfo[ server ].getParityRevertCounterVal(), this->stateTransitInfo[ server ].counter.pendingNormalRequests.completed ? "true" : "false",
			// 	this->stateTransitInfo[ server ].counter.pendingNormalRequests.requestIds.size()
			// );
			if ( this->stateTransitInfo[ server ].counter.pendingNormalRequests.requestIds.size() ) {
				std::unordered_set<uint32_t> &requestIds = this->stateTransitInfo[ server ].counter.pendingNormalRequests.requestIds;
				std::unordered_set<uint32_t>::iterator it;
				for ( it = requestIds.begin(); it != requestIds.end(); ) {
					if ( ! ClientWorker::pending->findKeyValue( *it ) ) {
						it = requestIds.erase( it );
					} else {
						it++;
					}
				}
			}
		}
		UNLOCK( &this->serversStateLock[ server ] );
		return false;
	}

	switch ( state ) {
		case STATE_INTERMEDIATE:
			state = STATE_WAIT_DEGRADED;
			break;
		case STATE_COORDINATED:
			state = STATE_WAIT_NORMAL;
			break;
		default:
			UNLOCK( &this->serversStateLock[ server ] );
			return false;
	}
	this->serversState[ server ] = state;
	UNLOCK( &this->serversStateLock[ server ] );
	__INFO__( GREEN, "ClientStateTransitHandler", "checkAckForServer", "Ack transition for server [%s:%hu].", buf, ntohs( server.sin_port ) );

	return true;
}
