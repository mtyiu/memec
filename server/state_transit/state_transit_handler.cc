#include <cstdlib>
#include <arpa/inet.h>
#include <pthread.h>
#include <string.h>
#include "../../common/state_transit/state_transit_group.hh"
#include "../../common/util/debug.hh"
#include "../main/server.hh"
#include "state_transit_handler.hh"

ServerStateTransitHandler::ServerStateTransitHandler() :
		StateTransitHandler() {
	this->group = ( char* )SERVER_GROUP;
	LOCK_INIT( &this->aliveServersLock );
}

ServerStateTransitHandler::~ServerStateTransitHandler() {
}

bool ServerStateTransitHandler::init( const int ip, const int port, const char *user ) {
	char addrbuf[ 32 ], ipstr[ INET_ADDRSTRLEN ];
	struct in_addr addr;
	memset( addrbuf, 0, 32 );
	addr.s_addr = ip;
	inet_ntop( AF_INET, &addr, ipstr, INET_ADDRSTRLEN );
	sprintf( addrbuf, "%u@%s", ntohs( port ), ipstr );
	return StateTransitHandler::init( addrbuf , user ) ;
}

void ServerStateTransitHandler::quit() {
	StateTransitHandler::quit();
	if ( this->isListening ) {
		this->stop();
	}
	pthread_join( this->reader, NULL );
	this->isListening = true;
	this->reader = -1;
}

bool ServerStateTransitHandler::start() {
	if ( ! this->isConnected )
		return false;

	this->isListening = true;
	// read message using a background thread
	if ( pthread_create( &this->reader, NULL, ServerStateTransitHandler::readMessages, this ) < 0 ){
		__ERROR__( "ServerStateTransitHandler", "start", "Server FAILED to start reading state transiton messages\n" );
		return false;
	}

	return true;
}

bool ServerStateTransitHandler::stop() {
	int ret = 0;
	if ( ! this->isConnected || ! this->isListening )
		return false;

	// stop reading messages
	this->isListening = false;

	return ( ret == 0 );
}
void *ServerStateTransitHandler::readMessages( void *argv ) {
	ServerStateTransitHandler *myself = ( ServerStateTransitHandler* ) argv;
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
			__ERROR__( "ServerStateTransitHandler", "readMessages" , "Failed to receive message %d\n", ret );
		}
	}

	pthread_exit( ( void * ) &myself->msgCount );
	return ( void* ) &myself->msgCount;
}

void ServerStateTransitHandler::setState( char* msg , int len ) {
	RemapState signal;
	uint8_t serverCount = ( uint8_t ) msg[0];
	struct sockaddr_in serverPeer;
	int ofs = 1;
	uint32_t recordSize = this->serverStateRecordSize;

	for ( uint8_t i = 0; i < serverCount; i++ ) {
		serverPeer.sin_addr.s_addr = (*( ( uint32_t * )( msg + ofs ) ) );
		serverPeer.sin_port = *( ( uint16_t * )( msg + ofs + 4 ) );
		signal = ( RemapState ) msg[ ofs + 6 ];
		ofs += recordSize;

		char buf[ INET_ADDRSTRLEN ];
		inet_ntop( AF_INET, &serverPeer.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
		if ( this->serversState.count( serverPeer ) == 0 ) {
			__ERROR__( "ServerStateTransitHandler", "setState" , "Server %s:%hu not found\n", buf, ntohs( serverPeer.sin_port ) );
			continue;
		}

		Server *server = Server::getInstance();
		LOCK_T &lock = server->sockets.serverPeers.lock;
		std::vector<ServerPeerSocket *> &servers = server->sockets.serverPeers.values;
		ServerPeerSocket *target = 0;

		LOCK( &lock );
		for ( size_t i = 0, count = servers.size(); i < count; i++ ) {
			if ( servers[ i ]->equal( serverPeer.sin_addr.s_addr, serverPeer.sin_port ) ) {
				target = servers[ i ];
				break;
			}
		}
		UNLOCK( &lock );

		if ( ! target ) {
			__ERROR__( "ServerStateTransitHandler", "setState" , "ServerSocket for %s:%hu not found\n", buf, ntohs( serverPeer.sin_port ) );
			continue;
		}

		LOCK( &this->serversStateLock[ serverPeer ] );
		switch ( signal ) {
			case STATE_NORMAL:
				__DEBUG__( BLUE, "ServerStateTransitHandler", "setState", "STATE_NORMAL %s:%hu", buf, ntohs( serverPeer.sin_port ) );
				break;
			case STATE_INTERMEDIATE:
				__INFO__( BLUE, "ServerStateTransitHandler", "setState", "STATE_INTERMEDIATE %s:%hu", buf, ntohs( serverPeer.sin_port ) );
				break;
			case STATE_COORDINATED:
				break;
			case STATE_DEGRADED:
				__INFO__( BLUE, "ServerStateTransitHandler", "setState", "STATE_DEGRADED %s:%hu", buf, ntohs( serverPeer.sin_port ) );
				break;
			default:
				__INFO__( BLUE, "ServerStateTransitHandler", "setState", "Unknown %d %s:%hu", signal, buf, ntohs( serverPeer.sin_port ) );
				UNLOCK( &this->serversStateLock[ serverPeer ] );
				return;
		}
		this->serversState[ serverPeer ] = signal;
		UNLOCK( &this->serversStateLock[ serverPeer ] );
	}

}

bool ServerStateTransitHandler::addAliveServer( struct sockaddr_in server ) {
	LOCK( &this->aliveServersLock );
	if ( this->serversState.count( server ) >= 1 ) {
		UNLOCK( &this->aliveServersLock );
		return false;
	}
	this->serversState[ server ] = STATE_NORMAL;
	UNLOCK( &this->aliveServersLock );
	return true;
}

bool ServerStateTransitHandler::removeAliveServer( struct sockaddr_in server ) {
	LOCK( &this->aliveServersLock );
	if ( this->serversState.count( server ) < 1 ) {
		UNLOCK( &this->aliveServersLock );
		return false;
	}
	this->serversState.erase( server );
	UNLOCK( &this->aliveServersLock );
	return true;
}

bool ServerStateTransitHandler::useCoordinatedFlow( const struct sockaddr_in &server, bool needsLock, bool needsUnlock ) {
	if ( this->serversState.count( server ) == 0 )
		return false;
	if ( needsLock ) LOCK( &this->serversStateLock[ server ] );
	bool ret = this->serversState[ server ] != STATE_NORMAL;
	if ( needsUnlock ) UNLOCK( &this->serversStateLock[ server ] );
	return ret;
}

bool ServerStateTransitHandler::allowRemapping( const struct sockaddr_in &server ) {
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

bool ServerStateTransitHandler::acceptNormalResponse( const struct sockaddr_in &server ) {
	if ( this->serversState.count( server ) == 0 )
		return true;

	switch( this->serversState[ server ] ) {
		case STATE_UNDEFINED:
		case STATE_NORMAL:
		case STATE_INTERMEDIATE:
		case STATE_COORDINATED:
		case STATE_WAIT_DEGRADED:
		case STATE_WAIT_NORMAL:
			return true;
		case STATE_DEGRADED:
		default:
			return false;
	}
}
