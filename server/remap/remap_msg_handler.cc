#include <cstdlib>
#include <arpa/inet.h>
#include <pthread.h>
#include <string.h>
#include "../../common/remap/remap_group.hh"
#include "../../common/util/debug.hh"
#include "../main/server.hh"
#include "remap_msg_handler.hh"

ServerRemapMsgHandler::ServerRemapMsgHandler() :
		RemapMsgHandler() {
	this->group = ( char* )SERVER_GROUP;
	LOCK_INIT( &this->aliveServersLock );
}

ServerRemapMsgHandler::~ServerRemapMsgHandler() {
}

bool ServerRemapMsgHandler::init( const int ip, const int port, const char *user ) {
	char addrbuf[ 32 ], ipstr[ INET_ADDRSTRLEN ];
	struct in_addr addr;
	memset( addrbuf, 0, 32 );
	addr.s_addr = ip;
	inet_ntop( AF_INET, &addr, ipstr, INET_ADDRSTRLEN );
	sprintf( addrbuf, "%u@%s", ntohs( port ), ipstr );
	return RemapMsgHandler::init( addrbuf , user ) ;
}

void ServerRemapMsgHandler::quit() {
	RemapMsgHandler::quit();
	if ( this->isListening ) {
		this->stop();
	}
	pthread_join( this->reader, NULL );
	this->isListening = true;
	this->reader = -1;
}

bool ServerRemapMsgHandler::start() {
	if ( ! this->isConnected )
		return false;

	this->isListening = true;
	// read message using a background thread
	if ( pthread_create( &this->reader, NULL, ServerRemapMsgHandler::readMessages, this ) < 0 ){
		__ERROR__( "ServerRemapMsgHandler", "start", "Server FAILED to start reading remapping messages\n" );
		return false;
	}

	return true;
}

bool ServerRemapMsgHandler::stop() {
	int ret = 0;
	if ( ! this->isConnected || ! this->isListening )
		return false;

	// stop reading messages
	this->isListening = false;

	return ( ret == 0 );
}
void *ServerRemapMsgHandler::readMessages( void *argv ) {
	ServerRemapMsgHandler *myself = ( ServerRemapMsgHandler* ) argv;
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
			__ERROR__( "ServerRemapMsgHandler", "readMessages" , "Failed to receive message %d\n", ret );
		}
	}

	pthread_exit( ( void * ) &myself->msgCount );
	return ( void* ) &myself->msgCount;
}

void ServerRemapMsgHandler::setState( char* msg , int len ) {
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
			__ERROR__( "ServerRemapMsgHandler", "setState" , "Server %s:%hu not found\n", buf, ntohs( serverPeer.sin_port ) );
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
			__ERROR__( "ServerRemapMsgHandler", "setState" , "ServerSocket for %s:%hu not found\n", buf, ntohs( serverPeer.sin_port ) );
			continue;
		}

		LOCK( &this->serversStateLock[ serverPeer ] );
		switch ( signal ) {
			case REMAP_NORMAL:
				__DEBUG__( BLUE, "ServerRemapMsgHandler", "setState", "REMAP_NORMAL %s:%hu", buf, ntohs( serverPeer.sin_port ) );
				break;
			case REMAP_INTERMEDIATE:
				__INFO__( BLUE, "ServerRemapMsgHandler", "setState", "REMAP_INTERMEDIATE %s:%hu", buf, ntohs( serverPeer.sin_port ) );
				break;
			case REMAP_COORDINATED:
				break;
			case REMAP_DEGRADED:
				__INFO__( BLUE, "ServerRemapMsgHandler", "setState", "REMAP_DEGRADED %s:%hu", buf, ntohs( serverPeer.sin_port ) );
				break;
			default:
				__INFO__( BLUE, "ServerRemapMsgHandler", "setState", "Unknown %d %s:%hu", signal, buf, ntohs( serverPeer.sin_port ) );
				UNLOCK( &this->serversStateLock[ serverPeer ] );
				return;
		}
		this->serversState[ serverPeer ] = signal;
		UNLOCK( &this->serversStateLock[ serverPeer ] );
	}

}

bool ServerRemapMsgHandler::addAliveServer( struct sockaddr_in server ) {
	LOCK( &this->aliveServersLock );
	if ( this->serversState.count( server ) >= 1 ) {
		UNLOCK( &this->aliveServersLock );
		return false;
	}
	this->serversState[ server ] = REMAP_NORMAL;
	UNLOCK( &this->aliveServersLock );
	return true;
}

bool ServerRemapMsgHandler::removeAliveServer( struct sockaddr_in server ) {
	LOCK( &this->aliveServersLock );
	if ( this->serversState.count( server ) < 1 ) {
		UNLOCK( &this->aliveServersLock );
		return false;
	}
	this->serversState.erase( server );
	UNLOCK( &this->aliveServersLock );
	return true;
}

bool ServerRemapMsgHandler::useCoordinatedFlow( const struct sockaddr_in &server ) {
	if ( this->serversState.count( server ) == 0 )
		return false;
	return this->serversState[ server ] != REMAP_NORMAL;
}

bool ServerRemapMsgHandler::allowRemapping( const struct sockaddr_in &server ) {
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

bool ServerRemapMsgHandler::acceptNormalResponse( const struct sockaddr_in &server ) {
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
