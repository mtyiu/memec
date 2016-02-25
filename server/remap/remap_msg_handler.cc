#include <cstdlib>
#include <arpa/inet.h>
#include <pthread.h>
#include <string.h>
#include "../../common/remap/remap_group.hh"
#include "../../common/util/debug.hh"
#include "../main/server.hh"
#include "remap_msg_handler.hh"

SlaveRemapMsgHandler::SlaveRemapMsgHandler() :
		RemapMsgHandler() {
	this->group = ( char* )SLAVE_GROUP;
	LOCK_INIT( &this->aliveSlavesLock );
}

SlaveRemapMsgHandler::~SlaveRemapMsgHandler() {
}

bool SlaveRemapMsgHandler::init( const int ip, const int port, const char *user ) {
	char addrbuf[ 32 ], ipstr[ INET_ADDRSTRLEN ];
	struct in_addr addr;
	memset( addrbuf, 0, 32 );
	addr.s_addr = ip;
	inet_ntop( AF_INET, &addr, ipstr, INET_ADDRSTRLEN );
	sprintf( addrbuf, "%u@%s", ntohs( port ), ipstr );
	return RemapMsgHandler::init( addrbuf , user ) ;
}

void SlaveRemapMsgHandler::quit() {
	RemapMsgHandler::quit();
	if ( this->isListening ) {
		this->stop();
	}
	pthread_join( this->reader, NULL );
	this->isListening = true;
	this->reader = -1;
}

bool SlaveRemapMsgHandler::start() {
	if ( ! this->isConnected )
		return false;

	this->isListening = true;
	// read message using a background thread
	if ( pthread_create( &this->reader, NULL, SlaveRemapMsgHandler::readMessages, this ) < 0 ){
		__ERROR__( "SlaveRemapMsgHandler", "start", "Slave FAILED to start reading remapping messages\n" );
		return false;
	}

	return true;
}

bool SlaveRemapMsgHandler::stop() {
	int ret = 0;
	if ( ! this->isConnected || ! this->isListening )
		return false;

	// stop reading messages
	this->isListening = false;

	return ( ret == 0 );
}
void *SlaveRemapMsgHandler::readMessages( void *argv ) {
	SlaveRemapMsgHandler *myself = ( SlaveRemapMsgHandler* ) argv;
	int ret = 0;

	int service, groups, endian;
	int16 msgType;
	char sender[ MAX_GROUP_NAME ], msg[ MAX_MESSLEN ];
	char targetGroups[ MAX_GROUP_NUM ][ MAX_GROUP_NAME ];

	// handler messages
	while ( myself->isListening && ret >= 0 ) {
		ret = SP_receive( myself->mbox, &service, sender, MAX_GROUP_NUM, &groups, targetGroups, &msgType, &endian, MAX_MESSLEN, msg );
		if ( ret > 0 && myself->isRegularMessage( service ) ) {
			// change state accordingly
			myself->setState( msg, ret );
			myself->increMsgCount();
		}
	}
	if ( ret < 0 ) {
		__ERROR__( "SlaveRemapMsgHandler", "readMessages" , "Reader extis with error code %d\n", ret );
	}

	pthread_exit( ( void * ) &myself->msgCount );
	return ( void* ) &myself->msgCount;
}

void SlaveRemapMsgHandler::setState( char* msg , int len ) {
	RemapState signal;
	uint8_t slaveCount = ( uint8_t ) msg[0];
	struct sockaddr_in slavePeer;
	int ofs = 1;
	uint32_t recordSize = this->slaveStateRecordSize;

	for ( uint8_t i = 0; i < slaveCount; i++ ) {
		slavePeer.sin_addr.s_addr = (*( ( uint32_t * )( msg + ofs ) ) );
		slavePeer.sin_port = *( ( uint16_t * )( msg + ofs + 4 ) );
		signal = ( RemapState ) msg[ ofs + 6 ];
		ofs += recordSize;

		char buf[ INET_ADDRSTRLEN ];
		inet_ntop( AF_INET, &slavePeer.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
		if ( this->slavesState.count( slavePeer ) == 0 ) {
			__ERROR__( "SlaveRemapMsgHandler", "setState" , "Slave %s:%hu not found\n", buf, ntohs( slavePeer.sin_port ) );
			continue;
		}

		Slave *slave = Slave::getInstance();
		LOCK_T &lock = slave->sockets.slavePeers.lock;
		std::vector<SlavePeerSocket *> &slaves = slave->sockets.slavePeers.values;
		SlavePeerSocket *target = 0;

		LOCK( &lock );
		for ( size_t i = 0, count = slaves.size(); i < count; i++ ) {
			if ( slaves[ i ]->equal( slavePeer.sin_addr.s_addr, slavePeer.sin_port ) ) {
				target = slaves[ i ];
				break;
			}
		}
		UNLOCK( &lock );

		if ( ! target ) {
			__ERROR__( "SlaveRemapMsgHandler", "setState" , "SlaveSocket for %s:%hu not found\n", buf, ntohs( slavePeer.sin_port ) );
			continue;
		}

		LOCK( &this->slavesStateLock[ slavePeer ] );
		switch ( signal ) {
			case REMAP_NORMAL:
				__DEBUG__( BLUE, "SlaveRemapMsgHandler", "setState", "REMAP_NORMAL %s:%hu", buf, ntohs( slavePeer.sin_port ) );
				break;
			case REMAP_INTERMEDIATE:
				__INFO__( BLUE, "SlaveRemapMsgHandler", "setState", "REMAP_INTERMEDIATE %s:%hu", buf, ntohs( slavePeer.sin_port ) );
				break;
			case REMAP_COORDINATED:
				break;
			case REMAP_DEGRADED:
				__INFO__( BLUE, "SlaveRemapMsgHandler", "setState", "REMAP_DEGRADED %s:%hu", buf, ntohs( slavePeer.sin_port ) );
				break;
			default:
				__INFO__( BLUE, "SlaveRemapMsgHandler", "setState", "Unknown %d %s:%hu", signal, buf, ntohs( slavePeer.sin_port ) );
				UNLOCK( &this->slavesStateLock[ slavePeer ] );
				return;
		}
		this->slavesState[ slavePeer ] = signal;
		UNLOCK( &this->slavesStateLock[ slavePeer ] );
	}

}

bool SlaveRemapMsgHandler::addAliveSlave( struct sockaddr_in slave ) {
	LOCK( &this->aliveSlavesLock );
	if ( this->slavesState.count( slave ) >= 1 ) {
		UNLOCK( &this->aliveSlavesLock );
		return false;
	}
	this->slavesState[ slave ] = REMAP_NORMAL;
	UNLOCK( &this->aliveSlavesLock );
	return true;
}

bool SlaveRemapMsgHandler::removeAliveSlave( struct sockaddr_in slave ) {
	LOCK( &this->aliveSlavesLock );
	if ( this->slavesState.count( slave ) < 1 ) {
		UNLOCK( &this->aliveSlavesLock );
		return false;
	}
	this->slavesState.erase( slave );
	UNLOCK( &this->aliveSlavesLock );
	return true;
}

bool SlaveRemapMsgHandler::useCoordinatedFlow( const struct sockaddr_in &slave ) {
	if ( this->slavesState.count( slave ) == 0 )
		return false;
	return this->slavesState[ slave ] != REMAP_NORMAL;
}

bool SlaveRemapMsgHandler::allowRemapping( const struct sockaddr_in &slave ) {
	if ( this->slavesState.count( slave ) == 0 )
		return false;

	switch ( this->slavesState[ slave ] ) {
		case REMAP_INTERMEDIATE:
		case REMAP_WAIT_DEGRADED:
		case REMAP_DEGRADED:
			return true;
		default:
			break;
	}

	return false;
}

bool SlaveRemapMsgHandler::acceptNormalResponse( const struct sockaddr_in &slave ) {
	if ( this->slavesState.count( slave ) == 0 )
		return true;

	switch( this->slavesState[ slave ] ) {
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
