#include <cstdlib>
#include <arpa/inet.h>
#include <pthread.h>
#include <string.h>
#include "../../common/remap/remap_group.hh"
#include "../../common/util/debug.hh"
#include "../main/master.hh"
#include "remap_msg_handler.hh"

#define MASTER_BG_ACK_INTVL		5

MasterRemapMsgHandler::MasterRemapMsgHandler() :
		RemapMsgHandler() {
	this->group = ( char* )MASTER_GROUP;
	LOCK_INIT( &this->aliveSlavesLock );
}

MasterRemapMsgHandler::~MasterRemapMsgHandler() {
}

bool MasterRemapMsgHandler::init( const int ip, const int port, const char *user ) {
	char addrbuf[ 32 ], ipstr[ INET_ADDRSTRLEN ];
	struct in_addr addr;
	memset( addrbuf, 0, 32 );
	addr.s_addr = ip;
	inet_ntop( AF_INET, &addr, ipstr, INET_ADDRSTRLEN );
	sprintf( addrbuf, "%u@%s", ntohs( port ), ipstr );
	return RemapMsgHandler::init( addrbuf , user ) ;
}

void MasterRemapMsgHandler::quit() {
	RemapMsgHandler::quit();
	if ( this->isListening ) {
		this->stop();
	}
	pthread_join( this->reader, NULL );
	this->isListening = true;
	this->reader = -1;
}

bool MasterRemapMsgHandler::start() {
	if ( ! this->isConnected )
		return false;

	this->isListening = true;
	// read message using a background thread
	if ( pthread_create( &this->reader, NULL, MasterRemapMsgHandler::readMessages, this ) < 0 ){
		__ERROR__( "MasterRemapMsgHandler", "start", "Master FAILED to start reading remapping messages\n" );
		return false;
	}
	if ( pthread_create( &this->acker, NULL, MasterRemapMsgHandler::ackRemapLoop, this ) < 0 ){
		__ERROR__( "MasterRemapMsgHandler", "start", "Master FAILED to start background ack. service.\n" );
	}

	return true;
}

bool MasterRemapMsgHandler::stop() {
	int ret = 0;
	if ( ! this->isConnected || ! this->isListening )
		return false;

	// stop reading messages
	this->isListening = false;
	// avoid blocking call from blocking the stop action
	ret = pthread_cancel( this->reader );

	return ( ret == 0 );
}
void *MasterRemapMsgHandler::readMessages( void *argv ) {
	MasterRemapMsgHandler *myself = ( MasterRemapMsgHandler* ) argv;
	int ret = 0;

	int service, groups, endian;
	int16 msgType;
	char sender[ MAX_GROUP_NAME ], msg[ MAX_MESSLEN ];
	char targetGroups[ MAX_GROUP_NUM ][ MAX_GROUP_NAME ];

	// handler messages
	while ( myself->isListening && ret >= 0 ) {
		ret = SP_receive( myself->mbox, &service, sender, MAX_GROUP_NUM, &groups, targetGroups, &msgType, &endian, MAX_MESSLEN, msg );
		if ( ret > 0 && myself->isRegularMessage( service ) ) {
			// change status accordingly
			myself->setStatus( msg, ret );
			myself->increMsgCount();
		}
	}
	if ( ret < 0 ) {
		__ERROR__( "MasterRemapMsgHandler", "readMessages" , "Reader extis with error code %d\n", ret );
	}

	pthread_exit( ( void * ) &myself->msgCount );
	return ( void* ) &myself->msgCount;
}

void *MasterRemapMsgHandler::ackRemapLoop( void *argv ) {

	MasterRemapMsgHandler *myself = ( MasterRemapMsgHandler* ) argv;
	while ( myself->isListening ) {
		sleep( MASTER_BG_ACK_INTVL );
		myself->ackRemap();
	}

	pthread_exit(0);
	return NULL;
}

void MasterRemapMsgHandler::setStatus( char* msg , int len ) {
	RemapStatus signal;
	uint8_t slaveCount = ( uint8_t ) msg[0];
	struct sockaddr_in slave;
	int ofs = 1;
	uint32_t recordSize = this->slaveStatusRecordSize;

	for ( uint8_t i = 0; i < slaveCount; i++ ) {
		slave.sin_addr.s_addr = (*( ( uint32_t * )( msg + ofs ) ) );
		slave.sin_port = *( ( uint16_t * )( msg + ofs + 4 ) );
		signal = ( RemapStatus ) msg[ ofs + 6 ];
		ofs += recordSize;

		char buf[ INET_ADDRSTRLEN ];
		inet_ntop( AF_INET, &slave.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
		if ( this->slavesStatus.count( slave ) == 0 ) {
			__ERROR__( "MasterRemapMsgHandler", "setStatus" , "slave %s:%hu not found\n", buf, slave.sin_port );
			continue;
		}

		LOCK( &this->slavesStatusLock[ slave ] );
		switch ( signal ) {
			case REMAP_PREPARE_START:
				__DEBUG__( BLUE, "MasterRemapMsgHandler", "setStatus", "REMAP_PREPARE_START %s:%hu", buf, slave.sin_port );
				// waiting for other masters
				if ( this->slavesStatus[ slave ] == REMAP_WAIT_START )
					signal = REMAP_WAIT_START;
				break;
			case REMAP_START:
				__DEBUG__( BLUE, "MasterRemapMsgHandler", "setStatus", "REMAP_START %s:%hu", buf, slave.sin_port );
				break;
			case REMAP_PREPARE_END:
				__DEBUG__( BLUE, "MasterRemapMsgHandler", "setStatus", "REMAP_PREPARE_END %s:%hu", buf, slave.sin_port );
				// waiting for other masters
				if ( this->slavesStatus[ slave ] == REMAP_WAIT_END )
					signal = REMAP_WAIT_END ;
				break;
			case REMAP_NONE:
				__DEBUG__( BLUE, "MasterRemapMsgHandler", "setStatus", "REMAP_NONE = END %s:%hu", buf, slave.sin_port );
				break;
			default:
				__DEBUG__( BLUE, "MasterRemapMsgHandler", "setStatus", "REMAP_%d %s:%hu", signal, buf, slave.sin_port );
				UNLOCK( &this->slavesStatusLock[ slave ] );
				return;
		}
		this->slavesStatus[ slave ] = signal;
		UNLOCK( &this->slavesStatusLock[ slave ] );

		// check if the change can be immediately acked
		if ( signal == REMAP_PREPARE_START || signal == REMAP_PREPARE_END )
			this->ackRemap( &slave );
	}

}

bool MasterRemapMsgHandler::addAliveSlave( struct sockaddr_in slave ) {
	LOCK( &this->aliveSlavesLock );
	if ( this->slavesStatus.count( slave ) >= 1 ) {
		UNLOCK( &this->aliveSlavesLock );
		return false;
	}
	this->slavesStatus[ slave ] = REMAP_NONE;
	UNLOCK( &this->aliveSlavesLock );
	return true;
}

bool MasterRemapMsgHandler::removeAliveSlave( struct sockaddr_in slave ) {
	LOCK( &this->aliveSlavesLock );
	if ( this->slavesStatus.count( slave ) < 1 ) {
		UNLOCK( &this->aliveSlavesLock );
		return false;
	}
	this->slavesStatus.erase( slave );
	UNLOCK( &this->aliveSlavesLock );
	return true;
}

bool MasterRemapMsgHandler::useRemappingFlow( struct sockaddr_in slave ) {
	if ( this->slavesStatus.count( slave ) == 0 ) 
		return false;

	switch ( this->slavesStatus[ slave ] ) {
		case REMAP_PREPARE_START:
		case REMAP_WAIT_START:
		case REMAP_START:
		case REMAP_PREPARE_END:
			return true;
		default:
			break;
	}
	return false;
}

bool MasterRemapMsgHandler::allowRemapping( struct sockaddr_in slave ) {
	if ( this->slavesStatus.count( slave ) == 0 ) 
		return false;

	switch ( this->slavesStatus[ slave ] ) {
		case REMAP_START:
			return true;
		default:
			break;
	}

	return false;
}

bool MasterRemapMsgHandler::sendStatusToCoordinator( std::vector<struct sockaddr_in> slaves ) {
	uint32_t recordSize = this->slaveStatusRecordSize;
	if ( slaves.size() == 0 ) {
		// TODO send all slave status
		//slaves = std::vector<struct sockaddr_in>( this->aliveSlaves.begin(), this->aliveSlaves.end() );
		return false;
	} else if ( slaves.size() > 255 || slaves.size() * recordSize + 1 > MAX_MESSLEN ) {
		fprintf( stderr, "Too much slaves to include in message" );
		return false;
	}
	return sendStatus( slaves, COORD_GROUP );

}

bool MasterRemapMsgHandler::sendStatusToCoordinator( struct sockaddr_in slave ) {
	std::vector<struct sockaddr_in> slaves;
	slaves.push_back( slave );
	return sendStatusToCoordinator( slaves );
}

bool MasterRemapMsgHandler::ackRemap( struct sockaddr_in slave ) {
	return ackRemap( &slave );
}

bool MasterRemapMsgHandler::ackRemap( struct sockaddr_in *slave ) {
	LOCK( &this->aliveSlavesLock );
	if ( slave ) {
		// specific slave
		if ( this->slavesStatusLock.count( *slave ) == 0 ) {
			UNLOCK( &this->aliveSlavesLock );
			return false;
		}
		if ( this->checkAckRemapForSlave( *slave ) )
			sendStatusToCoordinator( *slave );
	} else {
		// check all slaves
		std::vector<struct sockaddr_in> slavesToAck;
		for ( auto s : this->slavesStatus ) {
			if ( this->checkAckRemapForSlave( s.first ) )
				slavesToAck.push_back( s.first );
		}
		if ( ! slavesToAck.empty() ) 
			sendStatusToCoordinator( slavesToAck );
	}
	UNLOCK( &this->aliveSlavesLock );

	return true;
}

bool MasterRemapMsgHandler::checkAckRemapForSlave( struct sockaddr_in slave ) {
	uint32_t normal = 0, remap = 0;
	Counter* counter = Master::getInstance()->counters.slaves[ slave ];
	if ( counter == NULL )
		return false;
	normal = counter->getNormal();
	remap = counter->getRemapping();
	LOCK( &this->slavesStatusLock[ slave ] );
	RemapStatus status = this->slavesStatus[ slave ];

	if ( ( status == REMAP_PREPARE_START && normal > 0 ) ||
			( status == REMAP_PREPARE_END && remap > 0 ) ||
			( status != REMAP_PREPARE_START && status != REMAP_PREPARE_END ) ) {
		UNLOCK( &this->slavesStatusLock[ slave ] );
		return false;
	}

	switch ( status ) {
		case REMAP_PREPARE_START:
			status = REMAP_WAIT_START;
			break;
		case REMAP_PREPARE_END:
			status = REMAP_WAIT_END;
			break;
		default:
			UNLOCK( &this->slavesStatusLock[ slave ] );
			return false;
	}
	this->slavesStatus[ slave ] = status;
	UNLOCK( &this->slavesStatusLock[ slave ] );

	return true;
}
