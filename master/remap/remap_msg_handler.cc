#include <cstdlib>
#include <arpa/inet.h>
#include <pthread.h>
#include <string.h>
#include "../../common/remap/remap_group.hh"
#include "../../common/util/debug.hh"
#include "../main/master.hh"
#include "remap_msg_handler.hh"

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
	this->bgAckInterval = Master::getInstance()->config.master.remap.backgroundAck;
	if ( this->bgAckInterval > 0 && pthread_create( &this->acker, NULL, MasterRemapMsgHandler::ackTransitThread, this ) < 0 ){
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
			// change state accordingly
			myself->setState( msg, ret );
			myself->increMsgCount();
		}
	}
	if ( ret < 0 ) {
		__ERROR__( "MasterRemapMsgHandler", "readMessages" , "Reader extis with error code %d\n", ret );
	}

	pthread_exit( ( void * ) &myself->msgCount );
	return ( void* ) &myself->msgCount;
}

void *MasterRemapMsgHandler::ackTransitThread( void *argv ) {

	MasterRemapMsgHandler *myself = ( MasterRemapMsgHandler* ) argv;

	while ( myself->bgAckInterval > 0 && myself->isListening ) {
		sleep( myself->bgAckInterval );
		myself->ackTransit();
	}

	pthread_exit(0);
	return NULL;
}

void MasterRemapMsgHandler::setState( char* msg , int len ) {
	RemapState signal;
	uint8_t slaveCount = ( uint8_t ) msg[0];
	struct sockaddr_in slave;
	int ofs = 1;
	uint32_t recordSize = this->slaveStateRecordSize;

	for ( uint8_t i = 0; i < slaveCount; i++ ) {
		slave.sin_addr.s_addr = (*( ( uint32_t * )( msg + ofs ) ) );
		slave.sin_port = *( ( uint16_t * )( msg + ofs + 4 ) );
		signal = ( RemapState ) msg[ ofs + 6 ];
		ofs += recordSize;

		char buf[ INET_ADDRSTRLEN ];
		inet_ntop( AF_INET, &slave.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
		if ( this->slavesState.count( slave ) == 0 ) {
			__ERROR__( "MasterRemapMsgHandler", "setState" , "Slave %s:%hu not found\n", buf, ntohs( slave.sin_port ) );
			continue;
		}

		Master *master = Master::getInstance();
		LOCK_T &lock = master->sockets.slaves.lock;
		std::vector<SlaveSocket *> &slaves = master->sockets.slaves.values;
		SlaveSocket *target = 0;

		LOCK( &lock );
		for ( size_t i = 0, count = slaves.size(); i < count; i++ ) {
			if ( slaves[ i ]->equal( slave.sin_addr.s_addr, slave.sin_port ) ) {
				target = slaves[ i ];
				break;
			}
		}
		UNLOCK( &lock );

		if ( ! target ) {
			__ERROR__( "MasterRemapMsgHandler", "setState" , "SlaveSocket for %s:%hu not found\n", buf, ntohs( slave.sin_port ) );
			continue;
		}

		LOCK( &this->slavesStateLock[ slave ] );
		RemapState state = this->slavesState[ slave ];
		switch ( signal ) {
			case REMAP_NORMAL:
				__DEBUG__( BLUE, "MasterRemapMsgHandler", "setState", "REMAP_NORMAL %s:%hu", buf, ntohs( slave.sin_port ) );
				break;
			case REMAP_INTERMEDIATE:
				__INFO__( BLUE, "MasterRemapMsgHandler", "setState", "REMAP_INTERMEDIATE %s:%hu", buf, ntohs( slave.sin_port ) );
				if ( state == REMAP_WAIT_DEGRADED )
					signal = state;
				else {
					// Insert a new event
					SlaveEvent event;
					event.syncMetadata( target );
					Master::getInstance()->eventQueue.insert( event );
					// revert parity deltas
					this->stateTransitInfo[ slave ].unsetCompleted( true );
					Master::getInstance()->revertDelta(
						0, target, 0,
						&this->stateTransitInfo[ slave ].counter.parityRevert.lock,
						&this->stateTransitInfo[ slave ].counter.parityRevert.value,
						true
					);
					// scan for normal requests to be completed
					MasterWorker::gatherPendingNormalRequests( target );
				}
				break;
			case REMAP_COORDINATED:
				__INFO__( BLUE, "MasterRemapMsgHandler", "setState", "REMAP_COORDINATED %s:%hu", buf, ntohs( slave.sin_port ) );
				if ( state == REMAP_WAIT_NORMAL )
					signal = state;
				break;
			case REMAP_DEGRADED:
				__INFO__( BLUE, "MasterRemapMsgHandler", "setState", "REMAP_DEGRADED %s:%hu", buf, ntohs( slave.sin_port ) );
				if ( state == REMAP_INTERMEDIATE )
					__ERROR__( "MasterRemapMsgHandler", "setState", "Not yet ready for transition to DEGRADED!\n" );
				break;
			default:
				__INFO__( BLUE, "MasterRemapMsgHandler", "setState", "Unknown %d %s:%hu", signal, buf, ntohs( slave.sin_port ) );
				UNLOCK( &this->slavesStateLock[ slave ] );
				return;
		}
		this->slavesState[ slave ] = signal;
		state = this->slavesState[ slave ];
		UNLOCK( &this->slavesStateLock[ slave ] );

		// actions/cleanup after state change
		switch ( state ) {
			case REMAP_INTERMEDIATE:
				// clean up pending items associated with this slave
				// TODO handle the case when insert happened after cleanup ( useCoordinatedFlow returns false > erase > add )
				// MasterWorker::removePending( target );
				this->ackTransit();
				break;
			case REMAP_COORDINATED:
				// check if the change can be immediately acked
				this->ackTransit( &slave );
				break;
			case REMAP_DEGRADED:
				// start replaying the requests
				// MasterWorker::replayRequestPrepare( target );
				// MasterWorker::replayRequest( target );
				break;
			default:
				break;
		}
	}

}

bool MasterRemapMsgHandler::addAliveSlave( struct sockaddr_in slave ) {
	LOCK( &this->aliveSlavesLock );
	if ( this->slavesState.count( slave ) >= 1 ) {
		UNLOCK( &this->aliveSlavesLock );
		return false;
	}
	this->slavesState[ slave ] = REMAP_NORMAL;
	this->stateTransitInfo[ slave ] = StateTransitInfo();
	UNLOCK( &this->aliveSlavesLock );
	return true;
}

bool MasterRemapMsgHandler::removeAliveSlave( struct sockaddr_in slave ) {
	LOCK( &this->aliveSlavesLock );
	if ( this->slavesState.count( slave ) < 1 ) {
		UNLOCK( &this->aliveSlavesLock );
		return false;
	}
	this->slavesState.erase( slave );
	this->stateTransitInfo.erase( slave );
	UNLOCK( &this->aliveSlavesLock );
	return true;
}

bool MasterRemapMsgHandler::useCoordinatedFlow( const struct sockaddr_in &slave ) {
	if ( this->slavesState.count( slave ) == 0 )
		return false;
	return this->slavesState[ slave ] != REMAP_NORMAL;
	/*
	switch ( this->slavesState[ slave ] ) {
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

bool MasterRemapMsgHandler::allowRemapping( const struct sockaddr_in &slave ) {
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

bool MasterRemapMsgHandler::acceptNormalResponse( const struct sockaddr_in &slave ) {
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

bool MasterRemapMsgHandler::sendStateToCoordinator( std::vector<struct sockaddr_in> slaves ) {
	uint32_t recordSize = this->slaveStateRecordSize;
	if ( slaves.size() == 0 ) {
		// TODO send all slave state
		//slaves = std::vector<struct sockaddr_in>( this->aliveSlaves.begin(), this->aliveSlaves.end() );
		return false;
	} else if ( slaves.size() > 255 || slaves.size() * recordSize + 1 > MAX_MESSLEN ) {
		fprintf( stderr, "Too much slaves to include in message" );
		return false;
	}
	return sendState( slaves, COORD_GROUP );

}

bool MasterRemapMsgHandler::sendStateToCoordinator( struct sockaddr_in slave ) {
	std::vector<struct sockaddr_in> slaves;
	slaves.push_back( slave );
	return sendStateToCoordinator( slaves );
}

bool MasterRemapMsgHandler::ackTransit( struct sockaddr_in slave ) {
	return ackTransit( &slave );
}

// Call after all metadata is synchonized
bool MasterRemapMsgHandler::ackTransit( struct sockaddr_in *slave ) {
	LOCK( &this->aliveSlavesLock );
	if ( slave ) {
		// specific slave
		if ( this->slavesStateLock.count( *slave ) == 0 ) {
			UNLOCK( &this->aliveSlavesLock );
			return false;
		}
		if ( this->checkAckForSlave( *slave ) )
			sendStateToCoordinator( *slave );
	} else {
		// check all slaves
		std::vector<struct sockaddr_in> slavesToAck;
		for ( auto s : this->slavesState ) {
			if ( this->checkAckForSlave( s.first ) )
				slavesToAck.push_back( s.first );
		}
		if ( ! slavesToAck.empty() )
			sendStateToCoordinator( slavesToAck );
	}
	UNLOCK( &this->aliveSlavesLock );

	return true;
}

bool MasterRemapMsgHandler::checkAckForSlave( struct sockaddr_in slave ) {
	LOCK( &this->slavesStateLock[ slave ] );
	RemapState state = this->slavesState[ slave ];
	char buf[ INET_ADDRSTRLEN ];
	inet_ntop( AF_INET, &slave.sin_addr.s_addr, buf, INET_ADDRSTRLEN );

	if (
		( state == REMAP_NORMAL ) ||
	    ( state == REMAP_INTERMEDIATE && 
			( 
				false /* yet sync all meta */ || 
				this->stateTransitInfo[ slave ].getParityRevertCounterVal() > 0 /* yet undo all parity */ || 
				! this->stateTransitInfo[ slave ].counter.pendingNormalRequests.completed /* yet complete all normal requests */ 
			)
		) ||
	    ( state == REMAP_COORDINATED && false ) ) {
		UNLOCK( &this->slavesStateLock[ slave ] );
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
			UNLOCK( &this->slavesStateLock[ slave ] );
			return false;
	}
	this->slavesState[ slave ] = state;
	UNLOCK( &this->slavesStateLock[ slave ] );

	return true;
}
