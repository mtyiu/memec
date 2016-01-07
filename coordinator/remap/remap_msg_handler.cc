#include <cstring>
#include <cstdlib>
#include <arpa/inet.h>
#include <pthread.h>
#include <sp.h>
#include <unistd.h>
#include "remap_msg_handler.hh"
#include "remap_worker.hh"
#include "../main/coordinator.hh"
#include "../../common/remap/remap_group.hh"

using namespace std;

CoordinatorRemapMsgHandler::CoordinatorRemapMsgHandler() :
		RemapMsgHandler() {
	this->group = ( char* ) COORD_GROUP;

	LOCK_INIT( &this->mastersLock );
	LOCK_INIT( &this->mastersAckLock );
	LOCK_INIT( &this->aliveSlavesLock );
	aliveSlaves.clear();

	Coordinator* coordinator = Coordinator::getInstance();

	uint32_t queue = coordinator->config.coordinator.remap.queue;
	this->eventQueue = new EventQueue<RemapStateEvent>( queue < 2 ? 2 : queue );

	uint32_t workers = coordinator->config.coordinator.remap.worker;
	this->workers = new CoordinatorRemapWorker[ workers < 1 ? 1 : workers ];
}

CoordinatorRemapMsgHandler::~CoordinatorRemapMsgHandler() {
	delete this->eventQueue;
	delete [] this->workers;
}

bool CoordinatorRemapMsgHandler::init( const int ip, const int port, const char *user ) {
	char addrbuf[ 32 ], ipstr[ INET_ADDRSTRLEN ];
	struct in_addr addr;
	memset( addrbuf, 0, 32 );
	addr.s_addr = ip;
	inet_ntop( AF_INET, &addr, ipstr, INET_ADDRSTRLEN );
	sprintf( addrbuf, "%u@%s", ntohs( port ), ipstr );
	RemapMsgHandler::init( addrbuf , user );
	pthread_mutex_init( &this->ackSignalLock, 0 );

	this->isListening = false;
	return ( SP_join( this->mbox, MASTER_GROUP ) == 0 );
}

void CoordinatorRemapMsgHandler::quit() {
	SP_leave( mbox, COORD_GROUP );
	RemapMsgHandler::quit();
	if ( reader > 0 ) {
		pthread_join( this->reader, NULL );
		reader = -1;
	}
}

bool CoordinatorRemapMsgHandler::start() {
	if ( ! this->isConnected )
		return false;
	// start event queue processing
	this->eventQueue->start();
	// read messages using a background thread
	if ( pthread_create( &this->reader, NULL, CoordinatorRemapMsgHandler::readMessages, this ) < 0 ) {
		fprintf( stderr, "Coordinator FAILED to start reading messages" );
		return false;
	}
	this->isListening = true;
	// start all workers
	uint8_t workers = Coordinator::getInstance()->config.coordinator.remap.worker;
	for ( int i = 0; i < workers; i++ ) {
		this->workers[i].start();
	}
	return true;
}

bool CoordinatorRemapMsgHandler::stop() {
	fprintf( stderr, "Coordinator stop\n" );
	int ret = 0;
	if ( ! this->isConnected || ! this->isListening )
		return false;
	// stop event queue processing
	this->eventQueue->stop();
	// no longer listen to incoming messages
	this->isListening = false;
	// avoid blocking call from blocking the stop action
	ret = pthread_cancel( this->reader );
	// stop all workers
	uint8_t workers = Coordinator::getInstance()->config.coordinator.remap.worker;
	for ( int i = 0; i < workers; i++ ) {
		this->workers[i].stop();
	}
	return ( ret == 0 );
}

#define REMAP_PHASE_CHANGE_HANDLER( _ALL_SLAVES_, _CHECKED_SLAVES_, _EVENT_ ) \
	do { \
		_CHECKED_SLAVES_.clear(); \
		\
		bool start = _EVENT_.start; \
		for ( uint32_t i = 0; i < _ALL_SLAVES_->size(); ) { \
			LOCK( &this->slavesStateLock[ _ALL_SLAVES_->at(i) ] ); \
			/* check if slave is alive  \
			if ( this->slavesState.count( _ALL_SLAVES_->at(i) ) == 0 ) { \
				UNLOCK( &this->slavesStateLock[ _ALL_SLAVES_->at(i) ] ); \
				i++; \
				continue; \
			} */  \
			RemapState state = this->slavesState[ _ALL_SLAVES_->at(i) ]; \
			/* no need to trigger remapping if is undefined / already entered / already exited remapping phase */ \
			if ( ( state == REMAP_UNDEFINED ) || ( start && state != REMAP_NORMAL ) || \
				( ( ! start ) && state != REMAP_DEGRADED ) ) \
			{ \
				UNLOCK( &this->slavesStateLock[ _ALL_SLAVES_->at(i) ] ); \
				i++; \
				continue; \
			}  \
			/* set state for sync. and to avoid multiple start from others */ \
			this->slavesState[ _ALL_SLAVES_->at(i) ] = ( start ) ? REMAP_INTERMEDIATE : REMAP_COORDINATED; \
			/* reset ack pool anyway */\
			this->resetMasterAck( _ALL_SLAVES_->at(i) ); \
			_CHECKED_SLAVES_.push_back( _ALL_SLAVES_->at(i) ); \
			UNLOCK( &this->slavesStateLock[ _ALL_SLAVES_->at(i) ] ); \
			_ALL_SLAVES_->erase( _ALL_SLAVES_->begin() + i ); \
		} \
		/* ask master to change state */ \
		if ( this->sendStateToMasters( _CHECKED_SLAVES_ ) == false ) { \
			/* revert the state if failed to start */ \
			for ( uint32_t i = 0; i < _CHECKED_SLAVES_.size() ; i++ ) { \
				LOCK( &this->slavesStateLock[ _ALL_SLAVES_->at(i) ] ); \
				/* TODO is the previous state deterministic ?? */ \
				if ( start && this->slavesState[ _ALL_SLAVES_->at(i) ] == REMAP_INTERMEDIATE ) { \
					this->slavesState[ _ALL_SLAVES_->at(i) ] = REMAP_NORMAL; \
				} else if ( ( ! start ) && this->slavesState[ _ALL_SLAVES_->at(i) ] == REMAP_COORDINATED ) { \
					this->slavesState[ _ALL_SLAVES_->at(i) ] = REMAP_DEGRADED; \
				} else { \
					fprintf( stderr, "unexpected state of slave %u as %d\n",  \
						_ALL_SLAVES_->at(i).sin_addr.s_addr, this->slavesState[ _ALL_SLAVES_->at(i) ]  \
					); \
				} \
				UNLOCK( &this->slavesStateLock[ _ALL_SLAVES_->at(i) ] ); \
			} \
			/* let the caller know all slaves failed */ \
			_ALL_SLAVES_->insert( _ALL_SLAVES_->end(), _CHECKED_SLAVES_.begin(), _CHECKED_SLAVES_.end() ); \
			return false; \
		} \
		/* keep retrying until success */ \
		/* TODO reset only failed ones instead */ \
		while ( ! this->insertRepeatedEvents( _EVENT_, &_CHECKED_SLAVES_ ) ); \
	} while (0)


bool CoordinatorRemapMsgHandler::transitToDegraded( std::vector<struct sockaddr_in> *slaves ) {
	RemapStateEvent event;
	event.start = true;
	vector<struct sockaddr_in> slavesToStart;

	REMAP_PHASE_CHANGE_HANDLER( slaves, slavesToStart, event );

	return true;
}

bool CoordinatorRemapMsgHandler::transitToDegradedEnd( const struct sockaddr_in &slave ) {
	// all operation to slave get lock from coordinator
	Coordinator *coordinator = Coordinator::getInstance();
	LOCK_T *lock = &coordinator->sockets.slaves.lock;
	std::vector<SlaveSocket *> &slaves = coordinator->sockets.slaves.values;
	SlaveSocket *target = 0;

	LOCK( lock );
	for ( size_t i = 0, size = slaves.size(); i < size; i++ ) {
		if ( slaves[ i ]->equal( slave.sin_addr.s_addr, slave.sin_port ) ) {
			target = slaves[ i ];
			break;
		}
	}
	UNLOCK( lock );

	if ( target ) {
		PendingTransition *pendingTransition = coordinator->pending.findPendingTransition( target->instanceId, true );

		if ( pendingTransition ) {
			pthread_mutex_lock( &pendingTransition->lock );
			while ( pendingTransition->pending )
				pthread_cond_wait( &pendingTransition->cond, &pendingTransition->lock );
			pthread_mutex_unlock( &pendingTransition->lock );

			coordinator->pending.erasePendingTransition( target->instanceId, true );
		} else {
			fprintf( stderr, "Pending transition not found.\n" );
		}
	} else {
		fprintf( stderr, "Slave not found.\n" );
	}

	LOCK( &this->aliveSlavesLock );
	if ( this->crashedSlaves.find( slave ) != this->crashedSlaves.end() ) {
		printf( "Triggering reconstruction for crashed slave...\n" );
		SlaveEvent event;
		event.triggerReconstruction( slave );
		coordinator->eventQueue.insert( event );
	}
	UNLOCK( &this->aliveSlavesLock );

	return true;
}

bool CoordinatorRemapMsgHandler::transitToNormal( std::vector<struct sockaddr_in> *slaves ) {
	RemapStateEvent event;
	event.start = false;
	vector<struct sockaddr_in> slavesToStop;

	LOCK( &this->aliveSlavesLock );
	for ( auto it = slaves->begin(); it != slaves->end(); ) {
		if ( this->crashedSlaves.count( *it ) > 0 ) {
			// Never transit to normal state if it is crashed
			it = slaves->erase( it );
		} else {
			it++;
		}
	}
	UNLOCK( &this->aliveSlavesLock );

	REMAP_PHASE_CHANGE_HANDLER( slaves, slavesToStop, event );

	return true;
}

bool CoordinatorRemapMsgHandler::transitToNormalEnd( const struct sockaddr_in &slave ) {
	// backward migration before getting back to normal
	Coordinator *coordinator = Coordinator::getInstance();

	pthread_mutex_t lock;
	pthread_cond_t cond;
	bool done;

	pthread_mutex_init( &lock, 0 );
	pthread_cond_init( &cond, 0 );

	// REMAP SET
	done = false;
	coordinator->syncRemappedData( slave, &lock, &cond, &done );

	pthread_mutex_lock( &lock );
	while( ! done )
		pthread_cond_wait( &cond, &lock );
	pthread_mutex_unlock( &lock );

	coordinator->remappingRecords.erase( slave );

	// DEGRADED
	done = false;
	coordinator->releaseDegradedLock( slave, &lock, &cond, &done );

	pthread_mutex_lock( &lock );
	while( ! done )
		pthread_cond_wait( &cond, &lock );
	pthread_mutex_unlock( &lock );

	return true;
}

#undef REMAP_PHASE_CHANGE_HANDLER

bool CoordinatorRemapMsgHandler::insertRepeatedEvents( RemapStateEvent event, std::vector<struct sockaddr_in> *slaves ) {
	bool ret = true;
	uint32_t i = 0;
	for ( i = 0; i < slaves->size(); i++ ) {
		event.slave = slaves->at(i);
		ret = this->eventQueue->insert( event );
		if ( ! ret )
			break;
	}
	// notify the caller if any slave cannot start remapping
	if ( ret )
		slaves->clear();
	else
		slaves->erase( slaves->begin(), slaves->begin()+i );
	return ret;
}

bool CoordinatorRemapMsgHandler::isMasterLeft( int service, char *msg, char *subject ) {
	// assume masters name themselves "[PREFIX][0-9]*"
	if ( this->isMemberLeave( service ) ) {
		if ( strncmp( subject + 1, MASTER_PREFIX , MASTER_PREFIX_LEN ) == 0 ) {
			return true;
		}
	}
	return false;
}

bool CoordinatorRemapMsgHandler::isMasterJoin( int service, char *msg, char *subject ) {
	// assume masters name themselves "[PREFIX][0-9]*"
	if ( this->isMemberJoin( service ) ) {
		if ( strncmp( subject + 1, MASTER_PREFIX , MASTER_PREFIX_LEN ) == 0 ) {
			return true;
		}
	}
	return false;
}

/*
 * packet: [# of slaves](1) [ [ip addr](4) [port](2) [state](1) ](7) [..](7) [..](7) ...
 */
bool CoordinatorRemapMsgHandler::sendStateToMasters( std::vector<struct sockaddr_in> slaves ) {
	int recordSize = this->slaveStateRecordSize;

	if ( slaves.size() == 0 ) {
		slaves = std::vector<struct sockaddr_in>( this->aliveSlaves.begin(), this->aliveSlaves.end() );
	} else if ( slaves.size() > 255 || slaves.size() * recordSize + 1 > MAX_MESSLEN ) {
		fprintf( stderr, "Too much slaves to include in message" );
		return false;
	}

	return sendState( slaves, MASTER_GROUP );
}

bool CoordinatorRemapMsgHandler::sendStateToMasters( struct sockaddr_in slave ) {
	std::vector<struct sockaddr_in> slaves;
	slaves.push_back( slave );
	return sendStateToMasters( slaves );
}

void *CoordinatorRemapMsgHandler::readMessages( void *argv ) {
	int ret = 0;

	int service, groups, endian;
	int16 msgType;
	char sender[ MAX_GROUP_NAME ], msg[ MAX_MESSLEN ];
	char targetGroups[ MAX_GROUP_NUM ][ MAX_GROUP_NAME ];
	char* subject;

	bool regular = false, fromMaster = false;

	CoordinatorRemapMsgHandler *myself = ( CoordinatorRemapMsgHandler* ) argv;

	while( myself->isListening && ret >= 0 ) {
		ret = SP_receive( myself->mbox, &service, sender, MAX_GROUP_NUM, &groups, targetGroups, &msgType, &endian, MAX_MESSLEN, msg );

		subject = &msg[ SP_get_vs_set_offset_memb_mess() ];
		regular = myself->isRegularMessage( service );
		fromMaster = ( strncmp( sender, MASTER_GROUP, MASTER_GROUP_LEN ) == 0 );

		if ( ! regular && fromMaster && myself->isMasterJoin( service , msg, subject ) ) {
			// master joined ( masters group )
			myself->addAliveMaster( subject );
			// notify the new master about the remapping state
			std::vector<struct sockaddr_in> slaves;
			myself->sendStateToMasters( slaves );
		} else if ( ! regular && myself->isMasterLeft( service , msg, subject ) ) {
			// master left
			myself->removeAliveMaster( subject );
		} else if ( regular ){
			// ack from masters, etc.
			myself->updateState( sender, msg, ret );
		}

		myself->increMsgCount();
	}
	if ( ret < 0 )
		fprintf( stderr, "coord reader exits with error code %d\n", ret );

	return ( void* ) &myself->msgCount;
}

/*
 * packet: [# of slaves](1) [ [ip addr](4) [port](2) [state](1) ](7) [..](7) [..](7) ...
 */
bool CoordinatorRemapMsgHandler::updateState( char *subject, char *msg, int len ) {

	// ignore messages that not from masters
	if ( strncmp( subject + 1, MASTER_PREFIX, MASTER_PREFIX_LEN ) != 0 ) {
		return false;
	}

	uint8_t slaveCount = msg[0], state = 0;
	struct sockaddr_in slave;
	int ofs = 1;
	int recordSize = this->slaveStateRecordSize;

	LOCK( &this->mastersAckLock );
	// check slave by slave for changes
	for ( uint8_t i = 0; i < slaveCount; i++ ) {
		slave.sin_addr.s_addr = *( ( uint32_t * ) ( msg + ofs ) );
		slave.sin_port = *( ( uint16_t *) ( msg + ofs + 4 ) );
		state = msg[ ofs + 6 ];
		ofs += recordSize;
		// ignore changes for non-existing slaves or slaves in invalid state
		// TODO sync state with master with invalid state of slaves
		if ( this->slavesState.count( slave ) == 0 ||
			( this->slavesState[ slave ] != REMAP_INTERMEDIATE &&
			this->slavesState[ slave ] != REMAP_COORDINATED )
		) {
			continue;
		}
		// check if the ack is corresponding to a correct state
		if ( ( this->slavesState[ slave ] == REMAP_INTERMEDIATE && state != REMAP_WAIT_DEGRADED ) ||
			( this->slavesState[ slave ] == REMAP_COORDINATED && state != REMAP_WAIT_NORMAL ) ) {
			continue;
		}

		if ( this->ackMasters.count( slave ) && aliveMasters.count( string( subject ) ) )
			ackMasters[ slave ]->insert( string( subject ) );
		else {
			char buf[ INET_ADDRSTRLEN ];
			inet_ntop( AF_INET, &slave.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
			fprintf(
				stderr, "master [%s] or slave [%s:%hu] not found !!",
				subject, buf, ntohs( slave.sin_port )
			);
		}
		// check if all master acked
		UNLOCK( &this->mastersAckLock );
		this->isAllMasterAcked( slave );
		LOCK( &this->mastersAckLock );
	}
	UNLOCK( &this->mastersAckLock );

	return true;
}

void CoordinatorRemapMsgHandler::addAliveMaster( char *name ) {
	LOCK( &this->mastersLock );
	aliveMasters.insert( string( name ) );
	UNLOCK( &this->mastersLock );
}

void CoordinatorRemapMsgHandler::removeAliveMaster( char *name ) {
	LOCK( &this->mastersLock );
	aliveMasters.erase( string( name ) );
	UNLOCK( &this->mastersLock );
	// remove the master from all alive slaves ack. pool
	LOCK( &this->mastersAckLock );
	for( auto it : ackMasters ) {
		it.second->erase( name );
	}
	UNLOCK( &this->mastersAckLock );
}

bool CoordinatorRemapMsgHandler::addAliveSlave( struct sockaddr_in slave ) {
	// alive slaves list
	LOCK( &this->aliveSlavesLock );
	if ( this->aliveSlaves.count( slave ) > 0 ) {
		UNLOCK( &this->aliveSlavesLock );
		return false;
	}
	aliveSlaves.insert( slave );
	UNLOCK( &this->aliveSlavesLock );
	// add the state
	LOCK_INIT( &this->slavesStateLock[ slave ] );
	LOCK( &this->slavesStateLock[ slave ] );
	slavesState[ slave ] = REMAP_NORMAL;
	UNLOCK( &this->slavesStateLock [ slave ] );
	// master ack pool
	LOCK( &this->mastersAckLock );
	ackMasters[ slave ] = new std::set<std::string>();
	UNLOCK( &this->mastersAckLock );
	// waiting slave
	pthread_cond_init( &this->ackSignal[ slave ], NULL );
	return true;
}

bool CoordinatorRemapMsgHandler::addCrashedSlave( struct sockaddr_in slave ) {
	LOCK( &this->aliveSlavesLock );
	if ( this->crashedSlaves.count( slave ) > 0 ) {
		UNLOCK( &this->aliveSlavesLock );
		return false;
	}
	crashedSlaves.insert( slave );
	UNLOCK( &this->aliveSlavesLock );
	return true;
}

bool CoordinatorRemapMsgHandler::removeAliveSlave( struct sockaddr_in slave ) {
	// alive slaves list
	LOCK( &this->aliveSlavesLock );
	if ( this->aliveSlaves.count( slave ) == 0 ) {
		UNLOCK( &this->aliveSlavesLock );
		return false;
	}
	aliveSlaves.erase( slave );
	crashedSlaves.erase( slave );
	UNLOCK( &this->aliveSlavesLock );

	// add the state
	LOCK( &this->slavesStateLock[ slave ] );
	slavesState.erase( slave );
	UNLOCK( &this->slavesStateLock[ slave ] );
	this->slavesStateLock.erase( slave );
	// master ack pool
	LOCK( &this->mastersAckLock );
	delete ackMasters[ slave ];
	ackMasters.erase( slave );
	UNLOCK( &this->mastersAckLock );
	// waiting slave
	pthread_cond_broadcast( &this->ackSignal[ slave ] );
	return true;
}

bool CoordinatorRemapMsgHandler::resetMasterAck( struct sockaddr_in slave ) {
	LOCK( &this->mastersAckLock );
	// abort reset if slave does not exists
	if ( ackMasters.count( slave ) == 0 ) {
		UNLOCK( &this->mastersLock );
		return false;
	}
	ackMasters[ slave ]->clear();
	UNLOCK( &this->mastersAckLock );
	return true;
}

bool CoordinatorRemapMsgHandler::isAllMasterAcked( struct sockaddr_in slave ) {
	bool allAcked = false;
	char buf[ INET_ADDRSTRLEN ];
	inet_ntop( AF_INET, &slave.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
	LOCK( &this->mastersAckLock );
	// TODO abort checking if slave is no longer accessiable
	if ( ackMasters.count( slave ) == 0 ) {
		UNLOCK( &this->mastersAckLock );
		return true;
	}
	allAcked = ( aliveMasters.size() == ackMasters[ slave ]->size() );
	if ( allAcked ) {
		printf( "Slave %s:%hu changes its state to: (%d) ", buf, ntohs( slave.sin_port ), this->slavesState[ slave ] );
		switch( this->slavesState[ slave ] ) {
			case REMAP_UNDEFINED:
				printf( "REMAP_UNDEFINED\n" );
				break;
			case REMAP_NORMAL:
				printf( "REMAP_NORMAL\n" );
				break;
			case REMAP_INTERMEDIATE:
				printf( "REMAP_INTERMEDIATE\n" );
				break;
			case REMAP_DEGRADED:
				printf( "REMAP_DEGRADED\n" );
				break;
			case REMAP_COORDINATED:
				printf( "REMAP_COORDINATED\n" );
				break;
			default:
				printf( "UNKNOWN\n" );
				break;
		}
		pthread_cond_broadcast( &this->ackSignal[ slave ] );
	}
	UNLOCK( &this->mastersAckLock );
	return allAcked;
}

bool CoordinatorRemapMsgHandler::isInTransition( const struct sockaddr_in &slave ) {
	bool ret;
	LOCK( &this->slavesStateLock[ slave ] );
	ret = ( slavesState[ slave ] == REMAP_INTERMEDIATE ) || ( slavesState[ slave ] == REMAP_COORDINATED );
	UNLOCK( &this->slavesStateLock[ slave ] );
	return ret;
}

bool CoordinatorRemapMsgHandler::reachMaximumRemapped( uint32_t maximum ) {
	uint32_t count = 0;
	LOCK( &this->aliveSlavesLock );
	for ( auto it = this->aliveSlaves.begin(); it != this->aliveSlaves.end(); it++ ) {
		const struct sockaddr_in &slave = ( *it );
		LOCK( &this->slavesStateLock[ slave ] );
		if ( slavesState[ slave ] != REMAP_NORMAL ) {
			count++;
		}
		UNLOCK( &this->slavesStateLock[ slave ] );

		if ( count == maximum )
			break;
	}
	UNLOCK( &this->aliveSlavesLock );
	return ( count == maximum );
}
