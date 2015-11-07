#include <cstring>
#include <cstdlib>
#include <arpa/inet.h>
#include <sp.h>
#include <unistd.h>
#include "remap_msg_handler.hh"
#include "remap_worker.hh"
#include "../main/coordinator.hh"
#include "../../common/remap/remap_group.hh"

using namespace std;

#define POLL_ACK_TIME_INTVL	 1   // in seconds
#define WORKER_NUM			 8
#define EVENT_QUEUE_LEN		 256

CoordinatorRemapMsgHandler::CoordinatorRemapMsgHandler() :
		RemapMsgHandler() {
	this->group = ( char* ) COORD_GROUP;

	LOCK_INIT( &this->mastersLock );
	LOCK_INIT( &this->mastersAckLock );
	LOCK_INIT( &this->aliveSlavesLock );
	aliveSlaves.clear();
	this->eventQueue = new EventQueue<RemapStatusEvent>( EVENT_QUEUE_LEN );
	this->workers = new CoordinatorRemapWorker[ WORKER_NUM ];
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
	for ( int i = 0; i < WORKER_NUM; i++ ) {
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
	for ( int i = 0; i < WORKER_NUM; i++ ) {
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
			LOCK( &this->slavesStatusLock[ _ALL_SLAVES_->at(i) ] ); \
			/* no need to trigger remapping if is undefined / already entered / already exited remapping phase */ \
			if ( this->slavesStatus.count( _ALL_SLAVES_->at(i) ) == 0 ) { \
				UNLOCK( &this->slavesStatusLock[ _ALL_SLAVES_->at(i) ] ); \
				i++; \
				continue; \
			}  \
			RemapStatus status = this->slavesStatus[ _ALL_SLAVES_->at(i) ]; \
			if ( ( start && ( status == REMAP_PREPARE_START || status == REMAP_START ) ) || \
				( ( ! start ) && ( status == REMAP_PREPARE_END || status == REMAP_NONE ) ) ) \
			{ \
				UNLOCK( &this->slavesStatusLock[ _ALL_SLAVES_->at(i) ] ); \
				i++; \
				continue; \
			}  \
			/* set status for sync. and to avoid multiple start from others */ \
			this->slavesStatus[ _ALL_SLAVES_->at(i) ] = ( start )? REMAP_PREPARE_START : REMAP_PREPARE_END ; \
			/* reset ack pool anyway */\
			this->resetMasterAck( _ALL_SLAVES_->at(i) ); \
			_CHECKED_SLAVES_.push_back( _ALL_SLAVES_->at(i) ); \
			UNLOCK( &this->slavesStatusLock[ _ALL_SLAVES_->at(i) ] ); \
			_ALL_SLAVES_->erase( _ALL_SLAVES_->begin() + i ); \
		} \
		/* ask master to prepare for start */ \
		if ( this->sendStatusToMasters( _CHECKED_SLAVES_ ) == false ) { \
			/* revert the status if failed to start */ \
			for ( uint32_t i = 0; i < _CHECKED_SLAVES_.size() ; i++ ) { \
				LOCK( &this->slavesStatusLock[ _ALL_SLAVES_->at(i) ] ); \
				/* TODO is the previous status deterministic ?? */ \
				if ( start && this->slavesStatus[ _ALL_SLAVES_->at(i) ] == REMAP_PREPARE_START ) { \
					this->slavesStatus[ _ALL_SLAVES_->at(i) ] = REMAP_NONE; \
				} else if ( ( ! start ) && this->slavesStatus[ _ALL_SLAVES_->at(i) ] == REMAP_PREPARE_END ) { \
					this->slavesStatus[ _ALL_SLAVES_->at(i) ] = REMAP_START; \
				} else { \
					fprintf( stderr, "unexpected status of slave %u as %d\n",  \
						_ALL_SLAVES_->at(i).sin_addr.s_addr, this->slavesStatus[ _ALL_SLAVES_->at(i) ]  \
					); \
				} \
				UNLOCK( &this->slavesStatusLock[ _ALL_SLAVES_->at(i) ] ); \
			} \
			/* let the caller know all slaves failed */ \
			_ALL_SLAVES_->insert( _ALL_SLAVES_->end(), _CHECKED_SLAVES_.begin(), _CHECKED_SLAVES_.end() ); \
			return false; \
		} \
		/* keep retrying until success */ \
		/* TODO reset only failed ones instead */ \
		while ( ! this->insertRepeatedEvents( _EVENT_, &_CHECKED_SLAVES_ ) ); \
	} while (0) 


bool CoordinatorRemapMsgHandler::startRemap( std::vector<struct sockaddr_in> *slaves ) {
	RemapStatusEvent event;
	event.start = true;
	vector<struct sockaddr_in> slavesToStart;
	
	REMAP_PHASE_CHANGE_HANDLER( slaves, slavesToStart, event );

	return true;
}

bool CoordinatorRemapMsgHandler::startRemapEnd( const struct sockaddr_in &slave ) {
	// TODO all operation to slave get lock from coordinator, sync metadata before remapping
	bool sync = false;
	Coordinator::getInstance()->syncSlaveMeta( slave, &sync );
	// busy waiting for meta sync to complete
	fprintf( stderr, "start waiting !!\n");
	while ( sync == false );
	fprintf( stderr, "end of waiting, yeah!!\n");
	return false;
}

bool CoordinatorRemapMsgHandler::stopRemap( std::vector<struct sockaddr_in> *slaves ) {
	RemapStatusEvent event;
	event.start = false;
	vector<struct sockaddr_in> slavesToStop;

	REMAP_PHASE_CHANGE_HANDLER( slaves, slavesToStop, event );

	return true;
}

bool CoordinatorRemapMsgHandler::stopRemapEnd( const struct sockaddr_in &slave ) {
	// TODO backward migration before getting back to normal 
	return false;
}

#undef REMAP_PHASE_CHANGE_HANDLER

bool CoordinatorRemapMsgHandler::insertRepeatedEvents( RemapStatusEvent event, std::vector<struct sockaddr_in> *slaves ) {
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
 * packet: [# of slaves](1) [ [ip addr](4) [port](2) [status](1) ](7) [..](7) [..](7) ...
 */
bool CoordinatorRemapMsgHandler::sendStatusToMasters( std::vector<struct sockaddr_in> slaves ) {
	int recordSize = this->slaveStatusRecordSize;

	if ( slaves.size() == 0 ) {
		slaves = std::vector<struct sockaddr_in>( this->aliveSlaves.begin(), this->aliveSlaves.end() );
	} else if ( slaves.size() > 255 || slaves.size() * recordSize + 1 > MAX_MESSLEN ) {
		fprintf( stderr, "Too much slaves to include in message" );
		return false;
	}

	return sendStatus( slaves, MASTER_GROUP );
}

bool CoordinatorRemapMsgHandler::sendStatusToMasters( struct sockaddr_in slave ) {
	std::vector<struct sockaddr_in> slaves;
	slaves.push_back( slave );
	return sendStatusToMasters( slaves );
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
			// notify the new master about the remapping status
			myself->sendStatusToMasters();
		} else if ( ! regular && myself->isMasterLeft( service , msg, subject ) ) {
			// master left
			myself->removeAliveMaster( subject );
		} else if ( regular ){
			// ack from masters, etc.
			myself->updateStatus( sender, msg, ret );
		}

		myself->increMsgCount();
	}
	if ( ret < 0 )
		fprintf( stderr, "coord reader exits with error code %d\n", ret );

	return ( void* ) &myself->msgCount;
}

/* 
 * packet: [# of slaves](1) [ [ip addr](4) [port](2) [status](1) ](7) [..](7) [..](7) ...
 */
bool CoordinatorRemapMsgHandler::updateStatus( char *subject, char *msg, int len ) {

	// ignore messages that not from masters
	if ( strncmp( subject + 1, MASTER_PREFIX, MASTER_PREFIX_LEN ) != 0 ) {
		return false;
	}

	uint8_t slaveCount = msg[0], status = 0;
	struct sockaddr_in slave;
	int ofs = 1;
	int recordSize = this->slaveStatusRecordSize;

	LOCK( &this->mastersAckLock );
	// check slave by slave for changes
	for ( uint8_t i = 0; i < slaveCount; i++ ) {
		slave.sin_addr.s_addr = *( ( uint32_t * ) ( msg + ofs ) );
		slave.sin_port = *( ( uint16_t *) ( msg + ofs + 4 ) );
		status = msg[ ofs + 6 ];
		ofs += recordSize;
		// ignore changes for non-existing slaves or slaves in invalid status
		// TODO sync status with master with invalid status of slaves
		if ( this->slavesStatus.count( slave ) == 0 ||
			( this->slavesStatus[ slave ] != REMAP_PREPARE_START &&
			this->slavesStatus[ slave ] != REMAP_PREPARE_END )
		) {
			continue;
		}
		// check if the ack is corresponding to a correct status
		if ( ( this->slavesStatus[ slave ] == REMAP_PREPARE_START && status != REMAP_WAIT_START ) ||
			( this->slavesStatus[ slave ] == REMAP_PREPARE_END && status != REMAP_WAIT_END ) ) {
			continue;
		}

		if ( this->ackMasters.count( slave ) && aliveMasters.count( string( subject ) ) )
			ackMasters[ slave ]->insert( string( subject ) );
		else {
			char buf[ INET_ADDRSTRLEN ];
			inet_ntop( AF_INET, &slave.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
			fprintf( 
				stderr, "master [%s] or slave [%s:%hu] not found !!", 
				subject, buf , slave.sin_port
			);
		}
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
	// add the status
	LOCK_INIT( &this->slavesStatusLock[ slave ] );
	LOCK( &this->slavesStatusLock[ slave ] );
	slavesStatus[ slave ] = REMAP_NONE;
	UNLOCK( &this->slavesStatusLock [ slave ] );
	// master ack pool
	LOCK( &this->mastersAckLock );
	ackMasters[ slave ] = new std::set<std::string>();
	UNLOCK( &this->mastersAckLock );
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
	UNLOCK( &this->aliveSlavesLock );
	// add the status
	LOCK( &this->slavesStatusLock[ slave ] );
	slavesStatus.erase( slave );
	UNLOCK( &this->slavesStatusLock[ slave ] );
	this->slavesStatusLock.erase( slave );
	// master ack pool
	LOCK( &this->mastersAckLock );
	delete ackMasters[ slave ];
	ackMasters.erase( slave );
	UNLOCK( &this->mastersAckLock );

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
		fprintf( stderr, "all masters acked slave %s:%hu on %d\n", buf, slave.sin_port, this->slavesStatus[ slave ] );
	}
	//fprintf( stderr, "%lu of %lu masters acked slave %s:%hu\n", ackMasters[ slave ]->size(), aliveMasters.size(), buf , slave.sin_port );
	UNLOCK( &this->mastersAckLock );
	return allAcked;
}
