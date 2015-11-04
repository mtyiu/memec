#include <cstring>
#include <cstdlib>
#include <arpa/inet.h>
#include <sp.h>
#include <unistd.h>
#include "remap_msg_handler.hh"
#include "remap_worker.hh"
#include "../../common/remap/remap_group.hh"

using namespace std;

#define POLL_ACK_TIME_INTVL	 1   // in seconds
#define WORKER_NUM			 4

CoordinatorRemapMsgHandler::CoordinatorRemapMsgHandler() :
		RemapMsgHandler() {
	this->group = ( char* ) COORD_GROUP;
	LOCK_INIT( &this->mastersLock );
	LOCK_INIT( &this->mastersAckLock );
	slavesStatusLock.clear();
	this->eventQueue = new EventQueue<RemapStatusEvent>( WORKER_NUM * WORKER_NUM );
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

bool CoordinatorRemapMsgHandler::startRemap( std::vector<struct sockaddr_in> *slaves ) {
	RemapStatusEvent event;
	event.start = true;
	return this->insertRepeatedEvents( event, slaves );
}

bool CoordinatorRemapMsgHandler::stopRemap( std::vector<struct sockaddr_in> *slaves ) {
	RemapStatusEvent event;
	event.start = false;
	return this->insertRepeatedEvents( event, slaves );
}

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
	// assume masters name themselves "master[0-9]*"
	if ( this->isMemberLeave( service ) ) {
		if ( strncmp( subject + 1, MASTER_PREFIX , MASTER_PREFIX_LEN ) == 0 ) {
			return true;
		}
	}
	return false;
}

bool CoordinatorRemapMsgHandler::isMasterJoin( int service, char *msg, char *subject ) {
	// assume masters name themselves "master[0-9]*"
	if ( this->isMemberJoin( service ) ) {
		if ( strncmp( subject + 1, MASTER_PREFIX , MASTER_PREFIX_LEN ) == 0 ) {
			return true;
		} else {
		}
	}
	return false;
}

// TODO !!! redefine protocol here
bool CoordinatorRemapMsgHandler::sendMessageToMasters( RemapStatus to ) {
	if ( to == REMAP_UNDEFINED ) {
		to = this->status;
	}
	char buf[ MAX_MESSLEN ];
	int len = sprintf( buf, "%d\n", to );
	return ( SP_multicast ( this->mbox, MSG_TYPE, MASTER_GROUP, 0, len, buf ) > 0 );

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
			LOCK( &myself->stlock );
			// notify the new master about the remapping status
			myself->sendMessageToMasters();
			LOCK( &myself->stlock );
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

// TODO !!! redefine protocol here
bool CoordinatorRemapMsgHandler::updateStatus( char *subject, char *msg, int len ) {

	RemapStatus reply = ( RemapStatus ) atoi ( msg );

	// ignore messages that not from masters
	if ( strncmp( subject + 1, MASTER_PREFIX, MASTER_PREFIX_LEN ) != 0 ) {
		return false;
	}

	LOCK( &this->stlock );
	if ( ( this->status == REMAP_PREPARE_START && reply == REMAP_WAIT_START ) ||
			( this->status == REMAP_PREPARE_END && reply == REMAP_WAIT_END ) ) {
		// ack to preparation for starting/ending the remapping phase
		LOCK( &this->mastersLock );
		//if ( aliveMasters.count( string( subject ) ) )
		//	 ackMasters.insert( string( subject ) );
		//else
		//	fprintf( stderr, "master not found %s!!\n", subject );
		UNLOCK( &this->mastersLock );
	}
	UNLOCK( &this->stlock );

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

void CoordinatorRemapMsgHandler::addAliveSlave( struct sockaddr_in slave ) {
	LOCK( &this->mastersAckLock );
	ackMasters[ slave ] = new std::set<std::string>();
	UNLOCK( &this->mastersAckLock );
}

void CoordinatorRemapMsgHandler::removeAliveSlave( struct sockaddr_in slave ) {
	LOCK( &this->mastersAckLock );
	delete ackMasters[ slave ];
	ackMasters.erase( slave );
	UNLOCK( &this->mastersAckLock );
}

bool CoordinatorRemapMsgHandler::resetMasterAck( struct sockaddr_in slave ) {
	LOCK( &this->mastersAckLock );
	// slave does not exists
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
	LOCK( &this->mastersAckLock );
	// TODO slave is no longer accessiable
	if ( ackMasters.count( slave ) == 0 ) {
		UNLOCK( &this->mastersLock );
		return true;
	}
	allAcked = ( aliveMasters.size() == ackMasters[ slave ]->size() );
	fprintf( stderr, "%lu of %lu masters acked\n", ackMasters[ slave ]->size(), aliveMasters.size() );
	UNLOCK( &this->mastersLock );
	return allAcked;
}
