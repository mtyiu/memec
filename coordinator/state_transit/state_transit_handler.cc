#include <cstring>
#include <cstdlib>
#include <arpa/inet.h>
#include <pthread.h>
#include <sp.h>
#include <unistd.h>
#include "state_transit_handler.hh"
#include "state_transit_worker.hh"
#include "../main/coordinator.hh"
#include "../../common/state_transit/state_transit_group.hh"

using namespace std;

CoordinatorStateTransitHandler::CoordinatorStateTransitHandler() :
		StateTransitHandler() {
	this->group = ( char* ) COORD_GROUP;

	LOCK_INIT( &this->clientsLock );
	LOCK_INIT( &this->clientsAckLock );
	LOCK_INIT( &this->aliveServersLock );
	LOCK_INIT( &this->failedServersLock );
	LOCK_INIT( &this->updatedServersLock );
	aliveServers.clear();

	Coordinator* coordinator = Coordinator::getInstance();
	this->eventQueue = new EventQueue<StateTransitEvent>( coordinator->config.global.states.queue );
	this->workers = new CoordinatorStateTransitWorker[ coordinator->config.global.states.workers ];
}

CoordinatorStateTransitHandler::~CoordinatorStateTransitHandler() {
	delete this->eventQueue;
	delete [] this->workers;
}

bool CoordinatorStateTransitHandler::init( const int ip, const int port, const char *user ) {
	char addrbuf[ 32 ], ipstr[ INET_ADDRSTRLEN ];
	struct in_addr addr;
	memset( addrbuf, 0, 32 );
	addr.s_addr = ip;
	inet_ntop( AF_INET, &addr, ipstr, INET_ADDRSTRLEN );
	sprintf( addrbuf, "%u@%s", ntohs( port ), ipstr );
	StateTransitHandler::init( addrbuf , user );
	pthread_mutex_init( &this->ackSignalLock, 0 );

	this->isListening = false;

	return ( SP_join( this->mbox, CLIENT_GROUP ) == 0 );
}

void CoordinatorStateTransitHandler::quit() {
	SP_leave( mbox, COORD_GROUP );
	StateTransitHandler::quit();
	if ( reader > 0 ) {
		pthread_join( this->reader, NULL );
		reader = -1;
	}
}

bool CoordinatorStateTransitHandler::start() {
	if ( ! this->isConnected )
		return false;
	// start event queue processing
	this->eventQueue->start();
	// read messages using a background thread
	if ( pthread_create( &this->reader, NULL, CoordinatorStateTransitHandler::readMessages, this ) < 0 ) {
		fprintf( stderr, "Coordinator FAILED to start reading messages" );
		return false;
	}
	this->isListening = true;
	// start all workers
	uint32_t workers = Coordinator::getInstance()->config.global.states.workers;
	for ( uint32_t i = 0; i < workers; i++ ) {
		this->workers[i].start();
	}
	return true;
}

bool CoordinatorStateTransitHandler::stop() {
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
	uint32_t workers = Coordinator::getInstance()->config.global.states.workers;
	for ( uint32_t i = 0; i < workers; i++ ) {
		this->workers[i].stop();
	}
	return ( ret == 0 );
}

void CoordinatorStateTransitHandler::stateChangeInitHandler( std::vector<struct sockaddr_in> *allServers, std::vector<struct sockaddr_in> &checkedServers, StateTransitEvent event ) {
	bool start = event.start;
	struct sockaddr_in server;

	checkedServers.clear();

	for ( uint32_t i = 0; i < allServers->size(); ) {
		server = allServers->at(i);
		LOCK( &this->serversStateLock[ server ] );
		RemapState state = this->serversState[ server ]; \
		// no need to trigger transition if is undefined / already entered / already exited degraded state
		if ( state == STATE_UNDEFINED || ( start && state != STATE_NORMAL ) ||
				( ( ! start ) && state != STATE_DEGRADED ) ) {
			UNLOCK( &this->serversStateLock[ server ] );
			i++;
			continue;
		}
		// set state for sync. and to avoid multiple start from others
		this->serversState[ server ] = ( start ) ? STATE_INTERMEDIATE : STATE_COORDINATED;
		// reset ack pool anyway
		this->resetClientAck( server );
		checkedServers.push_back( server );
		UNLOCK( &this->serversStateLock[ server ] );
		// remove this server from list of servers to check
		allServers->erase( allServers->begin() + i );
	}
	// ask client to change state
	if ( this->broadcastState( checkedServers ) == false ) {
		// revert the state if failed to start
		for ( uint32_t i = 0; i < checkedServers.size(); i++ ) {
			server = allServers->at(i);
			LOCK( &this->serversStateLock[ server ] );
			if ( start && this->serversState[ server ] == STATE_INTERMEDIATE ) {
				this->serversState[ server ] = STATE_NORMAL;
			} else if ( ( ! start ) && this->serversState[ server ] == STATE_COORDINATED ) {
				this->serversState[ server ] = STATE_DEGRADED;
			} else {
				printServerState(
					this->serversState[ server ], ( char* ) "CoordinatorStateTransitHandler",
					( char* ) "phaseChangeHandler(unexpect revert state)"
				);
			}
			UNLOCK( &this->serversStateLock[ server ] );
		}
		allServers->insert( allServers->end(), checkedServers.begin(), checkedServers.end() );
	}
	// keep retrying until success
	// TODO reset only failed ones instead
	while ( ! this->insertRepeatedEvents( event, &checkedServers ) );
}


bool CoordinatorStateTransitHandler::transitToDegraded( std::vector<struct sockaddr_in> *servers, bool forced ) {
	StateTransitEvent event;
	event.start = true;
	vector<struct sockaddr_in> serversToTransit;

	this->stateChangeInitHandler( servers, serversToTransit, event );
	//if ( forced ) {
	//	for ( uint32_t i = 0, len = servers->size(); i < len; i++ ) {
	//		this->addFailedServer( servers->at( i ) );
	//		LOCK( &this->serversStateLock[ servers->at( i ) ] );
	//		this->serversState[ servers->at( i ) ] = STATE_INTERMEDIATE;
	//		UNLOCK( &this->serversStateLock[ servers->at( i ) ] );
	//	}
	//	this->broadcastState( *servers );
	//	this->insertRepeatedEvents( event, servers );
	//} else {
	//	//STATE_TRANSIT_PHASE_CHANGE_HANDLER( servers, serversToTransit, event );
	//	this->stateChangeInitHandler( servers, serversToTransit, event );
	//}

	return true;
}

bool CoordinatorStateTransitHandler::transitToDegradedEnd( const struct sockaddr_in &server ) {
	// all operation to server get lock from coordinator
	Coordinator *coordinator = Coordinator::getInstance();
	LOCK_T *lock = &coordinator->sockets.servers.lock;
	std::vector<ServerSocket *> &servers = coordinator->sockets.servers.values;
	ServerSocket *target = 0;

	LOCK( lock );
	for ( size_t i = 0, size = servers.size(); i < size; i++ ) {
		if ( servers[ i ]->equal( server.sin_addr.s_addr, server.sin_port ) ) {
			target = servers[ i ];
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
		fprintf( stderr, "Server not found.\n" );
	}

	LOCK( &this->aliveServersLock );
	if ( this->crashedServers.find( server ) != this->crashedServers.end() ) {
		printf( "Triggering reconstruction for crashed server...\n" );
		ServerEvent event;
		event.triggerReconstruction( server );
		coordinator->eventQueue.insert( event );
	}
	UNLOCK( &this->aliveServersLock );

	return true;
}

bool CoordinatorStateTransitHandler::transitToNormal( std::vector<struct sockaddr_in> *servers, bool forced ) {
	StateTransitEvent event;
	event.start = false;
	vector<struct sockaddr_in> serversToTransit;

	LOCK( &this->aliveServersLock );

	// Never transit to normal state if it is crashed
	for ( auto it = servers->begin(); it != servers->end(); ) {
		if ( this->crashedServers.count( *it ) > 0 ) {
			it = servers->erase( it );
		} else {
			it++;
		}
	}
	UNLOCK( &this->aliveServersLock );

	this->stateChangeInitHandler( servers, serversToTransit, event );
	//if ( forced ) {
	//	for ( uint32_t i = 0, len = servers->size(); i < len; i++ ) {
	//		this->addFailedServer( servers->at( i ) );
	//		LOCK( &this->serversStateLock[ servers->at(i) ] );
	//		this->serversState[ servers->at( i ) ] = STATE_COORDINATED;
	//		UNLOCK( &this->serversStateLock[ servers->at(i) ] );
	//	}
	//	this->broadcastState( *servers );
	//	this->insertRepeatedEvents( event, servers );
	//} else {
	//	//STATE_TRANSIT_PHASE_CHANGE_HANDLER( servers, serversToTransit, event );
	//	this->stateChangeInitHandler( servers, serversToTransit, event );
	//}

	return true;
}

bool CoordinatorStateTransitHandler::transitToNormalEnd( const struct sockaddr_in &server ) {
	// backward migration before getting back to normal
	Coordinator *coordinator = Coordinator::getInstance();

	pthread_mutex_t lock;
	pthread_cond_t cond;
	// try to avoid the state "while( ! done )" being optmized to "while( true )"
	volatile bool done;

	pthread_mutex_init( &lock, 0 );
	pthread_cond_init( &cond, 0 );

	// STATE_TRANSIT SET
	done = false;
	pthread_mutex_lock( &lock );
	coordinator->syncRemappedData( server, &lock, &cond, ( bool * ) &done );
	while( ! done )
		pthread_cond_wait( &cond, &lock );
	pthread_mutex_unlock( &lock );

	size_t original = coordinator->remappingRecords.size();
	size_t count = coordinator->remappingRecords.erase( server );

	__INFO__( YELLOW, "CoordinatorStateTransitHandler", "transitToNormalEnd", "Erased %lu remapping records (original = %lu, remaining = %lu).", count, original, coordinator->remappingRecords.size() );

	// DEGRADED
	done = false;
	pthread_mutex_lock( &lock );
	coordinator->releaseDegradedLock( server, &lock, &cond, ( bool * ) &done );
	while( ! done )
		pthread_cond_wait( &cond, &lock );
	pthread_mutex_unlock( &lock );

	return true;
}

#undef STATE_TRANSIT_PHASE_CHANGE_HANDLER

bool CoordinatorStateTransitHandler::insertRepeatedEvents( StateTransitEvent event, std::vector<struct sockaddr_in> *servers ) {
	bool ret = true;
	uint32_t i = 0;
	for ( i = 0; i < servers->size(); i++ ) {
		event.server = servers->at(i);
		ret = this->eventQueue->insert( event );
		if ( ! ret )
			break;
	}
	// notify the caller if any server cannot perform state transition
	if ( ret )
		servers->clear();
	else
		servers->erase( servers->begin(), servers->begin()+i );
	return ret;
}

bool CoordinatorStateTransitHandler::isClientJoin( int service, char *msg, char *subject ) {
	// assume clients name themselves "[PREFIX][0-9]*"
	return ( this->isMemberJoin( service ) && strncmp( subject + 1, CLIENT_PREFIX , CLIENT_PREFIX_LEN ) == 0 );
}

bool CoordinatorStateTransitHandler::isClientLeft( int service, char *msg, char *subject ) {
	// assume clients name themselves "[PREFIX][0-9]*"
	return ( this->isMemberLeave( service ) && strncmp( subject + 1, CLIENT_PREFIX , CLIENT_PREFIX_LEN ) == 0 );
}

bool CoordinatorStateTransitHandler::isServerJoin( int service, char *msg, char *subject ) {
	// assume clients name themselves "[PREFIX][0-9]*"
	return ( this->isMemberJoin( service ) && strncmp( subject + 1, SERVER_PREFIX , SERVER_PREFIX_LEN ) == 0 );
}
bool CoordinatorStateTransitHandler::isServerLeft( int service, char *msg, char *subject ) {
	// assume clients name themselves "[PREFIX][0-9]*"
	return ( this->isMemberLeave( service ) && strncmp( subject + 1, SERVER_PREFIX , SERVER_PREFIX_LEN ) == 0 );
}

/*
 * packet: [# of servers](1) [ [ip addr](4) [port](2) [state](1) ](7) [..](7) [..](7) ...
 */
int CoordinatorStateTransitHandler::sendStateToClients( std::vector<struct sockaddr_in> servers ) {
	char group[ 1 ][ MAX_GROUP_NAME ];
	int recordSize = this->serverStateRecordSize;

	if ( servers.size() == 0 ) {
		servers = std::vector<struct sockaddr_in>( this->aliveServers.begin(), this->aliveServers.end() );
	} else if ( servers.size() > 255 || servers.size() * recordSize + 1 > MAX_MESSLEN ) {
		fprintf( stderr, "Too much servers to include in message" );
		return false;
	}

	strcpy( group[ 0 ], CLIENT_GROUP );
	return sendState( servers, 1, group );
}

int CoordinatorStateTransitHandler::sendStateToClients( struct sockaddr_in server ) {
	std::vector<struct sockaddr_in> servers;
	servers.push_back( server );
	return sendStateToClients( servers );
}

int CoordinatorStateTransitHandler::broadcastState( std::vector<struct sockaddr_in> servers ) {
	char groups[ MAX_GROUP_NUM ][ MAX_GROUP_NAME ];
	int recordSize = this->serverStateRecordSize;
	if ( servers.size() == 0 ) {
		servers = std::vector<struct sockaddr_in>( this->aliveServers.begin(), this->aliveServers.end() );
	} else if ( servers.size() > 255 || servers.size() * recordSize + 1 > MAX_MESSLEN ) {
		fprintf( stderr, "Too much servers to include in message" );
		return false;
	}
	// send to clients and servers
	strcpy( groups[ 0 ], CLIENT_GROUP );
	strcpy( groups[ 1 ], SERVER_GROUP );
	return sendState( servers, 2, groups );
}

int CoordinatorStateTransitHandler::broadcastState( struct sockaddr_in server ) {
	std::vector<struct sockaddr_in> servers;
	servers.push_back( server );
	return broadcastState( servers );
}

void *CoordinatorStateTransitHandler::readMessages( void *argv ) {
	int ret = 0;

	int service, groups, endian;
	int16 msgType;
	char sender[ MAX_GROUP_NAME ], msg[ MAX_MESSLEN ];
	char targetGroups[ MAX_GROUP_NUM ][ MAX_GROUP_NAME ];
	char* subject;

	bool regular = false, fromClient = false;

	CoordinatorStateTransitHandler *myself = ( CoordinatorStateTransitHandler* ) argv;

	while( myself->isListening ) {
		ret = SP_receive( myself->mbox, &service, sender, MAX_GROUP_NUM, &groups, targetGroups, &msgType, &endian, MAX_MESSLEN, msg );

		subject = &msg[ SP_get_vs_set_offset_memb_mess() ];
		regular = myself->isRegularMessage( service );
		fromClient = ( strncmp( sender, CLIENT_GROUP, CLIENT_GROUP_LEN ) == 0 );

		if ( ret < 0 ) {
			__ERROR__( "CoordinatorStateTransitHandler", "readMessage", "Failed to receive messages %d\n", ret );
		} else if ( ! regular ) {
			std::vector<struct sockaddr_in> servers;
			if ( fromClient && myself->isClientJoin( service , msg, subject ) ) {
				// client joined ( clients group )
				myself->addAliveClient( subject );
				// notify the new client about the states
				if ( ( ret = myself->sendStateToClients( servers ) ) < 0 )
					__ERROR__( "CoordinatorStateTransitHandler", "readMessages", "Failed to broadcast states to clients %d", ret );
			} else if ( myself->isClientLeft( service, msg, subject ) ) {
				// client left
				myself->removeAliveClient( subject );
			} else if ( myself->isServerJoin( service, msg, subject ) ) {
				// server join
				if ( ( ret = myself->broadcastState( servers ) ) < 0 )
					__ERROR__( "CoordinatorStateTransitHandler", "readMessages", "Failed to broadcast states to clients %d", ret );
			} else if ( myself->isServerLeft( service, msg, subject ) ) {
				// server left
				// TODO: change state ?
			} else {
				// ignored
			}
		} else {
			// ack from clients, etc.
			myself->updateState( sender, msg, ret );
		}

		myself->increMsgCount();
	}

	return ( void* ) &myself->msgCount;
}

/*
 * packet: [# of servers](1) [ [ip addr](4) [port](2) [state](1) ](7) [..](7) [..](7) ...
 */
bool CoordinatorStateTransitHandler::updateState( char *subject, char *msg, int len ) {

	// ignore messages that not from clients
	if ( strncmp( subject + 1, CLIENT_PREFIX, CLIENT_PREFIX_LEN ) != 0 ) {
		return false;
	}

	uint8_t serverCount = msg[0], state = 0;
	struct sockaddr_in server;
	int ofs = 1;
	int recordSize = this->serverStateRecordSize;

	LOCK( &this->clientsAckLock );
	// check server by server for changes
	for ( uint8_t i = 0; i < serverCount; i++ ) {
		server.sin_addr.s_addr = *( ( uint32_t * ) ( msg + ofs ) );
		server.sin_port = *( ( uint16_t *) ( msg + ofs + 4 ) );
		state = msg[ ofs + 6 ];
		ofs += recordSize;
		// ignore changes for non-existing servers or servers in invalid state
		// TODO sync state with client with invalid state of servers
		if ( this->serversState.count( server ) == 0 ||
			( this->serversState[ server ] != STATE_INTERMEDIATE &&
			this->serversState[ server ] != STATE_COORDINATED )
		) {
			continue;
		}
		// check if the ack is corresponding to a correct state
		if ( ( this->serversState[ server ] == STATE_INTERMEDIATE && state != STATE_WAIT_DEGRADED ) ||
			( this->serversState[ server ] == STATE_COORDINATED && state != STATE_WAIT_NORMAL ) ) {
			continue;
		}

		if ( this->ackClients.count( server ) && aliveClients.count( string( subject ) ) )
			ackClients[ server ]->insert( string( subject ) );
		else {
			char buf[ INET_ADDRSTRLEN ];
			inet_ntop( AF_INET, &server.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
			fprintf(
				stderr, "client [%s] or server [%s:%hu] not found !!",
				subject, buf, ntohs( server.sin_port )
			);
		}
		// check if all client acked
		UNLOCK( &this->clientsAckLock );
		this->isAllClientAcked( server );
		LOCK( &this->clientsAckLock );
	}
	UNLOCK( &this->clientsAckLock );

	return true;
}

void CoordinatorStateTransitHandler::addAliveClient( char *name ) {
	LOCK( &this->clientsLock );
	aliveClients.insert( string( name ) );
	UNLOCK( &this->clientsLock );
}

void CoordinatorStateTransitHandler::removeAliveClient( char *name ) {
	LOCK( &this->clientsLock );
	aliveClients.erase( string( name ) );
	UNLOCK( &this->clientsLock );
	// remove the client from all alive servers ack. pool
	LOCK( &this->clientsAckLock );
	for( auto it : ackClients ) {
		it.second->erase( name );
	}
	UNLOCK( &this->clientsAckLock );
}

bool CoordinatorStateTransitHandler::addAliveServer( struct sockaddr_in server ) {
	// alive servers list
	LOCK( &this->aliveServersLock );
	if ( this->aliveServers.count( server ) > 0 ) {
		UNLOCK( &this->aliveServersLock );
		return false;
	}
	aliveServers.insert( server );
	UNLOCK( &this->aliveServersLock );
	// add the state
	LOCK_INIT( &this->serversStateLock[ server ] );
	LOCK( &this->serversStateLock[ server ] );
	serversState[ server ] = STATE_NORMAL;
	UNLOCK( &this->serversStateLock [ server ] );
	// client ack pool
	LOCK( &this->clientsAckLock );
	ackClients[ server ] = new std::set<std::string>();
	UNLOCK( &this->clientsAckLock );
	// waiting server
	pthread_cond_init( &this->ackSignal[ server ], NULL );
	return true;
}

bool CoordinatorStateTransitHandler::addCrashedServer( struct sockaddr_in server ) {
	LOCK( &this->aliveServersLock );
	if ( this->crashedServers.count( server ) > 0 ) {
		UNLOCK( &this->aliveServersLock );
		return false;
	}
	crashedServers.insert( server );
	UNLOCK( &this->aliveServersLock );
	return true;
}

uint32_t CoordinatorStateTransitHandler::addFailedServer( struct sockaddr_in server ) {
	LOCK( &this->failedServersLock );
	this->failedServers.insert( server );
	return getFailedServerCount( false, true );
}

uint32_t CoordinatorStateTransitHandler::removeFailedServer( struct sockaddr_in server ) {
	LOCK( &this->failedServersLock );
	if ( this->failedServers.erase( server ) ) {
		LOCK ( &this->updatedServersLock );
		this->updatedServers.push_back( server );
		UNLOCK ( &this->updatedServersLock );
	}
	return getFailedServerCount( false, true );
}

uint32_t CoordinatorStateTransitHandler::getFailedServerCount( bool needsLock, bool needsUnlock ) {
	if ( needsLock ) LOCK( &this->failedServersLock );
	uint32_t count = this->failedServers.size();
	if ( needsUnlock ) UNLOCK( &this->failedServersLock );
	return count;
}

uint32_t CoordinatorStateTransitHandler::eraseUpdatedServers( std::vector<struct sockaddr_in> * ret ) {
	LOCK ( &this->updatedServersLock );
	if ( ret ) {
		std::swap( this->updatedServers, *ret );
	}
	this->updatedServers.clear();
	UNLOCK ( &this->updatedServersLock );
	return ( ret ? ret->size() : 0 );
}

bool CoordinatorStateTransitHandler::removeAliveServer( struct sockaddr_in server ) {
	// alive servers list
	LOCK( &this->aliveServersLock );
	if ( this->aliveServers.count( server ) == 0 ) {
		UNLOCK( &this->aliveServersLock );
		return false;
	}
	aliveServers.erase( server );
	crashedServers.erase( server );
	UNLOCK( &this->aliveServersLock );

	// add the state
	LOCK( &this->serversStateLock[ server ] );
	serversState.erase( server );
	UNLOCK( &this->serversStateLock[ server ] );
	this->serversStateLock.erase( server );
	// client ack pool
	LOCK( &this->clientsAckLock );
	delete ackClients[ server ];
	ackClients.erase( server );
	UNLOCK( &this->clientsAckLock );
	// waiting server
	pthread_cond_broadcast( &this->ackSignal[ server ] );
	return true;
}

bool CoordinatorStateTransitHandler::resetClientAck( struct sockaddr_in server ) {
	LOCK( &this->clientsAckLock );
	// abort reset if server does not exists
	if ( ackClients.count( server ) == 0 ) {
		UNLOCK( &this->clientsLock );
		return false;
	}
	ackClients[ server ]->clear();
	UNLOCK( &this->clientsAckLock );
	return true;
}

bool CoordinatorStateTransitHandler::isAllClientAcked( struct sockaddr_in server ) {
	bool allAcked = false;
	char buf[ INET_ADDRSTRLEN ];
	inet_ntop( AF_INET, &server.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
	LOCK( &this->clientsAckLock );
	// TODO abort checking if server is no longer accessiable
	if ( ackClients.count( server ) == 0 ) {
		UNLOCK( &this->clientsAckLock );
		return true;
	}
	allAcked = ( aliveClients.size() == ackClients[ server ]->size() );
	if ( allAcked ) {
		pthread_cond_broadcast( &this->ackSignal[ server ] );
	}
	UNLOCK( &this->clientsAckLock );
	return allAcked;
}

bool CoordinatorStateTransitHandler::isInTransition( const struct sockaddr_in &server ) {
	bool ret;
	LOCK( &this->serversStateLock[ server ] );
	ret = ( serversState[ server ] == STATE_INTERMEDIATE ) || ( serversState[ server ] == STATE_COORDINATED );
	UNLOCK( &this->serversStateLock[ server ] );
	return ret;
}

bool CoordinatorStateTransitHandler::allowRemapping( const struct sockaddr_in &server ) {
	bool ret;
	LOCK( &this->serversStateLock[ server ] );
	ret = ( serversState[ server ] == STATE_INTERMEDIATE ) || ( serversState[ server ] == STATE_DEGRADED );
	UNLOCK( &this->serversStateLock[ server ] );
	return ret;
}

bool CoordinatorStateTransitHandler::reachMaximumRemapped( uint32_t maximum ) {
	uint32_t count = 0;
	LOCK( &this->aliveServersLock );
	for ( auto it = this->aliveServers.begin(); it != this->aliveServers.end(); it++ ) {
		const struct sockaddr_in &server = ( *it );
		// LOCK( &this->serversStateLock[ server ] );
		if ( serversState[ server ] != STATE_NORMAL ) {
			count++;
		}
		// UNLOCK( &this->serversStateLock[ server ] );

		if ( count == maximum )
			break;
	}
	UNLOCK( &this->aliveServersLock );
	return ( count == maximum );
}
