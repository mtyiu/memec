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

	LOCK_INIT( &this->clientsLock );
	LOCK_INIT( &this->clientsAckLock );
	LOCK_INIT( &this->aliveServersLock );
	aliveServers.clear();

	Coordinator* coordinator = Coordinator::getInstance();
	this->eventQueue = new EventQueue<RemapStateEvent>( coordinator->config.global.states.queue );
	this->workers = new CoordinatorRemapWorker[ coordinator->config.global.states.workers ];
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
	return ( SP_join( this->mbox, CLIENT_GROUP ) == 0 );
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
	uint32_t workers = Coordinator::getInstance()->config.global.states.workers;
	for ( uint32_t i = 0; i < workers; i++ ) {
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
	uint32_t workers = Coordinator::getInstance()->config.global.states.workers;
	for ( uint32_t i = 0; i < workers; i++ ) {
		this->workers[i].stop();
	}
	return ( ret == 0 );
}

#define REMAP_PHASE_CHANGE_HANDLER( _ALL_SERVERS_, _CHECKED_SERVERS_, _EVENT_ ) \
	do { \
		_CHECKED_SERVERS_.clear(); \
		\
		bool start = _EVENT_.start; \
		for ( uint32_t i = 0; i < _ALL_SERVERS_->size(); ) { \
			LOCK( &this->serversStateLock[ _ALL_SERVERS_->at(i) ] ); \
			/* check if server is alive  \
			if ( this->serversState.count( _ALL_SERVERS_->at(i) ) == 0 ) { \
				UNLOCK( &this->serversStateLock[ _ALL_SERVERS_->at(i) ] ); \
				i++; \
				continue; \
			} */  \
			RemapState state = this->serversState[ _ALL_SERVERS_->at(i) ]; \
			/* no need to trigger remapping if is undefined / already entered / already exited remapping phase */ \
			if ( ( state == REMAP_UNDEFINED ) || ( start && state != REMAP_NORMAL ) || \
				( ( ! start ) && state != REMAP_DEGRADED ) ) \
			{ \
				UNLOCK( &this->serversStateLock[ _ALL_SERVERS_->at(i) ] ); \
				i++; \
				continue; \
			}  \
			/* set state for sync. and to avoid multiple start from others */ \
			this->serversState[ _ALL_SERVERS_->at(i) ] = ( start ) ? REMAP_INTERMEDIATE : REMAP_COORDINATED; \
			/* reset ack pool anyway */\
			this->resetClientAck( _ALL_SERVERS_->at(i) ); \
			_CHECKED_SERVERS_.push_back( _ALL_SERVERS_->at(i) ); \
			UNLOCK( &this->serversStateLock[ _ALL_SERVERS_->at(i) ] ); \
			_ALL_SERVERS_->erase( _ALL_SERVERS_->begin() + i ); \
		} \
		/* ask client to change state */ \
		if ( this->broadcastState( _CHECKED_SERVERS_ ) == false ) { \
			/* revert the state if failed to start */ \
			for ( uint32_t i = 0; i < _CHECKED_SERVERS_.size() ; i++ ) { \
				LOCK( &this->serversStateLock[ _ALL_SERVERS_->at(i) ] ); \
				/* TODO is the previous state deterministic ?? */ \
				if ( start && this->serversState[ _ALL_SERVERS_->at(i) ] == REMAP_INTERMEDIATE ) { \
					this->serversState[ _ALL_SERVERS_->at(i) ] = REMAP_NORMAL; \
				} else if ( ( ! start ) && this->serversState[ _ALL_SERVERS_->at(i) ] == REMAP_COORDINATED ) { \
					this->serversState[ _ALL_SERVERS_->at(i) ] = REMAP_DEGRADED; \
				} else { \
					fprintf( stderr, "unexpected state of server %u as %d\n",  \
						_ALL_SERVERS_->at(i).sin_addr.s_addr, this->serversState[ _ALL_SERVERS_->at(i) ]  \
					); \
				} \
				UNLOCK( &this->serversStateLock[ _ALL_SERVERS_->at(i) ] ); \
			} \
			/* let the caller know all servers failed */ \
			_ALL_SERVERS_->insert( _ALL_SERVERS_->end(), _CHECKED_SERVERS_.begin(), _CHECKED_SERVERS_.end() ); \
			return false; \
		} \
		/* keep retrying until success */ \
		/* TODO reset only failed ones instead */ \
		while ( ! this->insertRepeatedEvents( _EVENT_, &_CHECKED_SERVERS_ ) ); \
	} while (0)


bool CoordinatorRemapMsgHandler::transitToDegraded( std::vector<struct sockaddr_in> *servers, bool forced ) {
	RemapStateEvent event;
	event.start = true;
	vector<struct sockaddr_in> serversToStart;

	if ( forced ) {
		for ( uint32_t i = 0, len = servers->size(); i < len; i++ ) {
			LOCK( &this->serversStateLock[ servers->at(i) ] );
			this->serversState[ servers->at( i ) ] = REMAP_INTERMEDIATE;
			UNLOCK( &this->serversStateLock[ servers->at(i) ] );
		}
		this->broadcastState( *servers );
		this->insertRepeatedEvents( event, servers );
	} else {
		REMAP_PHASE_CHANGE_HANDLER( servers, serversToStart, event );
	}

	return true;
}

bool CoordinatorRemapMsgHandler::transitToDegradedEnd( const struct sockaddr_in &server ) {
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

bool CoordinatorRemapMsgHandler::transitToNormal( std::vector<struct sockaddr_in> *servers, bool forced ) {
	RemapStateEvent event;
	event.start = false;
	vector<struct sockaddr_in> serversToStop;

	LOCK( &this->aliveServersLock );
	for ( auto it = servers->begin(); it != servers->end(); ) {
		if ( this->crashedServers.count( *it ) > 0 ) {
			// Never transit to normal state if it is crashed
			it = servers->erase( it );
		} else {
			it++;
		}
	}
	UNLOCK( &this->aliveServersLock );

	if ( forced ) {
		for ( uint32_t i = 0, len = servers->size(); i < len; i++ ) {
			LOCK( &this->serversStateLock[ servers->at(i) ] );
			this->serversState[ servers->at( i ) ] = REMAP_COORDINATED;
			UNLOCK( &this->serversStateLock[ servers->at(i) ] );
		}
		this->broadcastState( *servers );
		this->insertRepeatedEvents( event, servers );
	} else {
		REMAP_PHASE_CHANGE_HANDLER( servers, serversToStop, event );
	}

	return true;
}

bool CoordinatorRemapMsgHandler::transitToNormalEnd( const struct sockaddr_in &server ) {
	// backward migration before getting back to normal
	Coordinator *coordinator = Coordinator::getInstance();

	pthread_mutex_t lock;
	pthread_cond_t cond;
	bool done;

	pthread_mutex_init( &lock, 0 );
	pthread_cond_init( &cond, 0 );

	// REMAP SET
	done = false;
	coordinator->syncRemappedData( server, &lock, &cond, &done );

	pthread_mutex_lock( &lock );
	while( ! done )
		pthread_cond_wait( &cond, &lock );
	pthread_mutex_unlock( &lock );

	size_t original = coordinator->remappingRecords.size();
	size_t count = coordinator->remappingRecords.erase( server );
	printf( "Erased %lu remapping records (original = %lu, remaining = %lu).\n", count, original, coordinator->remappingRecords.size() );

	// DEGRADED
	done = false;
	coordinator->releaseDegradedLock( server, &lock, &cond, &done );

	pthread_mutex_lock( &lock );
	while( ! done )
		pthread_cond_wait( &cond, &lock );
	pthread_mutex_unlock( &lock );

	return true;
}

#undef REMAP_PHASE_CHANGE_HANDLER

bool CoordinatorRemapMsgHandler::insertRepeatedEvents( RemapStateEvent event, std::vector<struct sockaddr_in> *servers ) {
	bool ret = true;
	uint32_t i = 0;
	for ( i = 0; i < servers->size(); i++ ) {
		event.server = servers->at(i);
		ret = this->eventQueue->insert( event );
		if ( ! ret )
			break;
	}
	// notify the caller if any server cannot start remapping
	if ( ret )
		servers->clear();
	else
		servers->erase( servers->begin(), servers->begin()+i );
	return ret;
}

bool CoordinatorRemapMsgHandler::isClientJoin( int service, char *msg, char *subject ) {
	// assume clients name themselves "[PREFIX][0-9]*"
	return ( this->isMemberJoin( service ) && strncmp( subject + 1, CLIENT_PREFIX , CLIENT_PREFIX_LEN ) == 0 );
}

bool CoordinatorRemapMsgHandler::isClientLeft( int service, char *msg, char *subject ) {
	// assume clients name themselves "[PREFIX][0-9]*"
	return ( this->isMemberLeave( service ) && strncmp( subject + 1, CLIENT_PREFIX , CLIENT_PREFIX_LEN ) == 0 );
}

bool CoordinatorRemapMsgHandler::isServerJoin( int service, char *msg, char *subject ) {
	// assume clients name themselves "[PREFIX][0-9]*"
	return ( this->isMemberJoin( service ) && strncmp( subject + 1, SERVER_PREFIX , SERVER_PREFIX_LEN ) == 0 );
}
bool CoordinatorRemapMsgHandler::isServerLeft( int service, char *msg, char *subject ) {
	// assume clients name themselves "[PREFIX][0-9]*"
	return ( this->isMemberLeave( service ) && strncmp( subject + 1, SERVER_PREFIX , SERVER_PREFIX_LEN ) == 0 );
}

/*
 * packet: [# of servers](1) [ [ip addr](4) [port](2) [state](1) ](7) [..](7) [..](7) ...
 */
int CoordinatorRemapMsgHandler::sendStateToClients( std::vector<struct sockaddr_in> servers ) {
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

int CoordinatorRemapMsgHandler::sendStateToClients( struct sockaddr_in server ) {
	std::vector<struct sockaddr_in> servers;
	servers.push_back( server );
	return sendStateToClients( servers );
}

int CoordinatorRemapMsgHandler::broadcastState( std::vector<struct sockaddr_in> servers ) {
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

int CoordinatorRemapMsgHandler::broadcastState( struct sockaddr_in server ) {
	std::vector<struct sockaddr_in> servers;
	servers.push_back( server );
	return broadcastState( servers );
}

void *CoordinatorRemapMsgHandler::readMessages( void *argv ) {
	int ret = 0;

	int service, groups, endian;
	int16 msgType;
	char sender[ MAX_GROUP_NAME ], msg[ MAX_MESSLEN ];
	char targetGroups[ MAX_GROUP_NUM ][ MAX_GROUP_NAME ];
	char* subject;

	bool regular = false, fromClient = false;

	CoordinatorRemapMsgHandler *myself = ( CoordinatorRemapMsgHandler* ) argv;

	while( myself->isListening ) {
		ret = SP_receive( myself->mbox, &service, sender, MAX_GROUP_NUM, &groups, targetGroups, &msgType, &endian, MAX_MESSLEN, msg );

		subject = &msg[ SP_get_vs_set_offset_memb_mess() ];
		regular = myself->isRegularMessage( service );
		fromClient = ( strncmp( sender, CLIENT_GROUP, CLIENT_GROUP_LEN ) == 0 );

		if ( ret < 0 ) {
			__ERROR__( "CoordinatorRemapMsgHandler", "readMessage", "Failed to receive messages %d\n", ret );
		} else if ( ! regular ) {
			std::vector<struct sockaddr_in> servers;
			if ( fromClient && myself->isClientJoin( service , msg, subject ) ) {
				// client joined ( clients group )
				myself->addAliveClient( subject );
				// notify the new client about the remapping state
				if ( ( ret = myself->sendStateToClients( servers ) ) < 0 )
					__ERROR__( "CoordinatorRemapMsgHandler", "readMessages", "Failed to broadcast states to clients %d", ret );
			} else if ( myself->isClientLeft( service, msg, subject ) ) {
				// client left
				myself->removeAliveClient( subject );
			} else if ( myself->isServerJoin( service, msg, subject ) ) {
				// server join
				if ( ( ret = myself->broadcastState( servers ) ) < 0 )
					__ERROR__( "CoordinatorRemapMsgHandler", "readMessages", "Failed to broadcast states to clients %d", ret );
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
bool CoordinatorRemapMsgHandler::updateState( char *subject, char *msg, int len ) {

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
			( this->serversState[ server ] != REMAP_INTERMEDIATE &&
			this->serversState[ server ] != REMAP_COORDINATED )
		) {
			continue;
		}
		// check if the ack is corresponding to a correct state
		if ( ( this->serversState[ server ] == REMAP_INTERMEDIATE && state != REMAP_WAIT_DEGRADED ) ||
			( this->serversState[ server ] == REMAP_COORDINATED && state != REMAP_WAIT_NORMAL ) ) {
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

void CoordinatorRemapMsgHandler::addAliveClient( char *name ) {
	LOCK( &this->clientsLock );
	aliveClients.insert( string( name ) );
	UNLOCK( &this->clientsLock );
}

void CoordinatorRemapMsgHandler::removeAliveClient( char *name ) {
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

bool CoordinatorRemapMsgHandler::addAliveServer( struct sockaddr_in server ) {
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
	serversState[ server ] = REMAP_NORMAL;
	UNLOCK( &this->serversStateLock [ server ] );
	// client ack pool
	LOCK( &this->clientsAckLock );
	ackClients[ server ] = new std::set<std::string>();
	UNLOCK( &this->clientsAckLock );
	// waiting server
	pthread_cond_init( &this->ackSignal[ server ], NULL );
	return true;
}

bool CoordinatorRemapMsgHandler::addCrashedServer( struct sockaddr_in server ) {
	LOCK( &this->aliveServersLock );
	if ( this->crashedServers.count( server ) > 0 ) {
		UNLOCK( &this->aliveServersLock );
		return false;
	}
	crashedServers.insert( server );
	UNLOCK( &this->aliveServersLock );
	return true;
}

bool CoordinatorRemapMsgHandler::removeAliveServer( struct sockaddr_in server ) {
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

bool CoordinatorRemapMsgHandler::resetClientAck( struct sockaddr_in server ) {
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

bool CoordinatorRemapMsgHandler::isAllClientAcked( struct sockaddr_in server ) {
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
		/*
		printf( "Server %s:%hu changes its state to: (%d) ", buf, ntohs( server.sin_port ), this->serversState[ server ] );
		switch( this->serversState[ server ] ) {
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
		*/
		pthread_cond_broadcast( &this->ackSignal[ server ] );
	}
	UNLOCK( &this->clientsAckLock );
	return allAcked;
}

bool CoordinatorRemapMsgHandler::isInTransition( const struct sockaddr_in &server ) {
	bool ret;
	LOCK( &this->serversStateLock[ server ] );
	ret = ( serversState[ server ] == REMAP_INTERMEDIATE ) || ( serversState[ server ] == REMAP_COORDINATED );
	UNLOCK( &this->serversStateLock[ server ] );
	return ret;
}

bool CoordinatorRemapMsgHandler::allowRemapping( const struct sockaddr_in &server ) {
	bool ret;
	LOCK( &this->serversStateLock[ server ] );
	ret = ( serversState[ server ] == REMAP_INTERMEDIATE ) || ( serversState[ server ] == REMAP_DEGRADED );
	UNLOCK( &this->serversStateLock[ server ] );
	return ret;
}

bool CoordinatorRemapMsgHandler::reachMaximumRemapped( uint32_t maximum ) {
	uint32_t count = 0;
	LOCK( &this->aliveServersLock );
	for ( auto it = this->aliveServers.begin(); it != this->aliveServers.end(); it++ ) {
		const struct sockaddr_in &server = ( *it );
		// LOCK( &this->serversStateLock[ server ] );
		if ( serversState[ server ] != REMAP_NORMAL ) {
			count++;
		}
		// UNLOCK( &this->serversStateLock[ server ] );

		if ( count == maximum )
			break;
	}
	UNLOCK( &this->aliveServersLock );
	return ( count == maximum );
}
