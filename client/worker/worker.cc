#include <ctime>
#include <ctype.h>
#include <utility>
#include "worker.hh"
#include "../main/client.hh"
#include "../remap/basic_remap_scheme.hh"

uint32_t ClientWorker::dataChunkCount;
uint32_t ClientWorker::parityChunkCount;
uint32_t ClientWorker::updateInterval;
IDGenerator *ClientWorker::idGenerator;
Pending *ClientWorker::pending;
ClientEventQueue *ClientWorker::eventQueue;

void ClientWorker::dispatch( MixedEvent event ) {
	switch( event.type ) {
		case EVENT_TYPE_APPLICATION:
			this->dispatch( event.event.application );
			break;
		case EVENT_TYPE_COORDINATOR:
			this->dispatch( event.event.coordinator );
			break;
		case EVENT_TYPE_SERVER:
			this->dispatch( event.event.server );
			break;
		case EVENT_TYPE_DUMMY:
			break;
		default:
			__ERROR__( "ClientWorker", "dispatch", "Unsupported event type." );
			break;
	}
}

ServerSocket *ClientWorker::getServers( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId ) {
	ServerSocket *ret;
	Key key;
	key.set( size, data );
	listId = Client::getInstance()->stripeList->get(
		data, ( size_t ) size,
		this->dataServerSockets,
		this->parityServerSockets,
		&chunkId, true
	);
	ret = this->dataServerSockets[ chunkId ];
	return ret->ready() ? ret : 0;
}

bool ClientWorker::getServers(
	uint8_t opcode, char *data, uint8_t size,
	uint32_t *&original, uint32_t *&remapped, uint32_t &remappedCount,
	ServerSocket *&originalDataServerSocket, bool &useCoordinatedFlow,
	bool isGettingSplit
) {
	bool ret = true;
	Client *client = Client::getInstance();

	useCoordinatedFlow = false;

	// Determine original data server
	uint32_t originalListId, originalChunkId;
	originalListId = client->stripeList->get(
		data, ( size_t )( size - ( isGettingSplit ? SPLIT_OFFSET_SIZE : 0 ) ),
		this->dataServerSockets,
		this->parityServerSockets,
		&originalChunkId, true
	);

	uint32_t splitOffset, splitIndex;
	if ( isGettingSplit ) {
		bool isLarge;
		splitOffset = LargeObjectUtil::readSplitOffset( data + size - SPLIT_OFFSET_SIZE );
		splitIndex = LargeObjectUtil::getSplitIndex( size - SPLIT_OFFSET_SIZE, 0, splitOffset, isLarge );

		originalChunkId = ( originalChunkId + splitIndex ) % ClientWorker::dataChunkCount;
	}

	originalDataServerSocket = this->dataServerSockets[ originalChunkId ];

	switch( opcode ) {
		case PROTO_OPCODE_SET:
			// already checked in ClientWorker::handleSetRequest()
			useCoordinatedFlow = true;
			break;
		case PROTO_OPCODE_GET:
			if ( client->isDegraded( originalDataServerSocket ) )
				useCoordinatedFlow = true;
			break;
		case PROTO_OPCODE_UPDATE:
		case PROTO_OPCODE_DELETE:
			// Check the whole stripe
			for ( uint32_t i = 0, chunkCount = ClientWorker::dataChunkCount + ClientWorker::parityChunkCount; i < chunkCount; i++ ) {
				if ( client->isDegraded(
						( i < ClientWorker::dataChunkCount ) ?
						this->dataServerSockets[ i ] :
						this->parityServerSockets[ i - ClientWorker::dataChunkCount ]
					)
				) {
					useCoordinatedFlow = true;
					break;
				}
			}
			break;
	}

	if ( ! useCoordinatedFlow ) {
		remappedCount = 0;
		original = 0;
		remapped = 0;
		return ret;
	}

	original = this->original;
	remapped = this->remapped;

	uint32_t numEntries = 0;
	original[ numEntries * 2     ] = originalListId;
	original[ numEntries * 2 + 1 ] = originalChunkId;
	numEntries++;
	for ( uint32_t i = 0; i < ClientWorker::parityChunkCount; i++ ) {
		// Parity server fails
		original[ numEntries * 2 ] = originalListId;
		original[ numEntries * 2 + 1 ] = ClientWorker::dataChunkCount + i;
		numEntries++;
	}
	if ( opcode == PROTO_OPCODE_UPDATE || opcode == PROTO_OPCODE_DELETE ) {
		// Always perform a degraded read if some other data chunks in the same stripe are lost
		for ( uint32_t i = 0; i < ClientWorker::dataChunkCount; i++ ) {
			if ( i == originalChunkId )
				continue;
			if ( client->isDegraded( this->dataServerSockets[ i ] ) ) {
				original[ numEntries * 2 ] = originalListId;
				original[ numEntries * 2 + 1 ] = i;
				numEntries++;
			}
		}
	}

	// Determine remapped data server
	BasicRemappingScheme::redirect(
		this->original, this->remapped, numEntries, remappedCount,
		ClientWorker::dataChunkCount, ClientWorker::parityChunkCount,
		this->dataServerSockets, this->parityServerSockets,
		opcode == PROTO_OPCODE_GET
	);

	if ( remappedCount ) {
		uint32_t *_original = new uint32_t[ remappedCount * 2 ];
		uint32_t *_remapped = new uint32_t[ remappedCount * 2 ];
		for ( uint32_t i = 0; i < remappedCount * 2; i++ ) {
			_original[ i ] = original[ i ];
			_remapped[ i ] = remapped[ i ];
		}
		original = _original;
		remapped = _remapped;
	} else {
		original = 0;
		remapped = 0;
	}

	return ret;
}

ServerSocket *ClientWorker::getServers( uint32_t listId, uint32_t chunkId ) {
	ServerSocket *ret;
	Client::getInstance()->stripeList->get( listId, this->parityServerSockets, this->dataServerSockets );
	ret = chunkId < ClientWorker::dataChunkCount ? this->dataServerSockets[ chunkId ] : this->parityServerSockets[ chunkId - ClientWorker::dataChunkCount ];
	return ret->ready() ? ret : 0;
}

void ClientWorker::removePending( ServerSocket *server, bool needsAck ) {

	struct sockaddr_in saddr = server->getAddr();
	char buf[ INET_ADDRSTRLEN ];
	inet_ntop( AF_INET, &saddr.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
	// remove pending ack
	std::vector<AcknowledgementInfo> ackInfoList;
	// remove parity backup ack
	ClientWorker::pending->eraseAck( PT_ACK_REMOVE_PARITY, server->instanceId, &ackInfoList );
	for ( AcknowledgementInfo &it : ackInfoList ) {
		if ( it.lock ) LOCK( it.lock );
		if ( it.counter ) *it.counter -= 1;
		if ( it.lock ) UNLOCK( it.lock );
	}
	// revert parity ack
	ackInfoList.clear();
	ClientWorker::pending->eraseAck( PT_ACK_REVERT_DELTA, server->instanceId, &ackInfoList );
	for ( AcknowledgementInfo &it : ackInfoList ) {
		if ( it.lock ) LOCK( it.lock );
		if ( it.counter ) *it.counter -= 1;
		if ( it.lock ) UNLOCK( it.lock );
	}

	if ( needsAck )
		Client::getInstance()->stateTransitHandler.ackTransit();
}

void ClientWorker::replayRequestPrepare( ServerSocket *server ) {
	uint16_t instanceId = server->instanceId;
	Pending *pending = ClientWorker::pending;
	if ( pending->replay.requestsLock.count( instanceId ) == 0 ) {
		pending->replay.requestsLock[ instanceId ] = LOCK_T();
		LOCK_INIT( &pending->replay.requestsLock.at( instanceId ) );
	}
	if ( pending->replay.requests.count( instanceId ) == 0 ) {
		pending->replay.requests[ instanceId ] = std::map<uint32_t, RequestInfo>();
	}
	LOCK_T *lock = &pending->replay.requestsLock.at( instanceId );
	std::map<uint32_t, RequestInfo> *map = &pending->replay.requests.at( instanceId );
	LOCK_T *pendingLock;
	RequestInfo requestInfo;
	PendingIdentifier pid;
	uint32_t currentTime = Client::getInstance()->timestamp.getVal();
	uint32_t smallestTime = currentTime, smallestTimeAfterCurrent = currentTime;

#define SEARCH_MAP_FOR_REQUEST( _OPCODE_, _SERVER_VALUE_TYPE_, _APPLICATION_VALUE_TYPE_, _APPLICATION_VALUE_VAR_, _PENDING_TYPE_, _PENDING_SET_NAME_ ) \
	do { \
		_APPLICATION_VALUE_TYPE_ _APPLICATION_VALUE_VAR_; \
		bool needsDup = false; \
		LOCK( pendingLock ); \
		for ( \
			std::unordered_multimap<PendingIdentifier, _SERVER_VALUE_TYPE_>::iterator it = pending->servers._PENDING_SET_NAME_.begin(), safeIt = it; \
			it != pending->servers._PENDING_SET_NAME_.end(); it = safeIt \
		) { \
			/* hold a save ptr for safe erase */ \
			safeIt++; \
			/* skip requests other than those associated with target server */ \
			if ( it->first.ptr != server ) \
				continue; \
			/* skip request if backup is not available */ \
			if ( ! pending->erase##_APPLICATION_VALUE_TYPE_( PT_APPLICATION_##_PENDING_TYPE_, it->first.parentInstanceId, it->first.parentRequestId, ( void* ) 0, &pid, &_APPLICATION_VALUE_VAR_, true, true, true, it->second.data ) ) { \
				__ERROR__( "ClientWorker", "replayRequestPrepare", "Cannot find the %s request backup for ID = (%u, %u).", #_OPCODE_, it->first.parentInstanceId, it->first.parentRequestId ); \
				continue; \
			} \
			/* cancel the request reply to application by setting application socket to 0 */ \
			if ( ClientWorker::pending->count( PT_SERVER_##_PENDING_TYPE_, it->first.instanceId, it->first.requestId, false, false /* already locked (pendingLock) */ ) > 1 ) { \
				__DEBUG__( YELLOW, "ClientWorker", "replayRequestPrepare", "Reinsert ID = (%u,%u)\n", ( ( ServerSocket * ) it->first.ptr )->getSocket(), it->first.parentInstanceId, it->first.parentRequestId ); \
				if ( ! ClientWorker::pending->insert##_APPLICATION_VALUE_TYPE_( PT_APPLICATION_##_PENDING_TYPE_, pid.instanceId, pid.requestId, pid.ptr, _APPLICATION_VALUE_VAR_ ) ) \
					__ERROR__( "ClientWorker", "replayRequestPrepare", "Failed to reinsert the %s request backup for ID = (%u, %u).", #_OPCODE_, pid.instanceId, pid.requestId ); \
				else \
					needsDup = true; \
			} \
			__DEBUG__( YELLOW, "ClientWorker", "replayRequestPrepare", "Add %s request ID = (%u,%u) with timestamp %u to replay.", #_OPCODE_, pid.instanceId, pid.requestId, pid.timestamp ); \
			/* insert the request into pending set for replay */ \
			requestInfo.set( pid.ptr, pid.instanceId, pid.requestId, PROTO_OPCODE_##_OPCODE_, _APPLICATION_VALUE_VAR_, !needsDup ); \
			map->insert( std::pair<uint32_t, RequestInfo>( pid.timestamp, requestInfo ) ); \
			/* update for finding the first timestamp for replay */ \
			if ( pid.timestamp < smallestTime ) \
				smallestTime = pid.timestamp; \
			if ( pid.timestamp > currentTime && ( pid.timestamp < smallestTimeAfterCurrent || smallestTimeAfterCurrent == currentTime ) ) \
				smallestTimeAfterCurrent = pid.timestamp; \
			/* remove the pending ack */ \
			pending->servers._PENDING_SET_NAME_.erase( it ); \
		} \
		UNLOCK( pendingLock ); \
	} while( 0 )

	LOCK( lock );
	// SET
	pendingLock = &pending->servers.setLock;
	SEARCH_MAP_FOR_REQUEST( SET, Key, KeyValue, keyValue, SET, set );
	// GET
	pendingLock = &pending->servers.getLock;
	SEARCH_MAP_FOR_REQUEST( GET, Key, Key, key, GET, get );
	// UPDATE
	pendingLock = &pending->servers.updateLock;
	SEARCH_MAP_FOR_REQUEST( UPDATE, KeyValueUpdate, KeyValueUpdate, keyValueUpdate, UPDATE, update );
	// DELETE
	pendingLock = &pending->servers.delLock;
	SEARCH_MAP_FOR_REQUEST( DELETE, Key, Key, key, DEL, del );

	/* mark the first timestamp to start the replay */
	if ( ! pending->replay.requests.empty() || pending->replay.requestsStartTime.count( instanceId ) == 0 ) {
		if ( smallestTimeAfterCurrent == currentTime )
			pending->replay.requestsStartTime[ instanceId ] = smallestTime;
		else if ( smallestTimeAfterCurrent > currentTime ) // wrapped around case
			pending->replay.requestsStartTime[ instanceId ] = smallestTimeAfterCurrent;
		else
			; // BUG
	}
	UNLOCK( lock );

#undef SEARCH_MAP_FOR_REQUEST
}

void ClientWorker::replayRequest( ServerSocket *server ) {
	uint16_t instanceId = server->instanceId;

	if (
		ClientWorker::pending->replay.requestsLock.count( instanceId ) == 0 ||
		ClientWorker::pending->replay.requests.count( instanceId ) == 0
	) {
		__ERROR__( "ClientWorker", "replayRequest", "Cannot replay request for server with id = %u.", instanceId );
		return;
	}
	LOCK_T *lock = &ClientWorker::pending->replay.requestsLock.at( instanceId );

	std::map<uint32_t, RequestInfo> *map = &ClientWorker::pending->replay.requests.at( instanceId );
	std::map<uint32_t, RequestInfo>::iterator lit, rit;
	ApplicationEvent event;

	LOCK( lock );

	if ( ! ClientWorker::pending->replay.requestsStartTime.count( instanceId ) || map->empty() ) {
		UNLOCK( lock );
		return;
	}

	// from the first timestamp, to the first timestamp
	lit = map->find( pending->replay.requestsStartTime[ instanceId ] );
	__DEBUG__( GREEN, "ClientWorker", "replayRequest", "Start from time %u with current time %u", pending->replay.requestsStartTime[ instanceId ], Client::getInstance()->timestamp.getVal() );
	rit = lit;
	// replay requests
	do {
		switch ( lit->second.opcode ) {
			case PROTO_OPCODE_SET:
				event.replaySetRequest(
					( ApplicationSocket * ) lit->second.application,
					lit->second.instanceId, lit->second.requestId,
					lit->second.keyValue
				);
				break;
			case PROTO_OPCODE_GET:
				event.replayGetRequest(
					( ApplicationSocket * ) lit->second.application,
					lit->second.instanceId, lit->second.requestId,
					lit->second.key
				);
				break;
			case PROTO_OPCODE_UPDATE:
				event.replayUpdateRequest(
					( ApplicationSocket * ) lit->second.application,
					lit->second.instanceId, lit->second.requestId,
					lit->second.keyValueUpdate
				);
				break;
			case PROTO_OPCODE_DELETE:
				event.replayDeleteRequest(
					( ApplicationSocket * ) lit->second.application,
					lit->second.instanceId, lit->second.requestId,
					lit->second.key
				);
				break;
			default:
				__ERROR__( "ClientWorker", "replayRequest", "Unknown request OPCODE = 0x%02x", lit->second.opcode );
				continue;
		}
		ClientWorker::eventQueue->insert( event );
		lit++;
		if ( lit == map->end() )
			lit = map->begin();
	} while( lit != rit && lit != map->end() );
	if ( lit == map->begin() )
		lit = map->end();
	lit--;
	__DEBUG__( YELLOW, "ClientWorker", "replayRequest", "Last replayed request OPCODE = 0x%02x at time %u", lit->second.opcode, lit->first );

	map->clear();
	pending->replay.requestsStartTime.erase( instanceId );

	UNLOCK( lock );
}

void ClientWorker::gatherPendingNormalRequests( ServerSocket *target, bool needsAck ) {
	Client *client = Client::getInstance();
	StripeList<ServerSocket> *stripeList = client->stripeList;
	std::unordered_set<uint32_t> listIds;
	// find the lists including the failed server as parity server
	for ( uint32_t i = 0, len = stripeList->getNumList(); i < len; i++ ) {
		for ( uint32_t j = 0; j < parityChunkCount; j++ ) {
			if ( stripeList->get( i, j + dataChunkCount ) == target ) {
				listIds.insert( i );
				break;
			}
		}
	}
	__DEBUG__( CYAN, "ClientWorker", "gatherPendingNormalRequest", "%lu list(s) included the server as parity", listIds.size() );

	uint32_t listId, chunkId;
	struct sockaddr_in addr = target->getAddr();
	bool hasPending = false;
	std::unordered_set<uint32_t> outdatedPendingRequests;

	ClientStateTransitHandler *mh = &client->stateTransitHandler;

	LOCK ( &mh->stateTransitInfo[ addr ].counter.pendingNormalRequests.lock );

#define GATHER_PENDING_NORMAL_REQUESTS( _OP_TYPE_, _MAP_VALUE_TYPE_ ) { \
	LOCK ( &pending->servers._OP_TYPE_##Lock ); \
	std::unordered_multimap<PendingIdentifier, _MAP_VALUE_TYPE_> *map = &pending->servers._OP_TYPE_; \
	std::unordered_multimap<PendingIdentifier, _MAP_VALUE_TYPE_>::iterator it, saveIt; \
	for( it = map->begin(), saveIt = it; it != map->end(); it = saveIt ) { \
		saveIt++; \
		listId = stripeList->get( it->second.data, it->second.size, 0, 0, &chunkId ); \
		ServerSocket *dataServerSocket = stripeList->get( listId, chunkId ); \
		/* skip request whose data server fails, but if the data server is newly added, remove pending normal request for this server */ \
		if ( target->instanceId == dataServerSocket->instanceId ) { \
			outdatedPendingRequests.insert( it->first.parentRequestId ); \
			continue; \
		} \
		/* skip requests not in stripes involving the failed server */ \
		if ( listIds.count( listId ) == 0 ) \
			continue; \
		/* no need to wait for requests whose data server is failed */ \
		if ( mh->useCoordinatedFlow( dataServerSocket->getAddr() ) ) { \
			continue; \
		} \
		/* put the completion of request in account for state transition */ \
		mh->stateTransitInfo.at( addr ).addPendingRequest( it->first.parentRequestId, false, false ); \
		__DEBUG__( CYAN, "ClientWorker", "gatherPendingNormalRequest", "Pending normal %s instance=%d request id=%u parentid=%u dataServer=%d in list=%d for transit.", #_OP_TYPE_, target->instanceId, it->first.requestId, it->first.parentRequestId, stripeList->get( listId, chunkId )->instanceId, listId ); \
		hasPending = true; \
	} \
	UNLOCK ( &pending->servers._OP_TYPE_##Lock ); \
}
	// SET
	GATHER_PENDING_NORMAL_REQUESTS( set, Key );
	// UPDATE
	GATHER_PENDING_NORMAL_REQUESTS( update, KeyValueUpdate );
	// DELETE
	GATHER_PENDING_NORMAL_REQUESTS( del, Key );

	if ( ! hasPending  ) {
		__INFO__( GREEN, "ClientWorker", "gatherPendingNormalRequest", "No pending normal requests for transit." );
		mh->stateTransitInfo[ addr ].setCompleted( false, false );
		if ( needsAck )
			mh->ackTransit( addr );
	} else {
		__DEBUG__( CYAN, "ClientWorker", "gatherPendingNormalRequest", "id=%d has %u request pending", target->instanceId, mh->stateTransitInfo.at( addr ).getPendingRequestCount( false, false ) );
	}

	UNLOCK ( &mh->stateTransitInfo[ addr ].counter.pendingNormalRequests.lock );

	for ( uint32_t id : outdatedPendingRequests ) {
		for ( auto &server : mh->stateTransitInfo ) {
			if ( server.first == target->getAddr() )
				continue;
			if ( mh->stateTransitInfo[ server.first ].removePendingRequest( id ) == 0 ) {
				if ( mh->stateTransitInfo.at( server.first ).setCompleted() ) {
					// Let the stateTransitHandler to perform ack after releasing all locks on server states
					//stateTransitHandler->ackTransit( server.first );
				}
			}
		}
	}

#undef GATHER_PENDING_NORMAL_REQUESTS
}

void ClientWorker::free() {
	this->protocol.free();
	delete[] original;
	delete[] remapped;
	delete[] this->dataServerSockets;
	delete[] this->parityServerSockets;
}

void *ClientWorker::run( void *argv ) {
	ClientWorker *worker = ( ClientWorker * ) argv;
	ClientEventQueue *eventQueue = ClientWorker::eventQueue;

	MixedEvent event;
	bool ret;
	while( worker->getIsRunning() | ( ret = eventQueue->extractMixed( event ) ) ) {
		if ( ret )
			worker->dispatch( event );
	}

	worker->free();
	pthread_exit( 0 );
	return 0;
}

bool ClientWorker::init() {
	Client *client = Client::getInstance();

	ClientWorker::idGenerator = &client->idGenerator;
	ClientWorker::dataChunkCount = client->config.global.coding.params.getDataChunkCount();
	ClientWorker::parityChunkCount = client->config.global.coding.params.getParityChunkCount();
	ClientWorker::updateInterval = client->config.global.timeout.load;
	ClientWorker::pending = &client->pending;
	ClientWorker::eventQueue = &client->eventQueue;
	return true;
}

bool ClientWorker::init( GlobalConfig &config, uint32_t workerId ) {
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			config.size.key,
			config.size.chunk,
			true
		)
	);
	this->original = new uint32_t[ ( ClientWorker::dataChunkCount + ClientWorker::parityChunkCount ) * 2 ];
	this->remapped = new uint32_t[ ( ClientWorker::dataChunkCount + ClientWorker::parityChunkCount ) * 2 ];
	this->dataServerSockets = new ServerSocket*[ ClientWorker::dataChunkCount ];
	this->parityServerSockets = new ServerSocket*[ ClientWorker::parityChunkCount ];
	this->workerId = workerId;
	return true;
}

bool ClientWorker::start() {
	this->isRunning = true;
	if ( pthread_create( &this->tid, NULL, ClientWorker::run, ( void * ) this ) != 0 ) {
		__ERROR__( "ClientWorker", "start", "Cannot start worker thread." );
		return false;
	}
	return true;
}

void ClientWorker::stop() {
	this->isRunning = false;
}

void ClientWorker::print( FILE *f ) {
	fprintf( f, "Worker (Thread ID = %lu): %srunning\n", this->tid, this->isRunning ? "" : "not " );
}
