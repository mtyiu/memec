#include <ctime>
#include <ctype.h>
#include <utility>
#include "worker.hh"
#include "../main/client.hh"
#include "../remap/basic_remap_scheme.hh"

uint32_t MasterWorker::dataChunkCount;
uint32_t MasterWorker::parityChunkCount;
uint32_t MasterWorker::updateInterval;
bool MasterWorker::disableDegraded;
IDGenerator *MasterWorker::idGenerator;
Pending *MasterWorker::pending;
ClientEventQueue *MasterWorker::eventQueue;
StripeList<ServerSocket> *MasterWorker::stripeList;
PacketPool *MasterWorker::packetPool;
ArrayMap<int, ServerSocket> *MasterWorker::serverSockets;
MasterRemapMsgHandler *MasterWorker::remapMsgHandler;

void MasterWorker::dispatch( MixedEvent event ) {
	switch( event.type ) {
		case EVENT_TYPE_APPLICATION:
			this->dispatch( event.event.application );
			break;
		case EVENT_TYPE_COORDINATOR:
			this->dispatch( event.event.coordinator );
			break;
		case EVENT_TYPE_CLIENT:
			this->dispatch( event.event.master );
			break;
		case EVENT_TYPE_SERVER:
			this->dispatch( event.event.slave );
			break;
		default:
			break;
	}
}

void MasterWorker::dispatch( ClientEvent event ) {}

ServerSocket *MasterWorker::getSlaves( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId ) {
	ServerSocket *ret;
	Key key;
	key.set( size, data );
	listId = MasterWorker::stripeList->get(
		data, ( size_t ) size,
		this->dataServerSockets,
		this->parityServerSockets,
		&chunkId, true
	);
	ret = this->dataServerSockets[ chunkId ];
	return ret->ready() ? ret : 0;
}

bool MasterWorker::getSlaves(
	uint8_t opcode, char *data, uint8_t size,
	uint32_t *&original, uint32_t *&remapped, uint32_t &remappedCount,
	ServerSocket *&originalDataServerSocket, bool &useCoordinatedFlow
) {
	bool ret = true;

	useCoordinatedFlow = false;

	// Determine original data slave
	uint32_t originalListId, originalChunkId;
	originalListId = MasterWorker::stripeList->get(
		data, ( size_t ) size,
		this->dataServerSockets,
		this->parityServerSockets,
		&originalChunkId, true
	);
	originalDataServerSocket = this->dataServerSockets[ originalChunkId ];

	Master *master = Master::getInstance();
	switch( opcode ) {
		case PROTO_OPCODE_SET:
			// already checked in MasterWorker::handleSetRequest()
			useCoordinatedFlow = true;
			break;
		case PROTO_OPCODE_GET:
			if ( master->isDegraded( originalDataServerSocket ) )
				useCoordinatedFlow = true;
			break;
		case PROTO_OPCODE_UPDATE:
		case PROTO_OPCODE_DELETE:
			// Check the whole stripe
			for ( uint32_t i = 0, chunkCount = MasterWorker::dataChunkCount + MasterWorker::parityChunkCount; i < chunkCount; i++ ) {
				if ( master->isDegraded(
						( i < MasterWorker::dataChunkCount ) ?
						this->dataServerSockets[ i ] :
						this->parityServerSockets[ i - MasterWorker::dataChunkCount ]
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
	for ( uint32_t i = 0; i < MasterWorker::parityChunkCount; i++ ) {
		// Parity server fails
		original[ numEntries * 2 ] = originalListId;
		original[ numEntries * 2 + 1 ] = MasterWorker::dataChunkCount + i;
		numEntries++;
	}
	if ( opcode == PROTO_OPCODE_UPDATE || opcode == PROTO_OPCODE_DELETE ) {
		// Always perform a degraded read if some other data chunks in the same stripe are lost
		for ( uint32_t i = 0; i < MasterWorker::dataChunkCount; i++ ) {
			if ( i == originalChunkId )
				continue;
			if ( master->isDegraded( this->dataServerSockets[ i ] ) ) {
				original[ numEntries * 2 ] = originalListId;
				original[ numEntries * 2 + 1 ] = i;
				numEntries++;
			}
		}
	}

	// Determine remapped data slave
	BasicRemappingScheme::redirect(
		this->original, this->remapped, numEntries, remappedCount,
		MasterWorker::dataChunkCount, MasterWorker::parityChunkCount,
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

		// printf( "[%s] %.*s: ", opcode == PROTO_OPCODE_GET ? "GET" : "UPDATE", size, data );
		// for ( uint32_t i = 0; i < remappedCount; i++ ) {
		// 	printf(
		// 		"%s(%u, %u) |-> (%u, %u)",
		// 		i == 0 ? "" : "; ",
		// 		original[ i * 2     ],
		// 		original[ i * 2 + 1 ],
		// 		remapped[ i * 2     ],
		// 		remapped[ i * 2 + 1 ]
		// 	);
		// }
		// printf( "\n" );
		// fflush( stdout );
	} else {
		original = 0;
		remapped = 0;
	}

	return ret;
}

ServerSocket *MasterWorker::getSlaves( uint32_t listId, uint32_t chunkId ) {
	ServerSocket *ret;
	MasterWorker::stripeList->get( listId, this->parityServerSockets, this->dataServerSockets );
	ret = chunkId < MasterWorker::dataChunkCount ? this->dataServerSockets[ chunkId ] : this->parityServerSockets[ chunkId - MasterWorker::dataChunkCount ];
	return ret->ready() ? ret : 0;
}

void MasterWorker::removePending( ServerSocket *slave, bool needsAck ) {

	struct sockaddr_in saddr = slave->getAddr();
	char buf[ INET_ADDRSTRLEN ];
	inet_ntop( AF_INET, &saddr.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
	// remove pending ack
	std::vector<AcknowledgementInfo> ackInfoList;
	// remove parity backup ack
	MasterWorker::pending->eraseAck( PT_ACK_REMOVE_PARITY, slave->instanceId, &ackInfoList );
	for ( AcknowledgementInfo &it : ackInfoList ) {
		if ( it.lock ) LOCK( it.lock );
		if ( it.counter ) *it.counter -= 1;
		if ( it.lock ) UNLOCK( it.lock );
	}
	// revert parity ack
	ackInfoList.clear();
	MasterWorker::pending->eraseAck( PT_ACK_REVERT_DELTA, slave->instanceId, &ackInfoList );
	for ( AcknowledgementInfo &it : ackInfoList ) {
		if ( it.lock ) LOCK( it.lock );
		if ( it.counter ) *it.counter -= 1;
		if ( it.lock ) UNLOCK( it.lock );
	}

	if ( needsAck )
		Master::getInstance()->remapMsgHandler.ackTransit();
}

void MasterWorker::replayRequestPrepare( ServerSocket *slave ) {
	uint16_t instanceId = slave->instanceId;
	Pending *pending = MasterWorker::pending;
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
	uint32_t currentTime = Master::getInstance()->timestamp.getVal();
	uint32_t smallestTime = currentTime, smallestTimeAfterCurrent = currentTime;

#define SEARCH_MAP_FOR_REQUEST( _OPCODE_, _SLAVE_VALUE_TYPE_, _APPLICATION_VALUE_TYPE_, _APPLICATION_VALUE_VAR_, _PENDING_TYPE_, _PENDING_SET_NAME_ ) \
	do { \
		_APPLICATION_VALUE_TYPE_ _APPLICATION_VALUE_VAR_; \
		bool needsDup = false; \
		LOCK( pendingLock ); \
		for ( \
			std::unordered_multimap<PendingIdentifier, _SLAVE_VALUE_TYPE_>::iterator it = pending->slaves._PENDING_SET_NAME_.begin(), safeIt = it; \
			it != pending->slaves._PENDING_SET_NAME_.end(); it = safeIt \
		) { \
			/* hold a save ptr for safe erase */ \
			safeIt++; \
			/* skip requests other than those associated with target slave */ \
			if ( it->first.ptr != slave ) \
				continue; \
			/* skip request if backup is not available */ \
			if ( ! pending->erase##_APPLICATION_VALUE_TYPE_( PT_APPLICATION_##_PENDING_TYPE_, it->first.parentInstanceId, it->first.parentRequestId, ( void* ) 0, &pid, &_APPLICATION_VALUE_VAR_, true, true, true, it->second.data ) ) { \
				__ERROR__( "MasterWorker", "replayRequestPrepare", "Cannot find the %s request backup for ID = (%u, %u).", #_OPCODE_, it->first.parentInstanceId, it->first.parentRequestId ); \
				continue; \
			} \
			/* cancel the request reply to application by setting application socket to 0 */ \
			if ( MasterWorker::pending->count( PT_SLAVE_##_PENDING_TYPE_, it->first.instanceId, it->first.requestId, false, false /* already locked (pendingLock) */ ) > 1 ) { \
				__DEBUG__( YELLOW, "MasterWorker", "replayRequestPrepare", "Reinsert ID = (%u,%u)\n", ( ( ServerSocket * ) it->first.ptr )->getSocket(), it->first.parentInstanceId, it->first.parentRequestId ); \
				if ( ! MasterWorker::pending->insert##_APPLICATION_VALUE_TYPE_( PT_APPLICATION_##_PENDING_TYPE_, pid.instanceId, pid.requestId, pid.ptr, _APPLICATION_VALUE_VAR_ ) ) \
					__ERROR__( "MasterWorker", "replayRequestPrepare", "Failed to reinsert the %s request backup for ID = (%u, %u).", #_OPCODE_, pid.instanceId, pid.requestId ); \
				else \
					needsDup = true; \
			} \
			__DEBUG__( YELLOW, "MasterWorker", "replayRequestPrepare", "Add %s request ID = (%u,%u) with timestamp %u to replay.", #_OPCODE_, pid.instanceId, pid.requestId, pid.timestamp ); \
			/* insert the request into pending set for replay */ \
			requestInfo.set( pid.ptr, pid.instanceId, pid.requestId, PROTO_OPCODE_##_OPCODE_, _APPLICATION_VALUE_VAR_, !needsDup ); \
			map->insert( std::pair<uint32_t, RequestInfo>( pid.timestamp, requestInfo ) ); \
			/* update for finding the first timestamp for replay */ \
			if ( pid.timestamp < smallestTime ) \
				smallestTime = pid.timestamp; \
			if ( pid.timestamp > currentTime && ( pid.timestamp < smallestTimeAfterCurrent || smallestTimeAfterCurrent == currentTime ) ) \
				smallestTimeAfterCurrent = pid.timestamp; \
			/* remove the pending ack */ \
			pending->slaves._PENDING_SET_NAME_.erase( it ); \
		} \
		UNLOCK( pendingLock ); \
	} while( 0 )

	LOCK( lock );
	// SET
	pendingLock = &pending->slaves.setLock;
	SEARCH_MAP_FOR_REQUEST( SET, Key, KeyValue, keyValue, SET, set );
	// GET
	pendingLock = &pending->slaves.getLock;
	SEARCH_MAP_FOR_REQUEST( GET, Key, Key, key, GET, get );
	// UPDATE
	pendingLock = &pending->slaves.updateLock;
	SEARCH_MAP_FOR_REQUEST( UPDATE, KeyValueUpdate, KeyValueUpdate, keyValueUpdate, UPDATE, update );
	// DELETE
	pendingLock = &pending->slaves.delLock;
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

void MasterWorker::replayRequest( ServerSocket *slave ) {
	uint16_t instanceId = slave->instanceId;

	if (
		MasterWorker::pending->replay.requestsLock.count( instanceId ) == 0 ||
		MasterWorker::pending->replay.requests.count( instanceId ) == 0
	) {
		__ERROR__( "MasterWorker", "replayRequest", "Cannot replay request for slave with id = %u.", instanceId );
		return;
	}
	LOCK_T *lock = &MasterWorker::pending->replay.requestsLock.at( instanceId );

	std::map<uint32_t, RequestInfo> *map = &MasterWorker::pending->replay.requests.at( instanceId );
	std::map<uint32_t, RequestInfo>::iterator lit, rit;
	ApplicationEvent event;

	LOCK( lock );

	if ( ! MasterWorker::pending->replay.requestsStartTime.count( instanceId ) || map->empty() ) {
		UNLOCK( lock );
		return;
	}

	// from the first timestamp, to the first timestamp
	lit = map->find( pending->replay.requestsStartTime[ instanceId ] );
	__DEBUG__( GREEN, "MasterWorker", "replayRequest", "Start from time %u with current time %u", pending->replay.requestsStartTime[ instanceId ], Master::getInstance()->timestamp.getVal() );
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
				__ERROR__( "MasterWorker", "replayRequest", "Unknown request OPCODE = 0x%02x", lit->second.opcode );
				continue;
		}
		MasterWorker::eventQueue->insert( event );
		lit++;
		if ( lit == map->end() )
			lit = map->begin();
	} while( lit != rit && lit != map->end() );
	if ( lit == map->begin() )
		lit = map->end();
	lit--;
	__DEBUG__( YELLOW, "MasterWorker", "replayRequest", "Last replayed request OPCODE = 0x%02x at time %u", lit->second.opcode, lit->first );

	map->clear();
	pending->replay.requestsStartTime.erase( instanceId );

	UNLOCK( lock );
}

void MasterWorker::gatherPendingNormalRequests( ServerSocket *target, bool needsAck ) {

	std::unordered_set<uint32_t> listIds;
	// find the lists including the failed slave as parity server
	for ( uint32_t i = 0, len = stripeList->getNumList(); i < len; i++ ) {
		for ( uint32_t j = 0; j < parityChunkCount; j++ ) {
			if ( stripeList->get( i, j + dataChunkCount ) == target )
				listIds.insert( i );
		}
	}
	__DEBUG__( CYAN, "MasterWorker", "gatherPendingNormalRequest", "%lu list(s) included the slave as parity", listIds.size() );

	uint32_t listId;
	struct sockaddr_in addr = target->getAddr();
	bool hasPending = false;

	MasterRemapMsgHandler *mh = &Master::getInstance()->remapMsgHandler;

	LOCK ( &mh->stateTransitInfo[ addr ].counter.pendingNormalRequests.lock );

#define GATHER_PENDING_NORMAL_REQUESTS( _OP_TYPE_, _MAP_VALUE_TYPE_ ) { \
	LOCK ( &pending->slaves._OP_TYPE_##Lock ); \
	std::unordered_multimap<PendingIdentifier, _MAP_VALUE_TYPE_> *map = &pending->slaves._OP_TYPE_; \
	std::unordered_multimap<PendingIdentifier, _MAP_VALUE_TYPE_>::iterator it, saveIt; \
	for( it = map->begin(), saveIt = it; it != map->end(); it = saveIt ) { \
		saveIt++; \
		listId = stripeList->get( it->second.data, it->second.size, 0, 0 ); \
		if ( listIds.count( listId ) == 0 ) \
			continue; \
		/* put the completion of request in account for state transition */\
		mh->stateTransitInfo.at( addr ).addPendingRequest( it->first.requestId, false, false ); \
		__INFO__( CYAN, "MasterWorker", "gatherPendingNormalRequest", "Pending normal request id=%u for transit.", it->first.requestId ); \
		hasPending = true; \
	} \
	UNLOCK ( &pending->slaves._OP_TYPE_##Lock ); \
}
	// SET
	GATHER_PENDING_NORMAL_REQUESTS( set, Key );
	// UPDATE
	GATHER_PENDING_NORMAL_REQUESTS( update, KeyValueUpdate );
	// DELETE
	GATHER_PENDING_NORMAL_REQUESTS( del, Key );

	UNLOCK ( &mh->stateTransitInfo[ addr ].counter.pendingNormalRequests.lock );

	if ( ! hasPending  ) {
		__INFO__( GREEN, "MasterWorker", "gatherPendingNormalRequest", "No pending normal requests for transit." );
		Master::getInstance()->remapMsgHandler.stateTransitInfo[ addr ].setCompleted();
		if ( needsAck )
			Master::getInstance()->remapMsgHandler.ackTransit( addr );
	}

#undef GATHER_PENDING_NORMAL_REQUESTS
}

void MasterWorker::free() {
	this->protocol.free();
	delete[] original;
	delete[] remapped;
	delete[] this->dataServerSockets;
	delete[] this->parityServerSockets;
}

void *MasterWorker::run( void *argv ) {
	MasterWorker *worker = ( MasterWorker * ) argv;
	ClientEventQueue *eventQueue = MasterWorker::eventQueue;

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

bool MasterWorker::init() {
	Master *master = Master::getInstance();

	MasterWorker::idGenerator = &master->idGenerator;
	MasterWorker::dataChunkCount = master->config.global.coding.params.getDataChunkCount();
	MasterWorker::parityChunkCount = master->config.global.coding.params.getParityChunkCount();
	MasterWorker::updateInterval = master->config.global.timeout.load;
	MasterWorker::disableDegraded = master->config.client.degraded.disabled;
	MasterWorker::pending = &master->pending;
	MasterWorker::eventQueue = &master->eventQueue;
	MasterWorker::stripeList = master->stripeList;
	MasterWorker::serverSockets = &master->sockets.slaves;
	MasterWorker::packetPool = &master->packetPool;
	MasterWorker::remapMsgHandler = &master->remapMsgHandler;
	return true;
}

bool MasterWorker::init( GlobalConfig &config, uint32_t workerId ) {
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			config.size.key,
			config.size.chunk
		)
	);
	this->original = new uint32_t[ ( MasterWorker::dataChunkCount + MasterWorker::parityChunkCount ) * 2 ];
	this->remapped = new uint32_t[ ( MasterWorker::dataChunkCount + MasterWorker::parityChunkCount ) * 2 ];
	this->dataServerSockets = new ServerSocket*[ MasterWorker::dataChunkCount ];
	this->parityServerSockets = new ServerSocket*[ MasterWorker::parityChunkCount ];
	this->workerId = workerId;
	return true;
}

bool MasterWorker::start() {
	this->isRunning = true;
	if ( pthread_create( &this->tid, NULL, MasterWorker::run, ( void * ) this ) != 0 ) {
		__ERROR__( "MasterWorker", "start", "Cannot start worker thread." );
		return false;
	}
	return true;
}

void MasterWorker::stop() {
	this->isRunning = false;
}

void MasterWorker::print( FILE *f ) {
	fprintf( f, "Worker (Thread ID = %lu): %srunning\n", this->tid, this->isRunning ? "" : "not " );
}
