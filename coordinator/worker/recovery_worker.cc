#include "worker.hh"
#include "../ds/log.hh"
#include "../main/coordinator.hh"

bool CoordinatorWorker::handlePromoteBackupServerResponse( ServerEvent event, char *buf, size_t size ) {
	struct PromoteBackupServerHeader header;
	if ( ! this->protocol.parsePromoteBackupServerHeader( header, false /* isRequest */, buf, size ) ) {
		__ERROR__( "CoordinatorWorker", "handlePromoteBackupServerResponse", "Invalid PROMOTE_BACKUP_SERVER response (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "CoordinatorWorker", "handlePromoteBackupServerResponse",
		"[PROMOTE_BACKUP_SERVER] Request ID: (%u, %u); Count: %u (%u:%u)",
		event.instanceId, event.requestId, header.count, header.addr, header.port
	);

	uint32_t remainingChunks, remainingKeys, totalChunks, totalKeys;
	double elapsedTime;
	ServerSocket *original;

	if ( ! CoordinatorWorker::pending->eraseRecovery( event.instanceId, event.requestId, header.addr, header.port, header.chunkCount, header.unsealedCount, event.socket, remainingChunks, totalChunks, remainingKeys, totalKeys, elapsedTime, original ) ) {
		__ERROR__( "ServerWorker", "handlePromoteBackupServerResponse", "Cannot find a pending RECOVERY request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	if ( remainingChunks == 0 && remainingKeys == 0 ) {
		__INFO__( CYAN, "CoordinatorWorker", "handlePromoteBackupServerResponse", "Recovery is completed. Number of chunks reconstructed = %u; number of keys reconstructed = %u; elapsed time = %lf s.\n", totalChunks, totalKeys, elapsedTime );
		event.socket->printAddress();

		event.ackCompletedReconstruction( event.socket, event.instanceId, event.requestId, true );
		this->dispatch( event );

		// notify the remap message handler of a "removed" server
		Coordinator *coordinator = Coordinator::getInstance();
		if ( coordinator->stateTransitHandler )
			coordinator->stateTransitHandler->removeAliveServer( original->getAddr() );

		Log log;
		log.setRecovery(
			header.addr,
			header.port,
			header.chunkCount,
			header.unsealedCount,
			elapsedTime
		);
		Coordinator::getInstance()->appendLog( log );

		// Start recovery
		ServerSocket *failedServerSocket = 0;
		LOCK( &coordinator->waitingForRecovery.lock );
		coordinator->waitingForRecovery.isRecovering = false;
		if ( coordinator->waitingForRecovery.sockets.size() ) {
			failedServerSocket = coordinator->waitingForRecovery.sockets[ 0 ];
			coordinator->waitingForRecovery.sockets.erase( coordinator->waitingForRecovery.sockets.begin() );
		}
		UNLOCK( &coordinator->waitingForRecovery.lock );

		// system( "ssh testbed-node10 'screen -S experiment -p 0 -X stuff \"$(printf '\r')\"'" );

		if ( failedServerSocket ) {
			event.handleReconstructionRequest( failedServerSocket );
			// Must use another worker thread as the function call handleReconstructionRequest() blocks while waiting for the response from other servers
			coordinator->eventQueue.insert( event );
		}
	}

	return true;
}

bool CoordinatorWorker::handleReconstructionRequest( ServerSocket *socket ) {
	int index = CoordinatorWorker::stripeList->search( socket );
	if ( index == -1 ) {
		__ERROR__( "CoordinatorWorker", "handleReconstructionRequest", "The disconnected server does not exist in the consistent hash ring.\n" );
		return false;
	}

	struct timespec startTime = start_timer();

	//////////////////////////////////////////////////////////////////////
	// Choose a backup server socket for reconstructing the failed node //
	//////////////////////////////////////////////////////////////////////
	Coordinator *coordinator = Coordinator::getInstance();
	ArrayMap<int, ServerSocket> &servers = coordinator->sockets.servers;
	ArrayMap<int, ServerSocket> &backupServers = coordinator->sockets.backupServers;
	int fd;
	ServerSocket *backupServerSocket;

	//////////////////////////////////////////////
	// Check if there is other ongoing recovery //
	//////////////////////////////////////////////
	LOCK( &coordinator->waitingForRecovery.lock );
	if ( coordinator->waitingForRecovery.isRecovering ) {
		coordinator->waitingForRecovery.sockets.push_back( socket );
		UNLOCK( &coordinator->waitingForRecovery.lock );
		return false;
	} else {
		coordinator->waitingForRecovery.isRecovering = true;
	}
	UNLOCK( &coordinator->waitingForRecovery.lock );

	/////////////////////////////////////////////////////////
	// Choose a backup server to replace the failed server //
	/////////////////////////////////////////////////////////
	if ( backupServers.size() == 0 ) {
		__ERROR__( "CoordinatorWorker", "handleReconstructionRequest", "No backup node is available!" );
		return false;
	}
	backupServerSocket = backupServers[ 0 ];
	backupServers.removeAt( 0 );

	/////////////////////////////
	// Update ServerSocket map //
	/////////////////////////////
	fd = backupServerSocket->getSocket();
	backupServerSocket->failed = socket;
	servers.set( index, fd, backupServerSocket );

	/////////////////////////////////////////////////
	// Add the server addrs to stateTransitHandler //
	/////////////////////////////////////////////////
	if ( coordinator->stateTransitHandler )
		coordinator->stateTransitHandler->addAliveServer( backupServerSocket->getAddr() );

	/////////////////////////////
	// Announce to the servers //
	/////////////////////////////
	struct {
		pthread_mutex_t lock;
		pthread_cond_t cond;
		std::unordered_set<ServerSocket *> sockets;
	} annoucement;
	ServerEvent serverEvent;

	pthread_mutex_init( &annoucement.lock, 0 );
	pthread_cond_init( &annoucement.cond, 0 );
	serverEvent.announceServerReconstructed(
		Coordinator::instanceId, CoordinatorWorker::idGenerator->nextVal( this->workerId ),
		&annoucement.lock, &annoucement.cond, &annoucement.sockets,
		socket, backupServerSocket
	);
	this->dispatch( serverEvent );

	pthread_mutex_lock( &annoucement.lock );
	while( annoucement.sockets.size() )
		pthread_cond_wait( &annoucement.cond, &annoucement.lock );
	pthread_mutex_unlock( &annoucement.lock );
	// __INFO__( "CoordinatorWorker", "handleReconstructionRequest", "All announced." );

	/////////////////////////////
	// Announce to the clients //
	/////////////////////////////
	ClientEvent clientEvent;
	clientEvent.announceServerReconstructed( socket, backupServerSocket );
	coordinator->eventQueue.insert( clientEvent );

	////////////////////////////////////////////////////////////////////////////

	uint32_t numLostChunks = 0, numLostUnsealedKeys = 0, listId, stripeId, chunkId, requestId = 0;
	bool connected, isCompleted, isAllCompleted;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	std::unordered_set<Metadata>::iterator chunksIt;
	std::unordered_map<Key, OpMetadata>::iterator keysIt;
	std::unordered_map<uint32_t, std::unordered_set<uint32_t>> stripeIds;
	std::unordered_map<uint32_t, std::unordered_set<uint32_t>>::iterator stripeIdsIt;
	std::unordered_set<uint32_t>::iterator stripeIdSetIt;
	std::unordered_map<uint32_t, ServerSocket **> sockets;
	std::unordered_map<uint32_t, std::unordered_set<Key>> unsealed; // List ID |-> Key
	std::unordered_map<uint32_t, std::unordered_set<Key>>::iterator unsealedIt;
	std::unordered_set<Key> unsealedKeysAggregated;
	std::unordered_set<Key>::iterator unsealedKeysIt;

	std::vector<StripeListIndex> lists = CoordinatorWorker::stripeList->list( ( uint32_t ) index );

	ArrayMap<int, ServerSocket> &map = Coordinator::getInstance()->sockets.servers;

	CoordinatorStateTransitHandler *csth = CoordinatorStateTransitHandler::getInstance();

	////////////////////////////////////////////////////
	// Get the ServerSockets of the surviving servers //
	////////////////////////////////////////////////////
	LOCK( &map.lock );
	for ( uint32_t i = 0, size = lists.size(); i < size; i++ ) {
		listId = lists[ i ].listId;
		if ( sockets.find( listId ) == sockets.end() ) {
			ServerSocket **s = new ServerSocket*[ CoordinatorWorker::chunkCount ];
			CoordinatorWorker::stripeList->get(
				listId, s + CoordinatorWorker::dataChunkCount, s
			);

			for ( uint32_t j = 0; j < CoordinatorWorker::chunkCount; j++ ) {
				if ( ! s[ j ] || ! s[ j ]->ready() || csth->allowRemapping( s[ j ]->getAddr() ) )
					s[ j ] = 0; // Don't use this server
			}

			sockets[ listId ] = s;
		}
	}
	UNLOCK( &map.lock );

	LOCK( &socket->map.chunksLock );
	LOCK( &socket->map.keysLock );

	///////////////////////////////////////
	// Prepare the list of sealed chunks //
	///////////////////////////////////////
	for ( chunksIt = socket->map.chunks.begin(); chunksIt != socket->map.chunks.end(); chunksIt++ ) {
		listId = chunksIt->listId;
		stripeId = chunksIt->stripeId;

		stripeIdsIt = stripeIds.find( listId );
		if ( stripeIdsIt == stripeIds.end() ) {
			std::unordered_set<uint32_t> ids;
			ids.insert( stripeId );
			stripeIds[ listId ] = ids;
		} else {
			stripeIdsIt->second.insert( stripeId );
		}
		numLostChunks++;
	}
	assert( numLostChunks == socket->map.chunks.size() );

	///////////////////////////////////////
	// Prepare the list of unsealed keys //
	///////////////////////////////////////
	for ( keysIt = socket->map.keys.begin(); keysIt != socket->map.keys.end(); keysIt++ ) {
		const Key &key = keysIt->first;
		OpMetadata &opMetadata = keysIt->second;

		chunksIt = socket->map.chunks.find( opMetadata );
		if ( chunksIt == socket->map.chunks.end() ) {
			unsealedIt = unsealed.find( opMetadata.listId );
			if ( unsealedIt == unsealed.end() ) {
				std::pair<uint32_t, std::unordered_set<Key>> p( opMetadata.listId, std::unordered_set<Key>() );
				std::pair<std::unordered_map<uint32_t, std::unordered_set<Key>>::iterator, bool> r = unsealed.insert( p );
				unsealedIt = r.first;
			}
			unsealedKeysIt = unsealedKeysAggregated.find( key );
			if ( unsealedKeysIt == unsealedKeysAggregated.end() ) {
				unsealedIt->second.insert( key );
				unsealedKeysAggregated.insert( key );
				numLostUnsealedKeys++;
			}
			opMetadata.stripeId = -1; // Reset stripe ID
		}
	}
	printf( "Number of unsealed chunks: %lu\n", unsealed.size() );

	//////////////////////////////
	// Promote the backup server //
	//////////////////////////////
	requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );
	chunksIt = socket->map.chunks.begin();
	unsealedKeysIt = unsealedKeysAggregated.begin();

	buffer.data = this->protocol.buffer.send;
	ServerAddr srcAddr = socket->getServerAddr();
	do {
		buffer.size = this->protocol.generatePromoteBackupServerHeader(
			PROTO_MAGIC_ANNOUNCEMENT,
			PROTO_MAGIC_TO_SERVER,
			PROTO_OPCODE_BACKUP_SERVER_PROMOTED,
			Coordinator::instanceId, requestId,
			srcAddr.addr,
			srcAddr.port,
			socket->map.chunks, chunksIt,
			unsealedKeysAggregated, unsealedKeysIt,
			isCompleted
		);

		ret = backupServerSocket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size ) {
			__ERROR__( "CoordinatorWorker", "handleReconstructionRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
			break;
		}
	} while ( ! isCompleted );

	// Insert into pending recovery map
	if ( ! CoordinatorWorker::pending->insertRecovery(
		Coordinator::instanceId,
		requestId,
		srcAddr.addr,
		srcAddr.port,
		socket->map.chunks.size(),
		numLostUnsealedKeys,
		startTime,
		backupServerSocket,
		socket
	) ) {
		__ERROR__( "CoordinatorWorker", "handleReconstructionRequest", "Cannot insert into pending recovery map." );
	}

	//////////////////////////////////////////////////////////////////
	// Distribute the reconstruction tasks to the surviving servers //
	//////////////////////////////////////////////////////////////////
	// Distribute the reconstruction task among the servers in the same stripe list
	for ( uint32_t i = 0, size = lists.size(); i < size; i++ ) {
		uint32_t numSurvivingServers = 0;
		uint32_t numStripePerServer;
		uint32_t numUnsealedKeys;

		listId = lists[ i ].listId;
		chunkId = lists[ i ].chunkId;

		if ( chunkId >= CoordinatorWorker::dataChunkCount ) {
			LOCK( &Map::stripesLock );
			// Update stripeIds for parity server
			stripeIdsIt = stripeIds.find( listId );
			if ( stripeIdsIt == stripeIds.end() ) {
				std::unordered_set<uint32_t> ids;
				for ( uint32_t j = 0; j < Map::stripes[ listId ]; j++ )
					ids.insert( j );
				stripeIds[ listId ] = ids;
			} else {
				for ( uint32_t j = 0; j < Map::stripes[ listId ]; j++ )
					stripeIdsIt->second.insert( j );
			}
			UNLOCK( &Map::stripesLock );
		}

		for ( uint32_t j = 0; j < CoordinatorWorker::chunkCount; j++ ) {
			ServerSocket *s = sockets[ listId ][ j ];
			if ( s && s->ready() && s != backupServerSocket )
				numSurvivingServers++;
		}

		numStripePerServer = stripeIds[ listId ].size() / numSurvivingServers;
		if ( stripeIds[ listId ].size() % numSurvivingServers > 0 )
			numStripePerServer++;

		// Insert into pending map
		pthread_mutex_t *lock;
		pthread_cond_t *cond;

		requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );

		CoordinatorWorker::pending->insertReconstruction(
			Coordinator::instanceId,
			requestId,
			listId, chunkId,
			stripeIds[ listId ],
			unsealed[ listId ],
			lock, cond
		);

		// Distribute the task
		stripeIdSetIt = stripeIds[ listId ].begin();
		unsealedKeysIt = unsealed[ listId ].begin();
		buffer.data = this->protocol.buffer.send;
		do {
			isAllCompleted = true;
			// Task to all servers: Reconstruct sealed chunks
			for ( uint32_t j = 0; j < CoordinatorWorker::chunkCount; j++ ) {
				if ( stripeIdSetIt == stripeIds[ listId ].end() )
					break;

				ServerSocket *s = sockets[ listId ][ j ];
				if ( s && s->ready() && s != backupServerSocket ) {
					buffer.size = this->protocol.generateReconstructionHeader(
						PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
						PROTO_OPCODE_RECONSTRUCTION,
						Coordinator::instanceId, requestId,
						listId, chunkId,
						stripeIds[ listId ], stripeIdSetIt,
						numStripePerServer,
						isCompleted
					);
					ret = s->send( buffer.data, buffer.size, connected );
					if ( ret != ( ssize_t ) buffer.size )
						__ERROR__( "CoordinatorWorker", "handleReconstructionRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );

					// Avoid generating too many requests
					pthread_mutex_lock( lock );
					pthread_cond_wait( cond, lock );
					pthread_mutex_unlock( lock );

					isAllCompleted &= isCompleted;
				}
			}
		} while ( ! isAllCompleted );

		do {
			isAllCompleted = true;
			// Task to parity servers: Send unsealed keys
			for ( uint32_t j = 0; j < CoordinatorWorker::parityChunkCount; j++ ) {
				if ( unsealedKeysIt == unsealed[ listId ].end() )
					break;

				ServerSocket *s = sockets[ listId ][ CoordinatorWorker::dataChunkCount + j ];
				if ( s && s->ready() && s != backupServerSocket ) {
					buffer.size = this->protocol.generateBatchKeyHeader(
						PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
						PROTO_OPCODE_RECONSTRUCTION_UNSEALED,
						Coordinator::instanceId, requestId,
						unsealed[ listId ], unsealedKeysIt, numUnsealedKeys,
						isCompleted
					);
					isAllCompleted &= isCompleted;

					ret = s->send( buffer.data, buffer.size, connected );
					if ( ret != ( ssize_t ) buffer.size )
						__ERROR__( "CoordinatorWorker", "handleReconstructionRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );

					// Avoid generating too many requests
					pthread_mutex_lock( lock );
					pthread_cond_wait( cond, lock );
					pthread_mutex_unlock( lock );

					isAllCompleted &= isCompleted;
				}
			}
		} while ( ! isAllCompleted );

		__INFO__(
			YELLOW, "CoordinatorWorker", "handleReconstructionRequest",
			"[%u] (%u, %u): Number of surviving servers: %u; number of stripes per server: %u; total number of stripes: %lu; total number of unsealed keys: %lu",
			requestId, listId, chunkId, numSurvivingServers, numStripePerServer, stripeIds[ listId ].size(), unsealed[ listId ].size()
		);
	}

	UNLOCK( &socket->map.keysLock );
	UNLOCK( &socket->map.chunksLock );

	printf( "Number of chunks that need to be recovered: %u\n", numLostChunks );

	return true;
}

bool CoordinatorWorker::handleReconstructionResponse( ServerEvent event, char *buf, size_t size ) {
	struct ReconstructionHeader header;
	if ( ! this->protocol.parseReconstructionHeader( header, false /* isRequest */, buf, size ) ) {
		__ERROR__( "CoordinatorWorker", "handleReconstructionResponse", "Invalid RECONSTRUCTION response (size = %lu).", size );
		return false;
	}

	uint32_t remainingChunks, remainingKeys;
	if ( ! CoordinatorWorker::pending->eraseReconstruction( event.instanceId, event.requestId, header.listId, header.chunkId, header.numStripes, 0, remainingChunks, remainingKeys ) ) {
		__ERROR__( "CoordinatorWorker", "handleReconstructionResponse", "The response does not match with the request!" );
		return false;
	}

	__DEBUG__(
		BLUE, "CoordinatorWorker", "handleReconstructionResponse",
		"[RECONSTRUCTION] Request ID: (%u, %u); list ID: %u, chunk Id: %u, number of stripes: %u (%s)",
		event.instanceId, event.requestId, header.listId, header.chunkId, header.numStripes,
		remainingChunks == 0 && remainingKeys == 0 ? "Done" : "In progress"
	);

	return true;
}

bool CoordinatorWorker::handleReconstructionUnsealedResponse( ServerEvent event, char *buf, size_t size ) {
	struct ReconstructionHeader header;
	if ( ! this->protocol.parseReconstructionHeader( header, false /* isRequest */, buf, size ) ) {
		__ERROR__( "CoordinatorWorker", "handleReconstructionUnsealedResponse", "Invalid RECONSTRUCTION_UNSEALED response (size = %lu).", size );
		return false;
	}

	// Determine the list ID and chunk ID
	uint32_t remainingChunks, remainingKeys;
	if ( ! CoordinatorWorker::pending->eraseReconstruction( event.instanceId, event.requestId, header.listId, header.chunkId, 0, header.numStripes, remainingChunks, remainingKeys ) ) {
		__ERROR__( "CoordinatorWorker", "handleReconstructionUnsealedResponse", "The response does not match with the request!" );
		return false;
	}

	__INFO__(
		BLUE, "CoordinatorWorker", "handleReconstructionUnsealedResponse",
		"[RECONSTRUCTION] Request ID: (%u, %u); list ID: %u, chunk Id: %u, number of unsealed keys: %u (%s)",
		event.instanceId, event.requestId, header.listId, header.chunkId, header.numStripes,
		remainingChunks == 0 && remainingKeys == 0 ? "Done" : "In progress"
	);

	return true;
}
