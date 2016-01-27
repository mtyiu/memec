#include "worker.hh"
#include "../ds/log.hh"
#include "../main/coordinator.hh"

bool CoordinatorWorker::handlePromoteBackupSlaveResponse( SlaveEvent event, char *buf, size_t size ) {
	struct PromoteBackupSlaveHeader header;
	if ( ! this->protocol.parsePromoteBackupSlaveHeader( header, false /* isRequest */, buf, size ) ) {
		__ERROR__( "CoordinatorWorker", "handlePromoteBackupSlaveResponse", "Invalid PROMOTE_BACKUP_SLAVE response (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "CoordinatorWorker", "handlePromoteBackupSlaveResponse",
		"[PROMOTE_BACKUP_SLAVE] Request ID: (%u, %u); Count: %u (%u:%u)",
		event.instanceId, event.requestId, header.count, header.addr, header.port
	);

	uint32_t remainingChunks, remainingKeys, totalChunks, totalKeys;
	double elapsedTime;
	SlaveSocket *original;

	if ( ! CoordinatorWorker::pending->eraseRecovery( event.instanceId, event.requestId, header.addr, header.port, header.chunkCount, header.unsealedCount, event.socket, remainingChunks, totalChunks, remainingKeys, totalKeys, elapsedTime, original ) ) {
		__ERROR__( "SlaveWorker", "handlePromoteBackupSlaveResponse", "Cannot find a pending RECOVERY request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	if ( remainingChunks == 0 && remainingKeys == 0 ) {
		__INFO__( CYAN, "CoordinatorWorker", "handlePromoteBackupSlaveResponse", "Recovery is completed. Number of chunks reconstructed = %u; number of keys reconstructed = %u; elapsed time = %lf s.\n", totalChunks, totalKeys, elapsedTime );
		event.socket->printAddress();

		event.ackCompletedReconstruction( event.socket, event.instanceId, event.requestId, true );
		this->dispatch( event );

		// notify the remap message handler of a "removed" slave
		Coordinator *coordinator = Coordinator::getInstance();
		if ( coordinator->remapMsgHandler )
			coordinator->remapMsgHandler->removeAliveSlave( original->getAddr() );

		Log log;
		log.setRecovery(
			header.addr,
			header.port,
			header.chunkCount,
			header.unsealedCount,
			elapsedTime
		);
		Coordinator::getInstance()->appendLog( log );

		// system( "ssh testbed-node10 'screen -S experiment -p 0 -X stuff \"$(printf '\r')\"'" );
	}

	return true;
}

bool CoordinatorWorker::handleReconstructionRequest( SlaveSocket *socket ) {
	int index = CoordinatorWorker::stripeList->search( socket );
	if ( index == -1 ) {
		__ERROR__( "CoordinatorWorker", "handleReconstructionRequest", "The disconnected server does not exist in the consistent hash ring.\n" );
		return false;
	}

	struct timespec startTime = start_timer();

	/////////////////////////////////////////////////////////////////////
	// Choose a backup slave socket for reconstructing the failed node //
	/////////////////////////////////////////////////////////////////////
	Coordinator *coordinator = Coordinator::getInstance();
	ArrayMap<int, SlaveSocket> &slaves = coordinator->sockets.slaves;
	ArrayMap<int, SlaveSocket> &backupSlaves = coordinator->sockets.backupSlaves;
	int fd;
	SlaveSocket *backupSlaveSocket;

	///////////////////////////////////////////////////////
	// Choose a backup slave to replace the failed slave //
	///////////////////////////////////////////////////////
	if ( backupSlaves.size() == 0 ) {
		__ERROR__( "CoordinatorWorker", "handleReconstructionRequest", "No backup node is available!" );
		return false;
	}
	backupSlaveSocket = backupSlaves[ 0 ];
	backupSlaves.removeAt( 0 );

	////////////////////////////
	// Update SlaveSocket map //
	////////////////////////////
	fd = backupSlaveSocket->getSocket();
	backupSlaveSocket->failed = socket;
	slaves.set( index, fd, backupSlaveSocket );

	////////////////////////////////////////////
	// Add the slave addrs to remapMsgHandler //
	////////////////////////////////////////////
	if ( coordinator->remapMsgHandler )
		coordinator->remapMsgHandler->addAliveSlave( backupSlaveSocket->getAddr() );

	////////////////////////////
	// Announce to the slaves //
	////////////////////////////
	struct {
		pthread_mutex_t lock;
		pthread_cond_t cond;
		std::unordered_set<SlaveSocket *> sockets;
	} annoucement;
	SlaveEvent slaveEvent;

	pthread_mutex_init( &annoucement.lock, 0 );
	pthread_cond_init( &annoucement.cond, 0 );
	slaveEvent.announceSlaveReconstructed(
		Coordinator::instanceId, CoordinatorWorker::idGenerator->nextVal( this->workerId ),
		&annoucement.lock, &annoucement.cond, &annoucement.sockets,
		socket, backupSlaveSocket
	);
	this->dispatch( slaveEvent );

	pthread_mutex_lock( &annoucement.lock );
	while( annoucement.sockets.size() )
		pthread_cond_wait( &annoucement.cond, &annoucement.lock );
	pthread_mutex_unlock( &annoucement.lock );
	printf( "~~~ All announced ~~~\n" );

	/////////////////////////////
	// Announce to the masters //
	/////////////////////////////
	MasterEvent masterEvent;
	masterEvent.announceSlaveReconstructed( socket, backupSlaveSocket );
	CoordinatorWorker::eventQueue->insert( masterEvent );

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
	std::unordered_map<uint32_t, SlaveSocket **> sockets;
	std::unordered_map<uint32_t, std::unordered_set<Key>> unsealed; // List ID |-> Key
	std::unordered_map<uint32_t, std::unordered_set<Key>>::iterator unsealedIt;
	std::unordered_set<Key> unsealedKeysAggregated;
	std::unordered_set<Key>::iterator unsealedKeysIt;

	std::vector<StripeListIndex> lists = CoordinatorWorker::stripeList->list( ( uint32_t ) index );

	ArrayMap<int, SlaveSocket> &map = Coordinator::getInstance()->sockets.slaves;

	CoordinatorRemapMsgHandler *crmh = CoordinatorRemapMsgHandler::getInstance();

	//////////////////////////////////////////////////
	// Get the SlaveSockets of the surviving slaves //
	//////////////////////////////////////////////////
	LOCK( &map.lock );
	for ( uint32_t i = 0, size = lists.size(); i < size; i++ ) {
		listId = lists[ i ].listId;
		if ( sockets.find( listId ) == sockets.end() ) {
			SlaveSocket **s = new SlaveSocket*[ CoordinatorWorker::chunkCount ];
			CoordinatorWorker::stripeList->get(
				listId, s + CoordinatorWorker::dataChunkCount, s
			);

			for ( uint32_t j = 0; j < CoordinatorWorker::chunkCount; j++ ) {
				if ( ! s[ j ] || ! s[ j ]->ready() || crmh->allowRemapping( s[ j ]->getAddr() ) )
					s[ j ] = 0; // Don't use this slave
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
	// Promote the backup slave //
	//////////////////////////////
	requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );
	chunksIt = socket->map.chunks.begin();
	unsealedKeysIt = unsealedKeysAggregated.begin();
	do {
		buffer.data = this->protocol.promoteBackupSlave(
			buffer.size,
			Coordinator::instanceId,
			requestId,
			socket,
			socket->map.chunks, chunksIt,
			unsealedKeysAggregated, unsealedKeysIt,
			isCompleted
		);

		ret = backupSlaveSocket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size ) {
			__ERROR__( "CoordinatorWorker", "handleReconstructionRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
			break;
		}
	} while ( ! isCompleted );

	// Insert into pending recovery map
	ServerAddr srcAddr = socket->getServerAddr();
	if ( ! CoordinatorWorker::pending->insertRecovery(
		Coordinator::instanceId,
		requestId,
		srcAddr.addr,
		srcAddr.port,
		socket->map.chunks.size(),
		numLostUnsealedKeys,
		startTime,
		backupSlaveSocket,
		socket
	) ) {
		__ERROR__( "CoordinatorWorker", "handleReconstructionRequest", "Cannot insert into pending recovery map." );
	}

	/////////////////////////////////////////////////////////////////
	// Distribute the reconstruction tasks to the surviving slaves //
	/////////////////////////////////////////////////////////////////
	// Distribute the reconstruction task among the slaves in the same stripe list
	for ( uint32_t i = 0, size = lists.size(); i < size; i++ ) {
		uint32_t numSurvivingSlaves = 0;
		uint32_t numStripePerSlave;
		uint32_t numUnsealedKeys;

		listId = lists[ i ].listId;
		chunkId = lists[ i ].chunkId;

		if ( chunkId >= CoordinatorWorker::dataChunkCount ) {
			LOCK( &Map::stripesLock );
			// Update stripeIds for parity slave
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
			SlaveSocket *s = sockets[ listId ][ j ];
			if ( s && s->ready() && s != backupSlaveSocket )
				numSurvivingSlaves++;
		}

		numStripePerSlave = stripeIds[ listId ].size() / numSurvivingSlaves;
		if ( stripeIds[ listId ].size() % numSurvivingSlaves > 0 )
			numStripePerSlave++;

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
		do {
			isAllCompleted = true;
			// Task to all slaves: Reconstruct sealed chunks
			for ( uint32_t j = 0; j < CoordinatorWorker::chunkCount; j++ ) {
				if ( stripeIdSetIt == stripeIds[ listId ].end() )
					break;

				SlaveSocket *s = sockets[ listId ][ j ];
				if ( s && s->ready() && s != backupSlaveSocket ) {
					buffer.data = this->protocol.reqReconstruction(
						buffer.size,
						Coordinator::instanceId,
						requestId,
						listId,
						chunkId,
						stripeIds[ listId ],
						stripeIdSetIt,
						numStripePerSlave,
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
			// Task to parity slaves: Send unsealed keys
			for ( uint32_t j = 0; j < CoordinatorWorker::parityChunkCount; j++ ) {
				if ( unsealedKeysIt == unsealed[ listId ].end() )
					break;

				SlaveSocket *s = sockets[ listId ][ CoordinatorWorker::dataChunkCount + j ];
				if ( s && s->ready() && s != backupSlaveSocket ) {
					buffer.data = this->protocol.reqReconstructionUnsealed(
						buffer.size,
						Coordinator::instanceId,
						requestId,
						unsealed[ listId ],
						unsealedKeysIt,
						numUnsealedKeys,
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
			"[%u] (%u, %u): Number of surviving slaves: %u; number of stripes per slave: %u; total number of stripes: %lu; total number of unsealed keys: %lu",
			requestId, listId, chunkId, numSurvivingSlaves, numStripePerSlave, stripeIds[ listId ].size(), unsealed[ listId ].size()
		);
	}

	UNLOCK( &socket->map.keysLock );
	UNLOCK( &socket->map.chunksLock );

	printf( "Number of chunks that need to be recovered: %u\n", numLostChunks );

	return true;
}

bool CoordinatorWorker::handleReconstructionResponse( SlaveEvent event, char *buf, size_t size ) {
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

bool CoordinatorWorker::handleReconstructionUnsealedResponse( SlaveEvent event, char *buf, size_t size ) {
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
