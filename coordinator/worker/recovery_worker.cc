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
		"[PROMOTE_BACKUP_SLAVE] Request ID: %u; Count: %u (%u:%u)",
		event.id, header.count, header.addr, header.port
	);

	uint32_t remaining, total;
	double elapsedTime;

	if ( ! CoordinatorWorker::pending->eraseRecovery( event.id, header.addr, header.port, header.count, event.socket, remaining, total, elapsedTime ) ) {
		__ERROR__( "SlaveWorker", "handlePromoteBackupSlaveResponse", "Cannot find a pending RECOVERY request that matches the response. This message will be discarded. (ID: %u)", event.id );
		return false;
	}

	if ( remaining == 0 ) {
		__INFO__( CYAN, "CoordinatorWorker", "handlePromoteBackupSlaveResponse", "Recovery is completed. Number of chunks reconstructed = %u; elapsed time = %lf s.\n", total, elapsedTime );

		Log log;
		log.setRecovery(
			header.addr,
			header.port,
			header.count,
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
	ArrayMap<int, SlaveSocket> &slaves = Coordinator::getInstance()->sockets.slaves;
	ArrayMap<int, SlaveSocket> &backupSlaves = Coordinator::getInstance()->sockets.backupSlaves;
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

	////////////////////////////
	// Announce to the slaves //
	////////////////////////////
	SlaveEvent slaveEvent;
	slaveEvent.announceSlaveReconstructed( socket, backupSlaveSocket );
	CoordinatorWorker::eventQueue->insert( slaveEvent );

	////////////////////////////////////////////////////////////////////////////

	uint32_t numLostChunks = 0, listId, stripeId, chunkId, requestId = 0;
	std::set<Metadata> unsealedChunks;
	bool connected, isCompleted, isAllCompleted;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	std::unordered_set<Metadata>::iterator chunksIt;
	std::unordered_map<uint32_t, std::unordered_set<uint32_t>> stripeIds;
	std::unordered_map<uint32_t, std::unordered_set<uint32_t>>::iterator stripeIdsIt;
	std::unordered_set<uint32_t>::iterator stripeIdSetIt;
	std::unordered_map<uint32_t, SlaveSocket **> sockets;

	std::vector<StripeListIndex> lists = CoordinatorWorker::stripeList->list( ( uint32_t ) index );

	ArrayMap<int, SlaveSocket> &map = Coordinator::getInstance()->sockets.slaves;

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
			sockets[ listId ] = s;
		}
	}
	UNLOCK( &map.lock );

	LOCK( &socket->map.chunksLock );
	LOCK( &socket->map.keysLock );

	//////////////////////////////
	// Promote the backup slave //
	//////////////////////////////
	requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );
	chunksIt = socket->map.chunks.begin();
	do {
		buffer.data = this->protocol.promoteBackupSlave(
			buffer.size,
			requestId,
			socket,
			socket->map.chunks,
			chunksIt,
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
		requestId,
		srcAddr.addr,
		srcAddr.port,
		socket->map.chunks.size(),
		startTime,
		backupSlaveSocket
	) ) {
		__ERROR__( "CoordinatorWorker", "handleReconstructionRequest", "Cannot insert into pending recovery map." );
	}

	/////////////////////////////////////////////////////////////////
	// Distribute the reconstruction tasks to the surviving slaves //
	/////////////////////////////////////////////////////////////////
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
	// Distribute the reconstruction task among the slaves in the same stripe list
	for ( uint32_t i = 0, size = lists.size(); i < size; i++ ) {
		uint32_t numSurvivingSlaves = 0;
		uint32_t numStripePerSlave;

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
			if ( s->ready() && s != backupSlaveSocket )
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
			requestId,
			listId, chunkId, stripeIds[ listId ],
			lock, cond
		);

		// Distribute the task
		stripeIdSetIt = stripeIds[ listId ].begin();
		do {
			isAllCompleted = true;
			for ( uint32_t j = 0; j < CoordinatorWorker::chunkCount; j++ ) {
				if ( stripeIdSetIt == stripeIds[ listId ].end() )
					break;

				SlaveSocket *s = sockets[ listId ][ j ];
				if ( s->ready() && s != backupSlaveSocket ) {
					buffer.data = this->protocol.reqReconstruction(
						buffer.size,
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

					pthread_mutex_lock( lock );
					pthread_cond_wait( cond, lock );
					pthread_mutex_unlock( lock );

					isAllCompleted &= isCompleted;
				}
			}
		} while ( ! isAllCompleted );

		__INFO__( YELLOW, "CoordinatorWorker", "handleReconstructionRequest", "[%u] (%u, %u): Number of surviving slaves: %u; number of stripes per slave: %u; total number of stripes: %lu", requestId, listId, chunkId, numSurvivingSlaves, numStripePerSlave, stripeIds[ listId ].size() );
	}

	////////////////////////////
	// Handle unsealed chunks //
	////////////////////////////
	// TODO

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

	uint32_t remaining;
	if ( ! CoordinatorWorker::pending->eraseReconstruction( event.id, header.listId, header.chunkId, header.numStripes, remaining ) ) {
		__ERROR__( "CoordinatorWorker", "handleReconstructionResponse", "The response does not match with the request!" );
		return false;
	}

	__DEBUG__(
		BLUE, "CoordinatorWorker", "handleReconstructionResponse",
		"[RECONSTRUCTION] Request ID: %u; list ID: %u, chunk Id: %u, number of stripes: %u (%s)",
		event.id, header.listId, header.chunkId, header.numStripes,
		remaining == 0 ? "Done" : "In progress"
	);

	return true;
}
