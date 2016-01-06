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

	uint32_t remaining, total;
	double elapsedTime;
	SlaveSocket *original;

	if ( ! CoordinatorWorker::pending->eraseRecovery( event.instanceId, event.requestId, header.addr, header.port, header.count, event.socket, remaining, total, elapsedTime, original ) ) {
		__ERROR__( "SlaveWorker", "handlePromoteBackupSlaveResponse", "Cannot find a pending RECOVERY request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	if ( remaining == 0 ) {
		__INFO__( CYAN, "CoordinatorWorker", "handlePromoteBackupSlaveResponse", "Recovery is completed. Number of chunks reconstructed = %u; elapsed time = %lf s.\n", total, elapsedTime );

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
			header.count,
			elapsedTime
		);
		Coordinator::getInstance()->appendLog( log );
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
	coordinator->remapMsgHandler->addAliveSlave( backupSlaveSocket->getAddr() );

	////////////////////////////
	// Announce to the slaves //
	////////////////////////////
	SlaveEvent slaveEvent;
	slaveEvent.announceSlaveReconstructed( socket, backupSlaveSocket );
	CoordinatorWorker::eventQueue->insert( slaveEvent );

	/////////////////////////////
	// Announce to the masters //
	/////////////////////////////
	MasterEvent masterEvent;
	masterEvent.announceSlaveReconstructed( socket, backupSlaveSocket );
	CoordinatorWorker::eventQueue->insert( masterEvent );

	////////////////////////////////////////////////////////////////////////////

	uint32_t numLostChunks = 0, listId, stripeId, chunkId, requestId = 0;
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
			Coordinator::instanceId,
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
		Coordinator::instanceId,
		requestId,
		srcAddr.addr,
		srcAddr.port,
		socket->map.chunks.size(),
		startTime,
		backupSlaveSocket,
		socket
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
			Coordinator::instanceId,
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
	// Construct the set of keys in unsealed chunks
	std::map<Metadata, std::vector<Key>> unsealed;
	std::map<Metadata, std::vector<Key>>::iterator unsealedIt;

	printf( "socket->map.keys.size = %lu\n", socket->map.keys.size() );
	for ( keysIt = socket->map.keys.begin(); keysIt != socket->map.keys.end(); keysIt++ ) {
		const Key &key = keysIt->first;
		const OpMetadata &opMetadata = keysIt->second;

		chunksIt = socket->map.chunks.find( opMetadata );
		if ( chunksIt == socket->map.chunks.end() ) {
			unsealedIt = unsealed.find( opMetadata );
			if ( unsealedIt == unsealed.end() ) {
				std::pair<Metadata, std::vector<Key>> p( opMetadata, std::vector<Key>() );
				std::pair<std::map<Metadata, std::vector<Key>>::iterator, bool> r = unsealed.insert( p );
				unsealedIt = r.first;
			}
			unsealedIt->second.push_back( key );
		}
	}
	printf( "Number of unsealed chunks: %lu\n", unsealed.size() );
	for ( unsealedIt = unsealed.begin(); unsealedIt != unsealed.end(); unsealedIt++ ) {
		// printf(
		// 	"- (%u, %u, %u): %lu keys\n",
		// 	unsealedIt->first.listId,
		// 	unsealedIt->first.stripeId,
		// 	unsealedIt->first.chunkId,
		// 	unsealedIt->second.size()
		// );
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

	uint32_t remaining;
	if ( ! CoordinatorWorker::pending->eraseReconstruction( event.instanceId, event.requestId, header.listId, header.chunkId, header.numStripes, remaining ) ) {
		__ERROR__( "CoordinatorWorker", "handleReconstructionResponse", "The response does not match with the request!" );
		return false;
	}

	__DEBUG__(
		BLUE, "CoordinatorWorker", "handleReconstructionResponse",
		"[RECONSTRUCTION] Request ID: (%u, %u); list ID: %u, chunk Id: %u, number of stripes: %u (%s)",
		event.instanceId, event.requestId, header.listId, header.chunkId, header.numStripes,
		remaining == 0 ? "Done" : "In progress"
	);

	return true;
}
