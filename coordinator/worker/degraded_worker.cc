#include "worker.hh"
#include "../main/coordinator.hh"

bool CoordinatorWorker::handleDegradedLockRequest( ClientEvent event, char *buf, size_t size ) {
	struct DegradedLockReqHeader header;
	if ( ! this->protocol.parseDegradedLockReqHeader( header, buf, size ) ) {
		__ERROR__( "CoordinatorWorker", "handleDegradedLockRequest", "Invalid DEGRADED_LOCK request (size = %lu).", size );
		return false;
	}
	if ( header.isLarge ) {
		__DEBUG__(
			BLUE, "CoordinatorWorker", "handleDegradedLockRequest",
			"[DEGRADED_LOCK] Key: %.*s.%u (key size = %u)%s.",
			( int ) header.keySize, header.key,
			LargeObjectUtil::readSplitOffset( header.key + header.keySize ),
			header.keySize,
			header.isLarge ? "; is large" : ""
		);

		// for ( uint32_t i = 0; i < header.reconstructedCount; i++ ) {
		// 	fprintf(
		// 		stderr,
		// 		"%s(%u, %u) |-> (%u, %u)%s",
		// 		i == 0 ? "Original: " : "; ",
		// 		header.original[ i * 2     ],
		// 		header.original[ i * 2 + 1 ],
		// 		header.reconstructed[ i * 2     ],
		// 		header.reconstructed[ i * 2 + 1 ],
		// 		i == header.reconstructedCount - 1 ? " || " : "\n"
		// 	);
		// }
	} else {
		__DEBUG__(
			BLUE, "CoordinatorWorker", "handleDegradedLockRequest",
			"[DEGRADED_LOCK] Key: %.*s (key size = %u).",
			( int ) header.keySize, header.key, header.keySize
		);
	}

	// Metadata metadata;
	LOCK_T *lock;
	RemappingRecord remappingRecord;
	Key key;
	key.set( header.keySize, header.key, 0, header.isLarge );

	if ( Coordinator::getInstance()->remappingRecords.find( key, &remappingRecord, &lock ) ) {
		// Remapped
		event.resDegradedLock(
			event.socket, event.instanceId, event.requestId, key,
			remappingRecord.original, remappingRecord.remapped, remappingRecord.remappedCount
		);
		this->dispatch( event );
		UNLOCK( lock );
		return false;
	}

	// Find the ServerSocket which stores the stripe with listId and srcDataChunkId
	ServerSocket *socket;
	uint32_t ongoingAtChunk;
	uint32_t listId = CoordinatorWorker::stripeList->get( header.key, header.keySize, &socket, 0, &ongoingAtChunk );
	if ( header.isLarge ) {
		uint32_t splitOffset = LargeObjectUtil::readSplitOffset( header.key + header.keySize );
		uint32_t splitIndex = LargeObjectUtil::getSplitIndex( header.keySize, 0, splitOffset, header.isLarge );
		ongoingAtChunk = ( ongoingAtChunk + splitIndex ) % ( CoordinatorWorker::dataChunkCount );
		socket = CoordinatorWorker::stripeList->get( listId, ongoingAtChunk );
	}
	Map *map = &( socket->map );
	Metadata srcMetadata; // set via findMetadataByKey()
	DegradedLock degradedLock;
	bool ret = true, exist;

	uint8_t numSurvivingChunkIds = 0;
	uint32_t ptr = 0;
	CoordinatorStateTransitHandler *csth = CoordinatorStateTransitHandler::getInstance();
	for ( uint32_t i = 0; i < CoordinatorWorker::chunkCount; i++ ) {
		ServerSocket *s = CoordinatorWorker::stripeList->get( listId, i );
		struct sockaddr_in addr = s->getAddr();
		if ( ! csth->allowRemapping( addr ) ) {
			numSurvivingChunkIds++;
			this->survivingChunkIds[ ptr++ ] = i;
		}
	}

	lock = 0;
	exist = map->findMetadataByKey( header.key, header.keySize, header.isLarge, srcMetadata );

	if ( ! exist ) {
		// Force header.isLarge to be true
		if ( map->findMetadataByKey( header.key, header.keySize, true, srcMetadata ) ) {
			header.isLarge = true;
			key.isLarge = true;
			exist = true;
		}
	}

	if ( ! exist ) {
		// Key not found
		event.resDegradedLock(
			event.socket, event.instanceId, event.requestId,
			key, true
		);
		ret = false;
	} else {
		// Check whether the "failed" servers are in intermediate or degraded state
		for ( uint32_t i = 0; i < header.reconstructedCount; ) {
			ServerSocket *s = CoordinatorWorker::stripeList->get(
				header.original[ i * 2    ],
				header.original[ i * 2 + 1 ]
			);
			struct sockaddr_in addr = s->getAddr();
			if ( ! csth->allowRemapping( addr ) ) {
				for ( uint32_t j = i; j < header.reconstructedCount - 1; j++ ) {
					header.original[ j * 2     ] = header.original[ ( j + 1 ) * 2     ];
					header.original[ j * 2 + 1 ] = header.original[ ( j + 1 ) * 2 + 1 ];
					header.reconstructed[ j * 2     ] = header.reconstructed[ ( j + 1 ) * 2     ];
					header.reconstructed[ j * 2 + 1 ] = header.reconstructed[ ( j + 1 ) * 2 + 1 ];
				}
				header.reconstructedCount--;
			} else if ( header.original[ i * 2 + 1 ] < CoordinatorWorker::dataChunkCount ) {
				// Ignore unsealed chunks
				Metadata tmpMetadata;
				tmpMetadata.set(
					srcMetadata.listId,
					srcMetadata.stripeId,
					header.original[ i * 2 + 1 ]
				);

				if ( ! s->map.isSealed( tmpMetadata ) && ongoingAtChunk != header.original[ i * 2 + 1 ] ) {
					for ( uint32_t j = i; j < header.reconstructedCount - 1; j++ ) {
						header.original[ j * 2     ] = header.original[ ( j + 1 ) * 2     ];
						header.original[ j * 2 + 1 ] = header.original[ ( j + 1 ) * 2 + 1 ];
						header.reconstructed[ j * 2     ] = header.reconstructed[ ( j + 1 ) * 2     ];
						header.reconstructed[ j * 2 + 1 ] = header.reconstructed[ ( j + 1 ) * 2 + 1 ];
					}
					header.reconstructedCount--;
				} else {
					i++;
				}
			} else {
				i++;
			}
		}

		if ( map->findDegradedLock( srcMetadata.listId, srcMetadata.stripeId, degradedLock, true, false, &lock ) ) {
			map->expandDegradedLock(
				srcMetadata.listId, srcMetadata.stripeId,
				header.original, header.reconstructed, header.reconstructedCount,
				ongoingAtChunk,
				numSurvivingChunkIds,
				this->survivingChunkIds,
				CoordinatorWorker::chunkCount,
				degradedLock,
				false, false
			);

			// The chunk is already locked
			event.resDegradedLock(
				event.socket, event.instanceId, event.requestId, key,
				false, // isLocked
				map->isSealed( srcMetadata ), // the chunk is sealed
				srcMetadata.stripeId,
				srcMetadata.chunkId,
				CoordinatorWorker::dataChunkCount,
				degradedLock.original,
				degradedLock.reconstructed,
				degradedLock.reconstructedCount,
				degradedLock.ongoingAtChunk,
				numSurvivingChunkIds,
				this->survivingChunkIds
			);
		} else if ( ! header.reconstructedCount ) {
			// No need to lock
			event.resDegradedLock(
				event.socket, event.instanceId, event.requestId,
				key, true
			);
		} else {
			if ( ! header.reconstructedCount ) {
				event.resDegradedLock(
					event.socket, event.instanceId, event.requestId,
					key, true
				);
			} else {
				for ( uint32_t i = 0; i < header.reconstructedCount; i++ ) {
					if ( ongoingAtChunk == header.original[ i * 2 + 1 ] ) {
						ongoingAtChunk = header.reconstructed[ i * 2 + 1 ];
						break;
					}
				}

				map->insertDegradedLock(
					srcMetadata.listId, srcMetadata.stripeId,
					header.original, header.reconstructed, header.reconstructedCount,
					ongoingAtChunk,
					numSurvivingChunkIds,
					this->survivingChunkIds,
					CoordinatorWorker::chunkCount,
					false, false
				);
				ret = map->findDegradedLock( srcMetadata.listId, srcMetadata.stripeId, degradedLock, false, false );

				if ( ret ) {
					event.resDegradedLock(
						event.socket, event.instanceId, event.requestId, key,
						true, // isLocked
						map->isSealed( srcMetadata ), // the chunk is sealed
						srcMetadata.stripeId,
						srcMetadata.chunkId,
						CoordinatorWorker::dataChunkCount,
						degradedLock.original,
						degradedLock.reconstructed,
						degradedLock.reconstructedCount,
						degradedLock.ongoingAtChunk,
						numSurvivingChunkIds,
						this->survivingChunkIds
					);
				} else {
					// Cannot lock
					event.resDegradedLock(
						event.socket, event.instanceId, event.requestId,
						key, true
					);
				}
			}
		}
	}
	this->dispatch( event );
	if ( lock ) UNLOCK( lock );
	return ret;
}

bool CoordinatorWorker::handleReleaseDegradedLockRequest( ServerSocket *socket, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done ) {
	std::unordered_map<ListStripe, DegradedLock>::iterator dlsIt;
	Map &map = socket->map;
	ServerSocket *dstSocket;
	bool isCompleted, connected;
	uint16_t instanceId;
	uint32_t requestId;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;
	// (dstListId, dstChunkId) |-> (srcListId, srcStripeId, srcChunkId)
	std::unordered_map<Metadata, std::vector<Metadata>> chunks;
	std::unordered_map<Metadata, std::vector<Metadata>>::iterator chunksIt;

	LOCK( &map.degradedLocksLock );
	for ( dlsIt = map.degradedLocks.begin(); dlsIt != map.degradedLocks.end(); dlsIt++ ) {
		const ListStripe &listStripe = dlsIt->first;
		const DegradedLock &degradedLock = dlsIt->second;

		for ( uint32_t i = 0; i < degradedLock.reconstructedCount; i++ ) {
			Metadata src, dst;
			src.set(
				degradedLock.original[ i * 2     ],
				listStripe.stripeId,
				degradedLock.original[ i * 2 + 1 ]
			);
			dst.set(
				degradedLock.reconstructed[ i * 2     ],
				0, // Stripe ID is not set
				degradedLock.reconstructed[ i * 2 + 1 ]
			);

			chunksIt = chunks.find( dst );
			if ( chunksIt != chunks.end() ) {
				std::vector<Metadata> &srcs = chunksIt->second;
				srcs.push_back( src );
			} else {
				std::vector<Metadata> srcs;
				srcs.push_back( src );
				chunks[ dst ] = srcs;
			}

		}
	}
	map.degradedLocks.swap( map.releasingDegradedLocks );
	UNLOCK( &map.degradedLocksLock );

	if ( chunks.size() == 0 ) {
		__INFO__( GREEN, "CoordinatorWorker", "handleReleaseDegradedLockRequest", "Complete release degraded locks for server id:%u cond@%p lock@%p map@%p", socket->instanceId, cond, lock, &socket->map );
		// No chunks needed to be sync.
		if ( lock ) pthread_mutex_lock( lock );
		if ( done ) *done = true;
		if ( cond ) pthread_cond_signal( cond );
		if ( lock ) pthread_mutex_unlock( lock );
		return true;
	}

	// Update pending map
	instanceId = Coordinator::instanceId;
	requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );

	for ( chunksIt = chunks.begin(); chunksIt != chunks.end(); chunksIt++ ) {
		std::vector<Metadata> &srcs = chunksIt->second;
		const Metadata &dst = chunksIt->first;
		dstSocket = CoordinatorWorker::stripeList->get( dst.listId, dst.chunkId );

		CoordinatorWorker::pending->addReleaseDegradedLock( requestId, srcs.size(), lock, cond, done );

		isCompleted = true;

		buffer.data = this->protocol.buffer.send;
		do {
			buffer.size = this->protocol.generateDegradedReleaseReqHeader(
				PROTO_MAGIC_REQUEST,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_RELEASE_DEGRADED_LOCKS,
				instanceId, requestId,
				srcs,
				isCompleted
			);
			ret = dstSocket->send( buffer.data, buffer.size, connected );
			if ( ret != ( ssize_t ) buffer.size )
				__ERROR__( "CoordinatorWorker", "handleReleaseDegradedLockRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
		} while ( ! isCompleted );

		// printf( "dst: (%u, %u) |-> %lu\n", chunksIt->first.listId, chunksIt->first.chunkId, srcs.size() );
	}

	return true;
}

bool CoordinatorWorker::handleReleaseDegradedLockResponse( ServerEvent event, char *buf, size_t size ) {
	struct DegradedReleaseResHeader header;
	if ( ! this->protocol.parseDegradedReleaseResHeader( header, buf, size ) ) {
		__ERROR__( "CoordinatorWorker", "handleReleaseDegradedLockResponse", "Invalid RELEASE_DEGRADED_LOCK request (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "CoordinatorWorker", "handleReleaseDegradedLockResponse",
		"[RELEASE_DEGRADED_LOCK] Request ID: %u; Count: %u",
		event.instanceId, event.requestId, header.count
	);

	pthread_mutex_t *lock = 0;
	pthread_cond_t *cond = 0;
	bool *done = 0;

	CoordinatorWorker::pending->removeReleaseDegradedLock( event.requestId, header.count, lock, cond, done );

	if ( lock ) pthread_mutex_lock( lock );
	if ( done ) *done = true;
	if ( cond ) pthread_cond_signal( cond );
	if ( cond ) __INFO__( GREEN, "CoordinatorWorker", "handleReleaseDegradedLockResponse", "Complete release for cond %p for request id = %u", cond, event.requestId );
	if ( lock ) pthread_mutex_unlock( lock );

	return true;
}
