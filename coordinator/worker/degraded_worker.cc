#include "worker.hh"
#include "../main/coordinator.hh"

bool CoordinatorWorker::handleDegradedLockRequest( MasterEvent event, char *buf, size_t size ) {
	struct DegradedLockReqHeader header;
	if ( ! this->protocol.parseDegradedLockReqHeader( header, buf, size ) ) {
		__ERROR__( "CoordinatorWorker", "handleDegradedLockRequest", "Invalid DEGRADED_LOCK request (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "CoordinatorWorker", "handleDegradedLockRequest",
		"[DEGRADED_LOCK] Key: %.*s (key size = %u); List ID: %u; Data: (%u --> %u); Parity: (%u --> %u).",
		( int ) header.keySize, header.key, header.keySize,
		header.listId,
		header.srcDataChunkId, header.dstDataChunkId,
		header.srcParityChunkId, header.dstParityChunkId
	);

	// Metadata metadata;
	RemappingRecord remappingRecord;
	Key key;
	key.set( header.keySize, header.key );

	if ( CoordinatorWorker::remappingRecords->find( key, &remappingRecord ) ) {
		// Remapped
		if ( remappingRecord.listId != header.listId || remappingRecord.chunkId != header.srcDataChunkId ) {
			// Reject the degraded operation if the data chunk ID does not match
			event.resDegradedLock(
				event.socket, event.instanceId, event.requestId, key,
				header.listId,
				header.srcDataChunkId, remappingRecord.chunkId,
				header.srcParityChunkId, header.srcParityChunkId
			);
			this->dispatch( event );
			return false;
		}
	}

	// Find the SlaveSocket which stores the stripe with listId and srcDataChunkId
	SlaveSocket *socket = CoordinatorWorker::stripeList->get( header.listId, header.srcDataChunkId );
	Map *map = &socket->map;
	Metadata srcMetadata /* set via findMetadataByKey() */, dstMetadata;
	bool ret = true;

	if ( ! map->findMetadataByKey( header.key, header.keySize, srcMetadata ) ) {
		// Key not found
		event.resDegradedLock(
			event.socket, event.instanceId, event.requestId,
			key, false,
			header.listId, header.srcDataChunkId, header.srcParityChunkId
		);
		ret = false;
	} else if ( header.srcDataChunkId != header.dstDataChunkId && map->findDegradedLock( srcMetadata.listId, srcMetadata.stripeId, header.srcDataChunkId, dstMetadata ) ) {
		// The chunk is already locked
		event.resDegradedLock(
			event.socket, event.instanceId, event.requestId, key,
			dstMetadata.listId == header.listId && dstMetadata.chunkId == header.dstDataChunkId, // the degraded lock is attained
			map->isSealed( srcMetadata ), // the chunk is sealed
			srcMetadata.listId, srcMetadata.stripeId,
			srcMetadata.chunkId, dstMetadata.chunkId,
			header.srcParityChunkId, header.srcParityChunkId // Ignore parity server redirection
		);
	} else if ( header.srcParityChunkId != header.dstParityChunkId && map->findDegradedLock( srcMetadata.listId, srcMetadata.stripeId, header.srcParityChunkId, dstMetadata ) ) {
		// The chunk is already locked
		event.resDegradedLock(
			event.socket, event.instanceId, event.requestId, key,
			dstMetadata.listId == header.listId && dstMetadata.chunkId == header.dstParityChunkId, // the degraded lock is attained
			map->isSealed( srcMetadata ), // the chunk is sealed
			srcMetadata.listId, srcMetadata.stripeId,
			srcMetadata.chunkId, srcMetadata.chunkId, // Ignore data server redirection
			header.srcParityChunkId, dstMetadata.chunkId
		);
	} else if ( header.srcDataChunkId == header.dstDataChunkId && header.srcParityChunkId == header.dstParityChunkId ) {
		// No need to lock
		event.resDegradedLock(
			event.socket, event.instanceId, event.requestId,
			key, true,
			srcMetadata.listId,
			header.srcDataChunkId,
			header.srcParityChunkId
		);
	} else {
		// Check whether any chunk in the same stripe has been locked
		bool exist = false;
		Metadata tmpDst;
		for ( uint32_t chunkId = 0; chunkId < CoordinatorWorker::chunkCount; chunkId++ ) {
			if ( chunkId != srcMetadata.chunkId && map->findDegradedLock( srcMetadata.listId, srcMetadata.stripeId, chunkId, tmpDst ) ) {
				printf(
					"Chunk (%u, %u, %u) in the same stripe has been locked (destination chunk ID: %u). Rejecting degraded lock request: (data: %u --> %u; parity: %u --> %u).\n",
					srcMetadata.listId, srcMetadata.stripeId,
					chunkId, tmpDst.chunkId,
					header.srcDataChunkId, header.dstDataChunkId,
					header.srcParityChunkId, header.dstParityChunkId
				);
				exist = true;
				break;
			}
		}

		if ( exist ) {
			// Reject the lock request
			event.resDegradedLock(
				event.socket, event.instanceId, event.requestId,
				key, true,
				srcMetadata.listId,
				header.srcDataChunkId,
				header.srcParityChunkId
			);
		} else {
			if ( header.srcDataChunkId != header.dstDataChunkId ) {
				dstMetadata.set( header.listId, 0, header.dstDataChunkId );
				ret = map->insertDegradedLock( srcMetadata, dstMetadata );
			} else {
				Metadata tmp;
				tmp.set( srcMetadata.listId, srcMetadata.stripeId, header.srcParityChunkId );

				dstMetadata.set( header.listId, 0, header.dstParityChunkId );
				ret = map->insertDegradedLock( tmp, dstMetadata );
			}

			event.resDegradedLock(
				event.socket, event.instanceId, event.requestId, key,
				true,                         // the degraded lock is attained
				map->isSealed( srcMetadata ), // the chunk is sealed
				srcMetadata.listId, srcMetadata.stripeId,
				srcMetadata.chunkId, header.dstDataChunkId,
				header.srcParityChunkId, header.dstParityChunkId
			);
		}
	}
	this->dispatch( event );
	return ret;
}

bool CoordinatorWorker::handleReleaseDegradedLockRequest( SlaveSocket *socket, bool *done ) {
	std::unordered_map<Metadata, Metadata>::iterator dlsIt;
	Map &map = socket->map;
	Metadata dst;
	SlaveSocket *dstSocket;
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
		const Metadata &src = dlsIt->first, &dst = dlsIt->second;
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
	map.degradedLocks.swap( map.releasingDegradedLocks );
	UNLOCK( &map.degradedLocksLock );

	if ( chunks.size() == 0 ) {
		// No chunks needed to be sync.
		*done = true;
		return true;
	}

	// Update pending map
	instanceId = Coordinator::instanceId;
	requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );

	for ( chunksIt = chunks.begin(); chunksIt != chunks.end(); chunksIt++ ) {
		std::vector<Metadata> &srcs = chunksIt->second;
		dst = chunksIt->first;
		dstSocket = CoordinatorWorker::stripeList->get( dst.listId, dst.chunkId );

		CoordinatorWorker::pending->addReleaseDegradedLock( requestId, srcs.size(), done );

		isCompleted = true;
		do {

			buffer.data = this->protocol.reqReleaseDegradedLock(
				buffer.size, instanceId, requestId, srcs, isCompleted
			);
			ret = dstSocket->send( buffer.data, buffer.size, connected );
			if ( ret != ( ssize_t ) buffer.size )
				__ERROR__( "CoordinatorWorker", "handleReleaseDegradedLockRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
		} while ( ! isCompleted );

		// printf( "dst: (%u, %u) |-> %lu\n", chunksIt->first.listId, chunksIt->first.chunkId, srcs.size() );
	}

	return true;
}

bool CoordinatorWorker::handleReleaseDegradedLockResponse( SlaveEvent event, char *buf, size_t size ) {
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

	bool *done = CoordinatorWorker::pending->removeReleaseDegradedLock( event.requestId, header.count );
	if ( done )
		*done = true;
	return true;
}
