#include "worker.hh"
#include "../main/coordinator.hh"

bool CoordinatorWorker::handleRemappingSetLockRequest( MasterEvent event, char *buf, size_t size ) {
	struct RemappingLockHeader header;
	if ( ! this->protocol.parseRemappingLockHeader( header, buf, size ) ) {
		__ERROR__( "CoordinatorWorker", "handleRemappingSetLockRequest", "Invalid REMAPPING_SET_LOCK request (size = %lu).", size );
		return false;
	}

	__DEBUG__(
		BLUE, "CoordinatorWorker", "handleRemappingSetLockRequest",
		"[REMAPPING_SET_LOCK] Key: %.*s (key size = %u).",
		( int ) header.keySize, header.key, header.keySize
	);

	Key key;
	key.set( header.keySize, header.key );

	uint32_t originalListId, originalChunkId, remappedListId, remappedChunkId;
	bool isRemapped;
	for ( uint32_t i = 0; i < header.remappedCount; i++ ) {
		originalListId  = header.original[ i     ];
		originalChunkId = header.original[ i + 1 ];
		remappedListId  = header.remapped[ i     ];
		remappedChunkId = header.remapped[ i + 1 ];
		isRemapped = originalListId != remappedListId || originalChunkId != remappedChunkId;

		// Find the SlaveSocket which stores the stripe with srcListId and srcChunkId
		SlaveSocket *socket = CoordinatorWorker::stripeList->get( originalListId, originalChunkId );
		Map *map = &socket->map;

		// if already exists, does not allow remap; otherwise insert the remapping record
		RemappingRecord remappingRecord( remappedListId, remappedChunkId );
		if ( map->insertKey(
			header.key, header.keySize,
			originalListId, 0 /* stripeId */, originalChunkId,
			PROTO_OPCODE_REMAPPING_LOCK, 0 /* timestamp */,
			true, true )
		) {
			if ( isRemapped ) {
				SlaveSocket *slaveSocket = this->stripeList->get( originalListId, originalChunkId );
				if ( CoordinatorWorker::remappingRecords->insert( key, remappingRecord, slaveSocket->getAddr() ) ) {
					// No need to update the remapping record

					// event.resRemappingSetLock( event.socket, event.instanceId, event.requestId, header.isRemapped, key, remappingRecord, true, header.sockfd );
				} else {
					header.remapped[ i     ] = remappingRecord.listId;
					header.remapped[ i + 1 ] = remappingRecord.chunkId;
					// event.resRemappingSetLock( event.socket, event.instanceId, event.requestId, header.isRemapped, key, remappingRecord, false, header.sockfd );
				}
			} else {
				// event.resRemappingSetLock( event.socket, event.instanceId, event.requestId, header.isRemapped, key, remappingRecord, true, header.sockfd );
			}
		} else {
			// event.resRemappingSetLock( event.socket, event.instanceId, event.requestId, header.isRemapped, key, remappingRecord, false, header.sockfd );
		}
	}
	this->dispatch( event );

	return true;
}
