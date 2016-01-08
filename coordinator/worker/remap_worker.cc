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

	uint32_t originalListId, originalChunkId;

	// Get original list and chunk ID //
	SlaveSocket *dataSlaveSocket;
	originalListId = CoordinatorWorker::stripeList->get( header.key, header.keySize, &dataSlaveSocket, 0, &originalChunkId );
	Map *map = &( dataSlaveSocket->map );

	if ( map->insertKey(
		header.key, header.keySize,
		originalListId, -1 /* stripeId */, originalChunkId,
		PROTO_OPCODE_REMAPPING_LOCK, 0 /* timestamp */,
		true, true )
	) {
		RemappingRecord remappingRecord;
		if ( header.remappedCount )
			remappingRecord.dup( header.original, header.remapped, header.remappedCount );
		else
			remappingRecord.set( 0, 0, 0 );

		if ( header.remappedCount == 0 /* no need to insert */ || CoordinatorWorker::remappingRecords->insert( key, remappingRecord, dataSlaveSocket->getAddr() ) ) {
			event.resRemappingSetLock(
				event.socket, event.instanceId, event.requestId, true, // success
				header.original, header.remapped, header.remappedCount, key
			);
		} else {
			event.resRemappingSetLock(
				event.socket, event.instanceId, event.requestId, false, // success
				0, 0, 0, key
			);
		}
	} else {
		// The key already exists
		event.resRemappingSetLock(
			event.socket, event.instanceId, event.requestId, false, // success
			0, 0, 0, key
		);
	}
	this->dispatch( event );

	return true;
}
