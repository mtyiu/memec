#include "worker.hh"
#include "../main/coordinator.hh"

bool CoordinatorWorker::handleRemappingSetLockRequest( MasterEvent event, char *buf, size_t size ) {
	struct RemappingLockHeader header;
	std::vector<uint32_t> remapList;
	if ( ! this->protocol.parseRemappingLockHeader( header, buf, size, &remapList ) ) {
		__ERROR__( "CoordinatorWorker", "handleRemappingSetLockRequest", "Invalid REMAPPING_SET_LOCK request (size = %lu).", size );
		return false;
	}

	for ( uint32_t i = 1; i < remapList.size(); i++ ) {
		if ( remapList[ i ] != CoordinatorWorker::dataChunkCount + i - 1 ) {
			// TODO record the remapped parity locations
			 //printf("remap parity: remapList[ %d ] = %u\n", i, remapList[ i ] );
		}
	}

	__DEBUG__(
		BLUE, "CoordinatorWorker", "handleRemappingSetLockRequest",
		"[REMAPPING_SET_LOCK] Key: %.*s (key size = %u); remapped list ID: %u, remapped chunk ID: %u",
		( int ) header.keySize, header.key, header.keySize, header.listId, header.chunkId
	);

	Key key;
	key.set( header.keySize, header.key );

	// Find the SlaveSocket which stores the stripe with srcListId and srcChunkId
	SlaveSocket *socket = CoordinatorWorker::stripeList->get( header.listId, header.chunkId );
	Map *map = &socket->map;

	// if already exists, does not allow remap; otherwise insert the remapping record
	RemappingRecord remappingRecord( header.listId, header.chunkId );
	if ( map->insertKey(
		header.key, header.keySize, header.listId, 0,
		header.chunkId, PROTO_OPCODE_REMAPPING_LOCK, 0 /* timestamp */,
		true, true )
	) {
		if ( header.isRemapped ) {
			uint32_t originalChunkId;
			SlaveSocket* dataSlaveSockets[ dataChunkCount ];
			this->stripeList->get( key.data, key.size, dataSlaveSockets, 0, &originalChunkId );
			if ( CoordinatorWorker::remappingRecords->insert( key, remappingRecord, dataSlaveSockets[ originalChunkId ]->getAddr() ) ) {
				key.dup();
				LOCK( &Coordinator::getInstance()->pendingRemappingRecords.toSendLock );
				Coordinator::getInstance()->pendingRemappingRecords.toSend[ key ] = remappingRecord;
				UNLOCK( &Coordinator::getInstance()->pendingRemappingRecords.toSendLock );
				event.resRemappingSetLock( event.socket, event.instanceId, event.requestId, header.isRemapped, key, remappingRecord, true, header.sockfd );
			} else {
				event.resRemappingSetLock( event.socket, event.instanceId, event.requestId, header.isRemapped, key, remappingRecord, false, header.sockfd );
			}
		} else {
			event.resRemappingSetLock( event.socket, event.instanceId, event.requestId, header.isRemapped, key, remappingRecord, true, header.sockfd );
		}
	} else {
		event.resRemappingSetLock( event.socket, event.instanceId, event.requestId, header.isRemapped, key, remappingRecord, false, header.sockfd );
	}
	this->dispatch( event );

	return true;
}
