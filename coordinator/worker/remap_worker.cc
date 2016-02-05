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

	// Check whether the "failed" servers are in intermediate or degraded state
	CoordinatorRemapMsgHandler *crmh = CoordinatorRemapMsgHandler::getInstance();

	// bool isPrinted = false;
	for ( uint32_t i = 0; i < header.remappedCount; ) {
		SlaveSocket *s = CoordinatorWorker::stripeList->get(
			header.original[ i * 2    ],
			header.original[ i * 2 + 1 ]
		);
		struct sockaddr_in addr = s->getAddr();
		if ( ! crmh->allowRemapping( addr ) ) {
			/*
			if ( ! isPrinted ) {
				for ( uint32_t i = 0; i < header.remappedCount; i++ ) {
					printf(
						"%s(%u, %u) |-> (%u, %u)%s",
						i == 0 ? "Original: " : "; ",
						header.original[ i * 2     ],
						header.original[ i * 2 + 1 ],
						header.remapped[ i * 2     ],
						header.remapped[ i * 2 + 1 ],
						i == header.remappedCount - 1 ? " || " : ""
					);
				}
				isPrinted = true;
			}
			printf(
				"** Not in intermediate or degraded state: (%u, %u) **",
				header.original[ i * 2    ],
				header.original[ i * 2 + 1 ]
			);
			*/
			for ( uint32_t j = i; j < header.remappedCount - 1; j++ ) {
				header.original[ j * 2     ] = header.original[ ( j + 1 ) * 2     ];
				header.original[ j * 2 + 1 ] = header.original[ ( j + 1 ) * 2 + 1 ];
				header.remapped[ j * 2     ] = header.remapped[ ( j + 1 ) * 2     ];
				header.remapped[ j * 2 + 1 ] = header.remapped[ ( j + 1 ) * 2 + 1 ];
			}
			header.remappedCount--;
		} else {
			i++;
		}
	}
	/*
	if ( isPrinted ) {
		for ( uint32_t i = 0; i < header.remappedCount; i++ ) {
			printf(
				"%s(%u, %u) |-> (%u, %u)%s",
				i == 0 ? "Modified: " : "; ",
				header.original[ i * 2     ],
				header.original[ i * 2 + 1 ],
				header.remapped[ i * 2     ],
				header.remapped[ i * 2 + 1 ],
				i == header.remappedCount - 1 ? "\n" : ""
			);
		}
	}
	*/

	if ( map->insertKey(
		header.key, header.keySize,
		originalListId, -1 /* stripeId */, originalChunkId,
		PROTO_OPCODE_REMAPPING_LOCK, 0 /* timestamp */,
		true, true )
		|| true /***** HACK FOR YCSB which sends duplicated keys for SET *****/
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
			// event.resRemappingSetLock(
			// 	event.socket, event.instanceId, event.requestId, false, // success
			// 	0, 0, 0, key
			// );

			// ---------- HACK FOR YCSB which sends duplicated keys for SET ----------
			LOCK_T *lock;
			if ( CoordinatorWorker::remappingRecords->find( key, &remappingRecord, &lock ) ) {
				// Remapped
				event.resRemappingSetLock(
					event.socket, event.instanceId, event.requestId, true, // success
					remappingRecord.original, remappingRecord.remapped, remappingRecord.remappedCount, key
				);
				this->dispatch( event );
				UNLOCK( lock );
				return true;
			} else {
				event.resRemappingSetLock(
					event.socket, event.instanceId, event.requestId, false, // success
					0, 0, 0, key
				);
			}
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
