#include "worker.hh"
#include "../main/slave.hh"

#define BATCH_THRESHOLD		20
static struct timespec BATCH_INTVL = { 0, 500000 };

bool SlaveWorker::handleRemappedData( CoordinatorEvent event, char *buf, size_t size ) {
	struct AddressHeader header;
	if ( ! this->protocol.parseAddressHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleRemappedData", "Invalid Remapped Data Sync request." );
		return false;
	}

	SlavePeerEvent slavePeerEvent;
	Slave *slave = Slave::getInstance();

	struct sockaddr_in target;
	target.sin_addr.s_addr = header.addr;
	target.sin_port = header.port;
	SlavePeerSocket *socket = 0;
	for ( uint32_t i = 0; i < slave->sockets.slavePeers.size(); i++ ) {
		if ( slave->sockets.slavePeers.values[ i ]->getAddr() == target ) {
			socket = slave->sockets.slavePeers.values[ i ];
			break;
		}
	}
	if ( socket == 0 )
		__ERROR__( "SlaveWorker", "handleRemappedData", "Cannot find slave sccket to send remapped data back." );

	std::set<PendingData> *remappedData;
	bool found = SlaveWorker::pending->eraseRemapData( target, &remappedData );

	if ( found && socket ) {
		uint16_t instanceId = Slave::instanceId;
		uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
		// insert into pending set to wait for acknowledgement
		SlaveWorker::pending->insertRemapDataRequest( instanceId, event.instanceId, requestId, event.requestId, remappedData->size(), socket );
		// dispatch one event for each key
		// TODO : batched SET
		uint32_t requestSent = 0;
		for ( PendingData pendingData : *remappedData) {
			slavePeerEvent.reqSet( socket, instanceId, requestId, pendingData.key, pendingData.value );
			slave->eventQueue.insert( slavePeerEvent );
			requestSent++;
			if ( requestSent % BATCH_THRESHOLD == 0 ) {
				nanosleep( &BATCH_INTVL, 0 );
				requestSent = 0;
			}
		}
		delete remappedData;
	} else {
		event.resRemappedData();
		this->dispatch( event );
		// slave not found, but remapped data found.. discard the data
		if ( found ) delete remappedData;
	}
	return false;
}

bool SlaveWorker::handleRemappingSetRequest( MasterEvent event, char *buf, size_t size ) {
	struct RemappingSetHeader header;
	if ( ! this->protocol.parseRemappingSetHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleRemappingSetRequest", "Invalid REMAPPING_SET request (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleRemappingSetRequest",
		"[SET] Key: %.*s (key size = %u); Value: (value size = %u); list ID = %u, chunk ID = %u.",
		( int ) header.keySize, header.key, header.keySize, header.valueSize,
		header.listId, header.chunkId
	);

	// Check whether this is the remapping target //
	bool isRemapped = true, bufferRemapData = true, success = true;
	std::vector<StripeListIndex> &stripeListIndex = Slave::getInstance()->stripeListIndex;

	for ( size_t j = 0, size = stripeListIndex.size(); j < size; j++ ) {
		if ( header.listId == stripeListIndex[ j ].listId &&
			 ( header.chunkId == stripeListIndex[ j ].chunkId || stripeListIndex[ j ].chunkId >= SlaveWorker::dataChunkCount ) ) {
			isRemapped = false;
			bufferRemapData = false;
			break;
		}
	}
	if ( isRemapped ) {
		struct sockaddr_in targetAddr;
		for ( uint32_t i = 0; i < header.remappedCount; i++ ) {
			if (   header.original[ i * 2     ] == header.listId &&
			     ( header.original[ i * 2 + 1 ] == header.chunkId ) ) {
				targetAddr = SlaveWorker::stripeList->get( header.remapped[ i * 2 ], header.remapped[ i * 2 + 1 ] )->getAddr();

				for ( size_t j = 0, size = stripeListIndex.size(); j < size; j++ ) {
					if ( header.remapped[ i * 2     ] == stripeListIndex[ j ].listId &&
					     header.remapped[ i * 2 + 1 ] == stripeListIndex[ j ].chunkId ) {
						bufferRemapData = false;
						break;
					}
				}
				break;
			}
		}

		// printf( "Remapping " );
		// for ( uint32_t i = 0; i < header.remappedCount; i++ ) {
		// 	if ( i ) printf( "; " );
		// 	printf(
		// 		"(%u, %u) |-> (%u, %u)",
		// 		header.original[ i * 2 ], header.original[ i * 2 + 1 ],
		// 		header.remapped[ i * 2 ], header.remapped[ i * 2 + 1 ]
		// 	);
		// }
		// printf( " (count = %u).\n", header.remappedCount );

		if ( bufferRemapData ) {
			__ERROR__(
				"SlaveWorker", "handleRemappingSetRequest",
				"Performing remapping data buffering: (original: (%u, %u)).",
				header.listId, header.chunkId
			);
			Key key;
			key.set( header.keySize, header.key );
			Value value;
			value.set( header.valueSize, header.value );
			SlaveWorker::pending->insertRemapData( targetAddr, header.listId, header.chunkId, key, value );
			return false;
		} else {
			SlaveWorker::remappedBuffer->insert(
				header.listId, header.chunkId,
				header.original, header.remapped, header.remappedCount,
				header.key, header.keySize,
				header.value, header.valueSize
			);
		}
	} else if (
		( map->findValueByKey( header.key, header.keySize, 0, 0 ) ) ||
		( SlaveWorker::chunkBuffer->at( header.listId )->findValueByKey( header.key, header.keySize, 0, 0 ) )
	) {
		// success = false;
	} else {
		// Store the key-value pair
		uint32_t timestamp, stripeId;
		bool isSealed;
		Metadata sealed;
		SlaveWorker::chunkBuffer->at( header.listId )->set(
			this,
			header.key, header.keySize,
			header.value, header.valueSize,
			PROTO_OPCODE_REMAPPING_SET, timestamp,
			stripeId, header.chunkId,
			&isSealed, &sealed,
			this->chunks, this->dataChunk, this->parityChunk
		);
	}

	Key key;
	key.set( header.keySize, header.key );
	event.resRemappingSet(
		event.socket, event.instanceId, event.requestId, success, key,
		header.listId, header.chunkId,
		header.original, header.remapped, header.remappedCount,
		false // needsFree
	);
	this->dispatch( event );

	return true;
}

bool SlaveWorker::handleRemappingSetRequest( SlavePeerEvent event, char *buf, size_t size ) {
	__ERROR__( "SlaveWorker", "handleRemappingSetRequest", "Why remapping SET request is sent by slave peer?" );
	return false;
	/*
	struct RemappingSetHeader header;
	if ( ! this->protocol.parseRemappingSetHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleRemappingSetRequest", "Invalid REMAPPING_SET request (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleRemappingSetRequest",
		"[SET] Key: %.*s (key size = %u); Value: (value size = %u); list ID = %u, chunk ID = %u; needs forwarding? %s",
		( int ) header.keySize, header.key, header.keySize, header.valueSize,
		header.listId, header.chunkId, header.needsForwarding ? "true" : "false"
	);

	bool isSealed;
	Metadata sealed;
	uint32_t timestamp, stripeId;
	SlaveWorker::chunkBuffer->at( header.listId )->set(
		this,
		header.key, header.keySize,
		header.value, header.valueSize,
		PROTO_OPCODE_REMAPPING_SET, timestamp,
		stripeId,
		SlaveWorker::chunkBuffer->at( header.listId )->getChunkId(),
		&isSealed, &sealed,
		this->chunks, this->dataChunk, this->parityChunk
	);

	Key key;
	key.set( header.keySize, header.key );
	event.resRemappingSet( event.socket, event.instanceId, event.requestId, key, header.listId, header.chunkId, true );
	this->dispatch( event );

	return true;
	*/
}

bool SlaveWorker::handleRemappingSetResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	__ERROR__( "SlaveWorker", "handleRemappingSetRequest", "Why remapping SET response is received by slave peer?" );
	return false;
	/*
	struct RemappingLockHeader header;
	if ( ! this->protocol.parseRemappingLockHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleRemappingSetResponse", "Invalid REMAPPING_SET response (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleRemappingSetResponse",
		"[REMAPPING_SET] Key: %.*s (key size = %u); list ID: %u, chunk ID: %u",
		( int ) header.keySize, header.key, header.keySize, header.listId, header.chunkId
	);

	int pending;
	PendingIdentifier pid;
	RemappingRecordKey record;

	if ( ! SlaveWorker::pending->eraseRemappingRecordKey( PT_SLAVE_PEER_REMAPPING_SET, event.instanceId, event.requestId, event.socket, &pid, &record, true, false ) ) {
		UNLOCK( &SlaveWorker::pending->slavePeers.remappingSetLock );
		__ERROR__( "SlaveWorker", "handleRemappingSetResponse", "Cannot find a pending slave REMAPPING_SET request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}
	// Check pending slave UPDATE requests
	pending = SlaveWorker::pending->count( PT_SLAVE_PEER_REMAPPING_SET, pid.instanceId, pid.requestId, false, true );

	__DEBUG__( BLUE, "SlaveWorker", "handleRemappingSetResponse", "Pending slave REMAPPING_SET requests = %d (%s).", pending, success ? "success" : "fail" );
	if ( pending == 0 ) {
		// Only send master REMAPPING_SET response when the number of pending slave REMAPPING_SET requests equal 0
		MasterEvent masterEvent;

		if ( ! SlaveWorker::pending->eraseRemappingRecordKey( PT_MASTER_REMAPPING_SET, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &record ) ) {
			__ERROR__( "SlaveWorker", "handleRemappingSetResponse", "Cannot find a pending master REMAPPING_SET request that matches the response. This message will be discarded." );
			return false;
		}

		masterEvent.resRemappingSet(
			( MasterSocket * ) pid.ptr, pid.instanceId, pid.requestId, record.key,
			record.remap.listId, record.remap.chunkId, success, true,
			header.sockfd, header.isRemapped
		);
		SlaveWorker::eventQueue->insert( masterEvent );
	}

	return true;
	*/
}

bool SlaveWorker::handleRemappedUpdateRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct KeyValueUpdateHeader header;
	bool ret;
	if ( ! this->protocol.parseKeyValueUpdateHeader( header, true, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleRemappedUpdateRequest", "Invalid UPDATE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleRemappedUpdateRequest",
		"[UPDATE] Key: %.*s (key size = %u); Value: (update size = %u, offset = %u).",
		( int ) header.keySize, header.key, header.keySize,
		header.valueUpdateSize, header.valueUpdateOffset
	);

	uint32_t listId, chunkId;
	Key key;

	this->getSlaves( header.key, header.keySize, listId, chunkId );
	key.set( header.keySize, header.key );

	if ( SlaveWorker::chunkBuffer->at( listId )->updateKeyValue(
		header.key, header.keySize,
		header.valueUpdateOffset, header.valueUpdateSize, header.valueUpdate
	) ) {
		// Parity not remapped
		ret = true;
	} else if ( remappedBuffer->update( header.keySize, header.key, header.valueUpdateSize, header.valueUpdateOffset, header.valueUpdate ) ) {
		// Parity remapped
		ret = false;
	} else {
		ret = false;
	}

	event.resRemappedUpdate(
		event.socket, event.instanceId, event.requestId, key,
		header.valueUpdateOffset, header.valueUpdateSize, ret
	);
	this->dispatch( event );

	return ret;
}

bool SlaveWorker::handleRemappedDeleteRequest( SlavePeerEvent event, char *buf, size_t size ) {
	printf( "handleRemappedDeleteRequest\n" );
	return true;
}

bool SlaveWorker::handleRemappedUpdateResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	struct KeyValueUpdateHeader header;
	if ( ! this->protocol.parseKeyValueUpdateHeader( header, false, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleRemappedUpdateResponse", "Invalid UPDATE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleRemappedUpdateResponse",
		"[UPDATE] Key: %.*s (key size = %u); Value: (update size = %u, offset = %u).",
		( int ) header.keySize, header.key, header.keySize,
		header.valueUpdateSize, header.valueUpdateOffset
	);

	int pending;
	KeyValueUpdate keyValueUpdate;
	PendingIdentifier pid;
	uint16_t instanceId = Slave::instanceId;

	if ( ! SlaveWorker::pending->eraseKeyValueUpdate( PT_SLAVE_PEER_UPDATE, instanceId, event.requestId, event.socket, &pid, &keyValueUpdate, true, false ) ) {
		UNLOCK( &SlaveWorker::pending->slavePeers.updateLock );
		__ERROR__( "SlaveWorker", "handleRemappedUpdateResponse", "Cannot find a pending slave UPDATE request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	// erase data delta backup
	// Slave *slave = Slave::getInstance();
	// LOCK( &slave->sockets.mastersIdToSocketLock );
	// try {
	// 	MasterSocket *masterSocket = slave->sockets.mastersIdToSocketMap.at( event.instanceId );
	// 	masterSocket->backup.removeDataUpdate( event.requestId, event.socket );
	// } catch ( std::out_of_range &e ) {
	// 	__ERROR__( "SlaveWorker", "handleUpdateResponse", "Cannot find a pending parity slave UPDATE backup for instance ID = %hu, request ID = %u. (Socket mapping not found)", event.instanceId, event.requestId );
	// }
	// UNLOCK( &slave->sockets.mastersIdToSocketLock );

	// Check pending slave UPDATE requests
	pending = SlaveWorker::pending->count( PT_SLAVE_PEER_UPDATE, pid.instanceId, pid.requestId, false, true );

	__DEBUG__( YELLOW, "SlaveWorker", "handleRemappedUpdateResponse", "Pending slave UPDATE requests = %d (%s) (Key: %.*s).", pending, success ? "success" : "fail", ( int ) header.keySize, header.key );

	if ( pending == 0 ) {
		// Only send master UPDATE response when the number of pending slave UPDATE requests equal 0
		MasterEvent masterEvent;

		if ( ! SlaveWorker::pending->eraseKeyValueUpdate( PT_MASTER_UPDATE, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &keyValueUpdate ) ) {
			__ERROR__( "SlaveWorker", "handleUpdateResponse", "Cannot find a pending master UPDATE request that matches the response. This message will be discarded." );
			return false;
		}

		masterEvent.resUpdate(
			( MasterSocket * ) pid.ptr, pid.instanceId, pid.requestId,
			keyValueUpdate,
			header.valueUpdateOffset,
			header.valueUpdateSize,
			success, true, false
		);
		this->dispatch( masterEvent );
	}
	return true;
}

bool SlaveWorker::handleRemappedDeleteResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	printf( "handleRemappedDeleteResponse\n" );
	return true;
}
