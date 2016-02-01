#include "worker.hh"
#include "../main/slave.hh"

bool SlaveWorker::handleForwardKeyResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	struct ForwardKeyHeader header;
	if ( ! this->protocol.parseForwardKeyResHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleForwardKeyResponse", "Invalid DEGRADED_SET response (size = %lu).", size );
		return false;
	}
	if ( header.opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		__DEBUG__(
			BLUE, "SlaveWorker", "handleForwardKeyResponse",
			"[DEGRADED_SET] Degraded opcode: 0x%x; list ID: %u, chunk ID: %u; key: %.*s (size = %u); value size: %u; value update size: %u, offset: %u.",
			header.opcode, header.listId, header.chunkId,
			header.keySize, header.key, header.keySize,
			header.valueSize, header.valueUpdateSize, header.valueUpdateOffset
		);
	} else {
		__DEBUG__(
			BLUE, "SlaveWorker", "handleForwardKeyResponse",
			"[DEGRADED_SET] Degraded opcode: 0x%x; list ID: %u, chunk ID: %u; key: %.*s (size = %u); value size: %u.",
			header.opcode, header.listId, header.chunkId,
			header.keySize, header.key, header.keySize,
			header.valueSize
		);
	}

	return this->handleForwardKeyResponse( header, success, false );
}

bool SlaveWorker::handleForwardKeyResponse( struct ForwardKeyHeader &header, bool success, bool self ) {
	DegradedMap *dmap = &SlaveWorker::degradedChunkBuffer->map;
	Key key;
	PendingIdentifier pid;
	DegradedOp op;
	std::vector<struct pid_s> pids;

	key.set( header.keySize, header.key );

	if ( ! dmap->deleteDegradedKey( key, pids ) ) {
		// __ERROR__( "SlaveWorker", "handleForwardKeyResponse", "SlaveWorker::degradedChunkBuffer->deleteDegradedKey() failed: %.*s (self? %s).", header.keySize, header.key, self ? "yes" : "no" );
		return false;
	}

	KeyValue keyValue;
	if ( map->findValueByKey( header.key, header.keySize, &keyValue, &key ) ) {
	} else {
		__ERROR__( "SlaveWorker", "handleForwardKeyResponse", "Cannot find the forwarded object locally." );
	}

	for ( int pidsIndex = 0, len = pids.size(); pidsIndex < len; pidsIndex++ ) {
		if ( pidsIndex == 0 ) {
			// assert( pids[ pidsIndex ].instanceId == pid.instanceId && pids[ pidsIndex ].requestId == pid.requestId );
		}

		if ( ! SlaveWorker::pending->eraseDegradedOp( PT_SLAVE_PEER_DEGRADED_OPS, pids[ pidsIndex ].instanceId, pids[ pidsIndex ].requestId, 0, &pid, &op ) ) {
			__ERROR__( "SlaveWorker", "handleGetResponse", "Cannot find a pending slave DEGRADED_OPS request that matches the response. This message will be discarded." );
			continue;
		}

		MasterEvent masterEvent;
		switch( op.opcode ) {
			case PROTO_OPCODE_DEGRADED_UPDATE:
				if ( success ) {
					Metadata metadata;
					metadata.set( op.listId, op.stripeId, op.chunkId );
					uint32_t dataUpdateOffset = KeyValue::getChunkUpdateOffset(
						0,                            // chunkOffset
						op.data.keyValueUpdate.size,  // keySize
						op.data.keyValueUpdate.offset // valueUpdateOffset
					);
					char *valueUpdate = ( char * ) op.data.keyValueUpdate.ptr;

					// Insert into master UPDATE pending set
					op.data.keyValueUpdate.ptr = op.socket;
					op.data.keyValueUpdate.isDegraded = true;
					if ( ! SlaveWorker::pending->insertKeyValueUpdate( PT_MASTER_UPDATE, pid.parentInstanceId, pid.parentRequestId, op.socket, op.data.keyValueUpdate ) ) {
						__ERROR__( "SlaveWorker", "handleForwardKeyResponse", "Cannot insert into master UPDATE pending map." );
					}

					// Compute data delta
					Coding::bitwiseXOR(
						valueUpdate,
						keyValue.data + dataUpdateOffset, // original data
						valueUpdate,                      // new data
						op.data.keyValueUpdate.length
					);
					// Perform actual data update
					Coding::bitwiseXOR(
						keyValue.data + dataUpdateOffset,
						keyValue.data + dataUpdateOffset, // original data
						valueUpdate,                      // new data
						op.data.keyValueUpdate.length
					);

					// Send UPDATE request to the parity slaves
					this->sendModifyChunkRequest(
						pid.parentInstanceId, pid.parentRequestId,
						op.data.keyValueUpdate.size,
						op.data.keyValueUpdate.data,
						metadata,
						0, /* chunkUpdateOffset */
						op.data.keyValueUpdate.length, /* deltaSize */
						op.data.keyValueUpdate.offset,
						valueUpdate,
						false /* isSealed */,
						true /* isUpdate */,
						op.timestamp,
						op.socket,
						op.original, op.reconstructed, op.reconstructedCount,
						false // reconstructParity - already reconstructed
					);

					delete[] valueUpdate;
				} else {
					masterEvent.resUpdate(
						op.socket, pid.parentInstanceId, pid.parentRequestId, key,
						op.data.keyValueUpdate.offset,
						op.data.keyValueUpdate.length,
						false, false, true
					);
					this->dispatch( masterEvent );

					op.data.keyValueUpdate.free();
					delete[] ( ( char * ) op.data.keyValueUpdate.ptr );
				}
				break;
			case PROTO_OPCODE_DEGRADED_DELETE:
				if ( success ) {
					Metadata metadata;
					metadata.set( op.listId, op.stripeId, op.chunkId );
					// Insert into master DELETE pending set
					op.data.key.ptr = op.socket;
					if ( ! SlaveWorker::pending->insertKey( PT_MASTER_DEL, pid.parentInstanceId, pid.parentRequestId, op.socket, op.data.key ) ) {
						__ERROR__( "SlaveWorker", "handleForwardKeyResponse", "Cannot insert into master DELETE pending map." );
					}

					this->sendModifyChunkRequest(
						pid.parentInstanceId, pid.parentRequestId, key.size, key.data,
						metadata,
						// not needed for deleting a key-value pair in an unsealed chunk:
						0, 0, 0, 0,
						false, // isSealed
						false,  // isUpdate
						op.timestamp,
						op.socket,
						op.original, op.reconstructed, op.reconstructedCount,
						false // reconstructParity - already reconstructed
					);
				} else {
					masterEvent.resDelete(
						op.socket, pid.parentInstanceId, pid.parentRequestId,
						key,
						false, // needsFree
						true   // isDegraded
					);
					this->dispatch( masterEvent );
				}
				op.data.key.free();
				break;
			default:
				__ERROR__( "SlaveWorker", "handleForwardKeyResponse", "Unexpected opcode: 0x%x", op.opcode );
		}
	}

	return true;
}

bool SlaveWorker::handleSetResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	PendingIdentifier pid;
	uint32_t requestCount;
	bool found = SlaveWorker::pending->decrementRemapDataRequest( event.instanceId, event.requestId, &pid, &requestCount );
	if ( found && requestCount == 0 ){
		CoordinatorEvent coordinatorEvent;
		Slave *slave = Slave::getInstance();
		for ( uint32_t i = 0; i < slave->sockets.coordinators.size(); i++ ) {
			CoordinatorSocket *socket = slave->sockets.coordinators.values[ i ];
			coordinatorEvent.resRemappedData( socket, pid.parentInstanceId, pid.parentRequestId );
			this->dispatch( coordinatorEvent );
		}
	}
	return found;
}

bool SlaveWorker::handleGetResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	Key key;
	KeyValue keyValue;
	Metadata metadata;
	DegradedMap *dmap = &SlaveWorker::degradedChunkBuffer->map;
	if ( success ) {
		struct KeyValueHeader header;
		if ( this->protocol.parseKeyValueHeader( header, buf, size ) ) {
			key.set( header.keySize, header.key, ( void * ) event.socket );
			keyValue.dup( header.key, header.keySize, header.value, header.valueSize );
		} else {
			__ERROR__( "SlaveWorker", "handleGetResponse", "Invalid GET response." );
			return false;
		}
	} else {
		struct KeyHeader header;
		if ( this->protocol.parseKeyHeader( header, buf, size ) ) {
			key.set( header.keySize, header.key, ( void * ) event.socket );
		} else {
			__ERROR__( "SlaveWorker", "handleGetResponse", "Invalid GET response." );
			return false;
		}
	}

	PendingIdentifier pid;
	DegradedOp op;
	std::vector<struct pid_s> pids;
	bool isInserted = false;
	uint32_t timestamp;

	if ( ! SlaveWorker::pending->eraseKey( PT_SLAVE_PEER_GET, event.instanceId, event.requestId, event.socket, &pid ) ) {
		__ERROR__( "SlaveWorker", "handleGetResponse", "Cannot find a pending slave UNSEALED_GET request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		if ( success ) keyValue.free();
		return false;
	}

	if ( ! dmap->deleteDegradedKey( key, pids ) ) {
		__ERROR__( "SlaveWorker", "handleGetResponse", "SlaveWorker::degradedChunkBuffer->deleteDegradedKey() failed." );
	}

	for ( int pidsIndex = 0, len = pids.size(); pidsIndex < len; pidsIndex++ ) {
		if ( pidsIndex == 0 ) {
			assert( pids[ pidsIndex ].instanceId == pid.instanceId && pids[ pidsIndex ].requestId == pid.requestId );
		}

		if ( ! SlaveWorker::pending->eraseDegradedOp( PT_SLAVE_PEER_DEGRADED_OPS, pids[ pidsIndex ].instanceId, pids[ pidsIndex ].requestId, 0, &pid, &op ) ) {
			__ERROR__( "SlaveWorker", "handleGetResponse", "Cannot find a pending slave DEGRADED_OPS request that matches the response. This message will be discarded." );
			if ( success ) keyValue.free();
			continue;
		}

		if ( success ) {
			if ( op.opcode == PROTO_OPCODE_DEGRADED_DELETE ) {
				metadata.set( op.listId, op.stripeId, op.chunkId );
				dmap->deleteValue( key, metadata, PROTO_OPCODE_DELETE, timestamp );
				keyValue.free();
			} else if ( ! isInserted ) {
				Metadata metadata;
				metadata.set( op.listId, op.stripeId, op.chunkId );
				isInserted = dmap->insertValue( keyValue, metadata );
				if ( ! isInserted ) {
					__ERROR__( "SlaveWorker", "handleGetResponse", "Cannot insert into degraded chunk buffer values map (key: %.*s).", key.size, key.data );
					keyValue.free();

					// Use the fetched key-value pair
					KeyMetadata keyMetadata;
					bool isSealed;
					bool isKeyValueFound = dmap->findValueByKey(
						key.data,
						key.size,
						isSealed,
						&keyValue, &key, &keyMetadata
					);

					success = isKeyValueFound;
				}
			}
		}

		MasterEvent masterEvent;
		switch( op.opcode ) {
			case PROTO_OPCODE_DEGRADED_GET:
				LOCK( &dmap->unsealed.lock );
				if ( success ) {
					masterEvent.resGet( op.socket, pid.parentInstanceId, pid.parentRequestId, keyValue, true );
				} else {
					masterEvent.resGet( op.socket, pid.parentInstanceId, pid.parentRequestId, key, true );
				}
				op.data.key.free();
				this->dispatch( masterEvent );
				UNLOCK( &dmap->unsealed.lock );
				break;
			case PROTO_OPCODE_DEGRADED_UPDATE:
				if ( success ) {
					/* {
						uint8_t _keySize;
						uint32_t _valueSize;
						char *_keyStr, *_valueStr;
						KeyValue kv;
						bool isSealed;
						dmap->findValueByKey(
							key.data,
							key.size,
							isSealed,
							&kv
						);
						kv.deserialize( _keyStr, _keySize, _valueStr, _valueSize );
						printf( "Before: %.*s - %.*s (%p vs %p) / update: %.*s (size = %u)\n", _keySize, _keyStr, _valueSize, _valueStr, kv.data, keyValue.data, op.data.keyValueUpdate.size, ( char * ) op.data.keyValueUpdate.ptr, op.data.keyValueUpdate.size );
					} */

					LOCK( &dmap->unsealed.lock );

					Metadata metadata;
					metadata.set( op.listId, op.stripeId, op.chunkId );
					uint32_t dataUpdateOffset = KeyValue::getChunkUpdateOffset(
						0,                            // chunkOffset
						op.data.keyValueUpdate.size,  // keySize
						op.data.keyValueUpdate.offset // valueUpdateOffset
					);
					char *valueUpdate = ( char * ) op.data.keyValueUpdate.ptr;

					// Insert into master UPDATE pending set
					op.data.keyValueUpdate.ptr = op.socket;
					op.data.keyValueUpdate.isDegraded = true;
					if ( ! SlaveWorker::pending->insertKeyValueUpdate( PT_MASTER_UPDATE, pid.parentInstanceId, pid.parentRequestId, op.socket, op.data.keyValueUpdate ) ) {
						__ERROR__( "SlaveWorker", "handleGetResponse", "Cannot insert into master UPDATE pending map." );
					}

					// Compute data delta
					Coding::bitwiseXOR(
						valueUpdate,
						keyValue.data + dataUpdateOffset, // original data
						valueUpdate,                      // new data
						op.data.keyValueUpdate.length
					);
					// Perform actual data update
					Coding::bitwiseXOR(
						keyValue.data + dataUpdateOffset,
						keyValue.data + dataUpdateOffset, // original data
						valueUpdate,                      // new data
						op.data.keyValueUpdate.length
					);

					UNLOCK( &dmap->unsealed.lock );

					/* {
						uint8_t _keySize;
						uint32_t _valueSize;
						char *_keyStr, *_valueStr;
						KeyValue kv;
						bool isSealed;
						dmap->findValueByKey(
							key.data,
							key.size,
							isSealed,
							&kv
						);
						kv.deserialize( _keyStr, _keySize, _valueStr, _valueSize );
						printf( "Intermediate: %.*s - %.*s (%p vs %p)\n", _keySize, _keyStr, _valueSize, _valueStr, kv.data, keyValue.data );
					} */

					// Send UPDATE request to the parity slaves
					this->sendModifyChunkRequest(
						pid.parentInstanceId, pid.parentRequestId,
						op.data.keyValueUpdate.size,
						op.data.keyValueUpdate.data,
						metadata,
						0, /* chunkUpdateOffset */
						op.data.keyValueUpdate.length, /* deltaSize */
						op.data.keyValueUpdate.offset,
						valueUpdate,
						false /* isSealed */,
						true /* isUpdate */,
						op.timestamp,
						op.socket,
						op.original, op.reconstructed, op.reconstructedCount,
						false // reconstructParity - already reconstructed
					);

					delete[] valueUpdate;

					/* {
						uint8_t _keySize;
						uint32_t _valueSize;
						char *_keyStr, *_valueStr;
						KeyValue kv;
						bool isSealed;
						dmap->findValueByKey(
							key.data,
							key.size,
							isSealed,
							&kv
						);
						kv.deserialize( _keyStr, _keySize, _valueStr, _valueSize );
						printf( "After: %.*s - %.*s (%p vs %p)\n", _keySize, _keyStr, _valueSize, _valueStr, kv.data, keyValue.data );
					} */
				} else {
					masterEvent.resUpdate(
						op.socket, pid.parentInstanceId, pid.parentRequestId, key,
						op.data.keyValueUpdate.offset,
						op.data.keyValueUpdate.length,
						false, false, true
					);
					this->dispatch( masterEvent );

					op.data.keyValueUpdate.free();
					delete[] ( ( char * ) op.data.keyValueUpdate.ptr );
				}
				break;
			case PROTO_OPCODE_DEGRADED_DELETE:
				if ( success ) {
					Metadata metadata;
					metadata.set( op.listId, op.stripeId, op.chunkId );
					// Insert into master DELETE pending set
					op.data.key.ptr = op.socket;
					if ( ! SlaveWorker::pending->insertKey( PT_MASTER_DEL, pid.parentInstanceId, pid.parentRequestId, op.socket, op.data.key ) ) {
						__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot insert into master DELETE pending map." );
					}

					this->sendModifyChunkRequest(
						pid.parentInstanceId, pid.parentRequestId, key.size, key.data,
						metadata,
						// not needed for deleting a key-value pair in an unsealed chunk:
						0, 0, 0, 0,
						false, // isSealed
						false,  // isUpdate
						op.timestamp,
						op.socket,
						op.original, op.reconstructed, op.reconstructedCount,
						false // reconstructParity - already reconstructed
					);
				} else {
					masterEvent.resDelete(
						op.socket, pid.parentInstanceId, pid.parentRequestId,
						key,
						false, // needsFree
						true   // isDegraded
					);
					this->dispatch( masterEvent );
				}
				op.data.key.free();
				break;
		}
	}

	return true;
}

bool SlaveWorker::handleUpdateResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	struct ChunkKeyValueUpdateHeader header;
	if ( ! this->protocol.parseChunkKeyValueUpdateHeader( header, false, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleUpdateResponse", "Invalid UPDATE response." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleUpdateResponse",
		"[UPDATE] Key: %.*s (key size = %u); ; update value size = %u at offset: %u; list ID = %u, stripe ID = %u, chunk Id = %u.",
		( int ) header.keySize, header.key, header.keySize,
		header.valueUpdateSize, header.valueUpdateOffset,
		header.listId, header.stripeId, header.chunkId
	);

	int pending;
	KeyValueUpdate keyValueUpdate;
	PendingIdentifier pid;
	uint16_t instanceId = Slave::instanceId;

	if ( ! SlaveWorker::pending->eraseKeyValueUpdate( PT_SLAVE_PEER_UPDATE, instanceId, event.requestId, event.socket, &pid, &keyValueUpdate, true, false ) ) {
		UNLOCK( &SlaveWorker::pending->slavePeers.updateLock );
		__ERROR__( "SlaveWorker", "handleUpdateResponse", "Cannot find a pending slave UPDATE request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	// erase data delta backup
	/* Seems that we don't need the data delta...
	Slave *slave = Slave::getInstance();
	LOCK( &slave->sockets.mastersIdToSocketLock );
	try {
		MasterSocket *masterSocket = slave->sockets.mastersIdToSocketMap.at( event.instanceId );
		masterSocket->backup.removeDataUpdate( event.requestId, event.instanceId, event.socket );
	} catch ( std::out_of_range &e ) {
		__ERROR__( "SlaveWorker", "handleUpdateResponse", "Cannot find a pending parity slave UPDATE backup for instance ID = %hu, request ID = %u. (Socket mapping not found)", event.instanceId, event.requestId );
	}
	UNLOCK( &slave->sockets.mastersIdToSocketLock );
	*/

	// Check pending slave UPDATE requests
	pending = SlaveWorker::pending->count( PT_SLAVE_PEER_UPDATE, pid.instanceId, pid.requestId, false, true );

	__DEBUG__( YELLOW, "SlaveWorker", "handleUpdateResponse", "Pending slave UPDATE requests = %d (%s) (Key: %.*s).", pending, success ? "success" : "fail", ( int ) header.keySize, header.key );

	if ( pending == 0 ) {
		// Only send master UPDATE response when the number of pending slave UPDATE requests equal 0
		MasterEvent masterEvent;

		// FOR TESTING REVERT ONLY (parity slave fails and skip waiting)
		//PendingIdentifier dpid = pid;

		if ( ! SlaveWorker::pending->eraseKeyValueUpdate( PT_MASTER_UPDATE, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &keyValueUpdate ) ) {
			__ERROR__( "SlaveWorker", "handleUpdateResponse", "Cannot find a pending master UPDATE request that matches the response. This message will be discarded." );
			return false;
		}


		// FOR TESTING REVERT ONLY (parity slave fails and skip waiting)
		//SlavePeerSocket *s = 0;
		//this->stripeList->get( keyValueUpdate.data, keyValueUpdate.size, 0, this->paritySlaveSockets );
		//for ( uint32_t i = 0; i < this->parityChunkCount; i++ ) {
		//	if ( this->paritySlaveSockets[ i ]->instanceId == 2 ) {
		//		s = this->paritySlaveSockets[ i ];
		//		break;
		//	}
		//}
		//SlaveWorker::pending->insertKeyValueUpdate( PT_MASTER_UPDATE, pid.instanceId, pid.requestId, pid.ptr , keyValueUpdate );
		//SlaveWorker::pending->insertKeyValueUpdate( PT_SLAVE_PEER_UPDATE, dpid.instanceId, pid.instanceId, dpid.requestId, pid.requestId, ( void * ) s, keyValueUpdate );
		//keyValueUpdate.dup();

		masterEvent.resUpdate(
			( MasterSocket * ) pid.ptr, pid.instanceId, pid.requestId,
			keyValueUpdate,
			header.valueUpdateOffset,
			header.valueUpdateSize,
			success, true, keyValueUpdate.isDegraded
		);
		this->dispatch( masterEvent );
	}
	return true;
}

bool SlaveWorker::handleDeleteResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	struct ChunkKeyHeader header;
	if ( ! this->protocol.parseChunkKeyHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleDeleteRequest", "Invalid DELETE response." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleDeleteRequest",
		"[DELETE] Key: %.*s (key size = %u); list ID = %u, stripe ID = %u, chunk Id = %u.",
		( int ) header.keySize, header.key, header.keySize,
		header.listId, header.stripeId, header.chunkId
	);

	int pending;
	Key key;
	PendingIdentifier pid;

	if ( ! SlaveWorker::pending->eraseKey( PT_SLAVE_PEER_DEL, event.instanceId, event.requestId, event.socket, &pid, &key, true, false ) ) {
		UNLOCK( &SlaveWorker::pending->slavePeers.delLock );
		__ERROR__( "SlaveWorker", "handleDeleteResponse", "Cannot find a pending slave DELETE request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	// erase data delta backup
	Slave *slave = Slave::getInstance();
	LOCK( &slave->sockets.mastersIdToSocketLock );
	try {
		MasterSocket *masterSocket = slave->sockets.mastersIdToSocketMap.at( event.instanceId );
		masterSocket->backup.removeDataDelete( event.requestId, event.instanceId, event.socket );
	} catch ( std::out_of_range &e ) {
		__ERROR__( "SlaveWorker", "handleDeleteResponse", "Cannot find a pending parity slave UPDATE backup for instance ID = %hu, request ID = %u. (Socket mapping not found)", event.instanceId, event.requestId );
	}
	UNLOCK( &slave->sockets.mastersIdToSocketLock );

	// Check pending slave UPDATE requests
	pending = SlaveWorker::pending->count( PT_SLAVE_PEER_DEL, pid.instanceId, pid.requestId, false, true );

	__DEBUG__( BLUE, "SlaveWorker", "handleDeleteResponse", "Pending slave DELETE requests = %d (%s).", pending, success ? "success" : "fail" );
	if ( pending == 0 ) {
		// Only send master DELETE response when the number of pending slave DELETE requests equal 0
		MasterEvent masterEvent;
		Key key;

		if ( ! SlaveWorker::pending->eraseKey( PT_MASTER_DEL, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &key ) ) {
			__ERROR__( "SlaveWorker", "handleDeleteResponse", "Cannot find a pending master DELETE request that matches the response. This message will be discarded." );
			return false;
		}

		if ( success ) {
			__ERROR__( "SlaveWorker", "handleDeleteResponse", "TODO: slave/worker/slave_peer_res_worker.cc - Line 289: Include the timestamp and metadata in the response.\n" );
			// uint32_t timestamp = SlaveWorker::timestamp->nextVal();
			masterEvent.resDelete(
				( MasterSocket * ) pid.ptr,
				pid.instanceId, pid.requestId,
				key,
				true, // needsFree
				false // isDegraded
			);
		} else {
			masterEvent.resDelete(
				( MasterSocket * ) pid.ptr,
				pid.instanceId, pid.requestId,
				key,
				true, // needsFree
				false // isDegraded
			);
		}
		SlaveWorker::eventQueue->insert( masterEvent );
	}
	return true;
}

bool SlaveWorker::handleGetChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	int pending;
	uint32_t listId, stripeId, chunkId;
	std::unordered_map<PendingIdentifier, ChunkRequest>::iterator it, tmp, end;
	ChunkRequest chunkRequest;
	PendingIdentifier pid;
	union {
		struct ChunkDataHeader chunkData;
		struct ChunkHeader chunk;
	} header;

	if ( success ) {
		// Parse header
		if ( ! this->protocol.parseChunkDataHeader( header.chunkData, buf, size ) ) {
			__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Invalid GET_CHUNK (success) response." );
			return false;
		}
		__DEBUG__(
			BLUE, "SlaveWorker", "handleGetChunkResponse",
			"[GET_CHUNK (success)] List ID: %u, stripe ID: %u, chunk ID: %u; chunk size = %u.",
			header.chunkData.listId, header.chunkData.stripeId, header.chunkData.chunkId, header.chunkData.size
		);
		listId = header.chunkData.listId;
		stripeId = header.chunkData.stripeId;
		chunkId = header.chunkData.chunkId;
	} else {
		if ( ! this->protocol.parseChunkHeader( header.chunk, buf, size ) ) {
			__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Invalid GET_CHUNK (failure) response." );
			return false;
		}
		__DEBUG__(
			BLUE, "SlaveWorker", "handleGetChunkResponse",
			"[GET_CHUNK (failure)] List ID: %u, stripe ID: %u, chunk ID: %u.",
			header.chunk.listId, header.chunk.stripeId, header.chunk.chunkId
		);
		listId = header.chunk.listId;
		stripeId = header.chunk.stripeId;
		chunkId = header.chunk.chunkId;
	}

	// Find the corresponding GET_CHUNK request from the pending set
	if ( ! SlaveWorker::pending->findChunkRequest( PT_SLAVE_PEER_GET_CHUNK, event.instanceId, event.requestId, event.socket, it, true, false ) ) {
		UNLOCK( &SlaveWorker::pending->slavePeers.getChunkLock );
		__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot find a pending slave GET_CHUNK request that matches the response. This message will be discarded." );
		return false;
	}

	pid = it->first;
	chunkRequest = it->second;

	// Prepare stripe buffer
	for ( uint32_t i = 0, total = SlaveWorker::chunkCount; i < total; i++ ) {
		this->chunks[ i ] = 0;
	}

	// Check remaining slave GET_CHUNK requests in the pending set
	pending = 0;
	tmp = it;
	end = SlaveWorker::pending->slavePeers.getChunk.end();
	while( tmp != end && tmp->first.instanceId == event.instanceId && tmp->first.requestId == event.requestId ) {
		if ( tmp->second.chunkId == chunkId ) {
			// Store the chunk into the buffer
			if ( success ) {
				if ( header.chunkData.size ) {
					// The requested chunk is sealed
					tmp->second.chunk = SlaveWorker::chunkPool->malloc();
					tmp->second.chunk->loadFromGetChunkRequest(
						header.chunkData.listId,
						header.chunkData.stripeId,
						header.chunkData.chunkId,
						header.chunkData.chunkId >= SlaveWorker::dataChunkCount, // isParity
						header.chunkData.size,
						header.chunkData.offset,
						header.chunkData.data
					);
				} else {
					// The requested chunk is not yet sealed
					tmp->second.chunk = Coding::zeros;
				}
			} else {
				tmp->second.chunk = Coding::zeros;
			}
			this->chunks[ chunkId ] = tmp->second.chunk;
		} else if ( ! tmp->second.chunk ) {
			// The chunk is not received yet
			pending++;
		} else {
			this->chunks[ tmp->second.chunkId ] = tmp->second.chunk;
		}
		tmp++;
	}
	if ( pending == 0 )
		SlaveWorker::pending->slavePeers.getChunk.erase( it, tmp );
	UNLOCK( &SlaveWorker::pending->slavePeers.getChunkLock );

	if ( pending == 0 ) {
		// Set up chunk buffer for storing reconstructed chunks
		for ( uint32_t i = 0, j = 0; i < SlaveWorker::chunkCount; i++ ) {
			if ( ! this->chunks[ i ] ) {
				this->freeChunks[ j ].setReconstructed(
					chunkRequest.listId, chunkRequest.stripeId, i,
					i >= SlaveWorker::dataChunkCount
				);
				this->chunks[ i ] = this->freeChunks + j;
				this->chunkStatus->unset( i );
				j++;
			} else {
				this->chunkStatus->set( i );
			}
		}

		// Decode to reconstruct the lost chunk
		CodingEvent codingEvent;
		codingEvent.decode( this->chunks, this->chunkStatus );
		this->dispatch( codingEvent );

		uint32_t maxChunkSize = 0, chunkSize;
		for ( uint32_t i = 0; i < SlaveWorker::dataChunkCount; i++ ) {
			chunkSize = this->chunks[ i ]->status == CHUNK_STATUS_RECONSTRUCTED ?
			            this->chunks[ i ]->updateData() :
			            this->chunks[ i ]->getSize();
			maxChunkSize = ( chunkSize > maxChunkSize ) ? chunkSize : maxChunkSize;
			if ( chunkSize > Chunk::capacity ) {
				__ERROR__(
					"SlaveWorker", "handleGetChunkResponse",
					"[%s] Invalid chunk size (%u, %u, %u): %u",
					this->chunks[ i ]->status == CHUNK_STATUS_RECONSTRUCTED ? "Reconstructed" : "Normal",
					chunkRequest.listId, chunkRequest.stripeId, i,
					chunkSize
				);
				for ( uint32_t x = 0; x < SlaveWorker::chunkCount; x++ ) {
					printf( "\t#[%u, %u]: ", chunkRequest.listId, x );
					switch( this->chunks[ x ]->status ) {
						case CHUNK_STATUS_EMPTY:
							printf( "CHUNK_STATUS_EMPTY" );
							break;
						case CHUNK_STATUS_DIRTY:
							printf( "CHUNK_STATUS_DIRTY" );
							break;
						case CHUNK_STATUS_CACHED:
							printf( "CHUNK_STATUS_CACHED" );
							break;
						case CHUNK_STATUS_NEEDS_LOAD_FROM_DISK:
							printf( "CHUNK_STATUS_NEEDS_LOAD_FROM_DISK" );
							break;
						case CHUNK_STATUS_FROM_GET_CHUNK:
							printf( "CHUNK_STATUS_FROM_GET_CHUNK" );
							break;
						case CHUNK_STATUS_RECONSTRUCTED:
							printf( "CHUNK_STATUS_RECONSTRUCTED" );
							break;
						case CHUNK_STATUS_TEMPORARY:
							printf( "CHUNK_STATUS_TEMPORARY" );
							break;
					}
					printf( "\n" );
				}
				printf( "\n" );
				fflush( stdout );
			}
		}

		for ( uint32_t i = SlaveWorker::dataChunkCount; i < SlaveWorker::chunkCount; i++ ) {
			if ( this->chunks[ i ]->status == CHUNK_STATUS_RECONSTRUCTED ) {
				this->chunks[ i ]->isParity = true;
				this->chunks[ i ]->setSize( maxChunkSize );
			}
		}

		if ( chunkRequest.isDegraded ) {
			// Respond the original GET/UPDATE/DELETE operation using the reconstructed data
			PendingIdentifier pid;
			DegradedOp op;

			if ( ! SlaveWorker::pending->eraseDegradedOp( PT_SLAVE_PEER_DEGRADED_OPS, event.instanceId, event.requestId, event.socket, &pid, &op ) ) {
				__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot find a pending slave DEGRADED_OPS request that matches the response. This message will be discarded." );
			} else {
				bool reconstructParity, reconstructData;
				int index = this->findInRedirectedList(
					op.original, op.reconstructed, op.reconstructedCount,
					op.ongoingAtChunk, reconstructParity, reconstructData,
					op.chunkId
				);

				// Check whether the reconstructed parity chunks need to be forwarded
				DegradedMap *dmap = &SlaveWorker::degradedChunkBuffer->map;
				bool isKeyValueFound, isSealed;
				Key key;
				KeyValue keyValue;
				KeyMetadata keyMetadata;
				Metadata metadata;
				std::vector<struct pid_s> pids;

				keyMetadata.offset = 0;

				if ( ! dmap->deleteDegradedChunk( op.listId, op.stripeId, op.chunkId, pids ) ) {
					__ERROR__( "SlaveWorker", "handleGetChunkResponse", "dmap->deleteDegradedChunk() failed (%u, %u, %u).", op.listId, op.stripeId, op.chunkId );
				}
				metadata.set( op.listId, op.stripeId, op.chunkId );

				for ( int pidsIndex = 0, len = pids.size(); pidsIndex < len; pidsIndex++ ) {
					if ( pidsIndex == 0 ) {
						assert( pids[ pidsIndex ].instanceId == pid.instanceId && pids[ pidsIndex ].requestId == pid.requestId );
					} else {
						if ( ! SlaveWorker::pending->eraseDegradedOp( PT_SLAVE_PEER_DEGRADED_OPS, pids[ pidsIndex ].instanceId, pids[ pidsIndex ].requestId, 0, &pid, &op ) ) {
							__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot find a pending slave DEGRADED_OPS request that matches the response. This message will be discarded." );
						}
					}

					switch( op.opcode ) {
						case PROTO_OPCODE_DEGRADED_GET:
						case PROTO_OPCODE_DEGRADED_DELETE:
							key.set( op.data.key.size, op.data.key.data );
							break;
						case PROTO_OPCODE_DEGRADED_UPDATE:
							key.set( op.data.keyValueUpdate.size, op.data.keyValueUpdate.data );
							break;
					}

					// Find the chunk from the map
					if ( index == -1 ) {
						MasterEvent masterEvent;
						masterEvent.instanceId = pid.parentInstanceId;
						masterEvent.requestId = pid.parentRequestId;
						masterEvent.socket = op.socket;

						if ( op.opcode == PROTO_OPCODE_DEGRADED_GET ) {
							struct KeyHeader header;
							header.keySize = op.data.key.size;
							header.key = op.data.key.data;
							this->handleGetRequest( masterEvent, header, true );
						} else if ( op.opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
							struct KeyValueUpdateHeader header;
							header.keySize = op.data.keyValueUpdate.size;
							header.valueUpdateSize = op.data.keyValueUpdate.length;
							header.valueUpdateOffset = op.data.keyValueUpdate.offset;
							header.key = op.data.keyValueUpdate.data;
							header.valueUpdate = ( char * ) op.data.keyValueUpdate.ptr;

							this->handleUpdateRequest(
								masterEvent, header,
								op.original, op.reconstructed, op.reconstructedCount,
								reconstructParity,
								this->chunks,
								pidsIndex == len - 1
							);
						} else if ( op.opcode == PROTO_OPCODE_DEGRADED_DELETE ) {
							struct KeyHeader header;
							header.keySize = op.data.key.size;
							header.key = op.data.key.data;
							this->handleDeleteRequest(
								masterEvent, header,
								op.original, op.reconstructed, op.reconstructedCount,
								reconstructParity,
								this->chunks,
								pidsIndex == len - 1
							);
						}
						continue;
					}

					Chunk *chunk = 0;
					KeyMetadata keyMetadata;
					bool dataChunkReconstructed = ( op.chunkId != SlaveWorker::chunkBuffer->at( op.listId )->getChunkId() );

					if ( dataChunkReconstructed ) {
						chunk = dmap->findChunkById( op.listId, op.stripeId, op.chunkId );

						if ( ! chunk ) {
							this->chunks[ op.chunkId ]->status = CHUNK_STATUS_RECONSTRUCTED;

							chunk = SlaveWorker::chunkPool->malloc();
							chunk->swap( this->chunks[ op.chunkId ] );

							if ( ! dmap->insertChunk(
								op.listId, op.stripeId, op.chunkId, chunk,
								op.chunkId >= SlaveWorker::dataChunkCount
							) ) {
								__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot insert into degraded chunk buffer's chunk map." );
							}
						// } else {
							// __ERROR__( "SlaveWorker", "handleGetChunkResponse", "The chunk already exist." );
						}
					} else {
						// chunk = map->findChunkById( op.listId, op.stripeId, op.chunkId );
						map->findValueByKey( key.data, key.size, 0, 0, &keyMetadata, 0, &chunk );
						if ( ! (
							chunk &&
							chunk->metadata.listId == op.listId &&
							chunk->metadata.stripeId == op.stripeId &&
							chunk->metadata.chunkId == op.chunkId
						) ) {
							printf(
								"Key: %.*s (%u, %u, %u); ",
								key.size,
								key.data,
								op.listId,
								op.stripeId,
								op.chunkId
							);
							if ( ! chunk )
								printf( "chunk = (nil)\n" );
							else
								printf(
									"chunk = %p (%u, %u, %u)\n",
									chunk,
									chunk->metadata.listId,
									chunk->metadata.stripeId,
									chunk->metadata.chunkId
								);
						}
						assert(
							chunk &&
							chunk->metadata.listId == op.listId &&
							chunk->metadata.stripeId == op.stripeId &&
							chunk->metadata.chunkId == op.chunkId
						);
					}

					switch( op.opcode ) {
						case PROTO_OPCODE_DEGRADED_UPDATE:
							key.set( op.data.keyValueUpdate.size, op.data.keyValueUpdate.data );
							break;
						case PROTO_OPCODE_DEGRADED_GET:
						case PROTO_OPCODE_DEGRADED_DELETE:
						default:
							key.set( op.data.key.size, op.data.key.data );
							break;
					}

					isKeyValueFound = dmap->findValueByKey( key.data, key.size, isSealed, &keyValue, &key, &keyMetadata );

					// Send response
					if ( op.opcode == PROTO_OPCODE_DEGRADED_GET ) {
						MasterEvent event;

						if ( isKeyValueFound )
							event.resGet( op.socket, pid.parentInstanceId, pid.parentRequestId, keyValue, true );
						else
							event.resGet( op.socket, pid.parentInstanceId, pid.parentRequestId, key, true );
						this->dispatch( event );
						op.data.key.free();
					} else if ( op.opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
						uint32_t chunkUpdateOffset = KeyValue::getChunkUpdateOffset(
							keyMetadata.offset, // chunkOffset
							key.size, // keySize
							op.data.keyValueUpdate.offset // valueUpdateOffset
						);
						char *valueUpdate = ( char * ) op.data.keyValueUpdate.ptr;
						op.data.keyValueUpdate.ptr = op.socket;
						// Insert into master UPDATE pending set
						if ( ! SlaveWorker::pending->insertKeyValueUpdate( PT_MASTER_UPDATE, pid.parentInstanceId, pid.parentRequestId, op.socket, op.data.keyValueUpdate ) ) {
							__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot insert into master UPDATE pending map." );
						}

						if ( isKeyValueFound ) {
							if ( dataChunkReconstructed ) {
								SlaveWorker::degradedChunkBuffer->updateKeyValue(
									key.size, key.data,
									op.data.keyValueUpdate.length,
									op.data.keyValueUpdate.offset,
									chunkUpdateOffset,
									valueUpdate,
									chunk,
									true /* isSealed */
								);

								this->sendModifyChunkRequest(
								   pid.parentInstanceId, pid.parentRequestId,
								   key.size, key.data,
								   metadata,
								   chunkUpdateOffset,
								   op.data.keyValueUpdate.length /* deltaSize */,
								   op.data.keyValueUpdate.offset,
								   valueUpdate,
								   true /* isSealed */,
								   true /* isUpdate */,
								   op.timestamp,
								   op.socket,
								   op.original, op.reconstructed, op.reconstructedCount,
								   reconstructParity,
								   this->chunks,
								   pidsIndex == len - 1
								);

								delete[] valueUpdate;
							} else {
								__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Undefined case." );
							}
						} else if ( ! dataChunkReconstructed ) {
							///// vvvvv Copied from handleUpdateRequest() vvvvv /////
						    uint32_t offset = keyMetadata.offset + PROTO_KEY_VALUE_SIZE + key.size + op.data.keyValueUpdate.offset;

						    LOCK_T *keysLock, *cacheLock;
						    std::unordered_map<Key, KeyMetadata> *keys;
						    std::unordered_map<Metadata, Chunk *> *cache;

						    SlaveWorker::map->getKeysMap( keys, keysLock );
						    SlaveWorker::map->getCacheMap( cache, cacheLock );
						    // Lock the data chunk buffer
						    MixedChunkBuffer *chunkBuffer = SlaveWorker::chunkBuffer->at( metadata.listId );
						    int chunkBufferIndex = chunkBuffer->lockChunk( chunk, true );

						    LOCK( keysLock );
						    LOCK( cacheLock );
						    // Compute delta and perform update
						    chunk->computeDelta(
						 	   valueUpdate, // delta
						 	   valueUpdate, // new data
						 	   offset, op.data.keyValueUpdate.length,
						 	   true // perform update
						    );
						    // Release the locks
						    UNLOCK( cacheLock );
						    UNLOCK( keysLock );
						    if ( chunkBufferIndex == -1 )
						 	   chunkBuffer->unlock();
						    ///// ^^^^^ Copied from handleUpdateRequest() ^^^^^ /////

							this->sendModifyChunkRequest(
							   pid.parentInstanceId, pid.parentRequestId,
							   key.size, key.data,
							   metadata,
							   chunkUpdateOffset,
							   op.data.keyValueUpdate.length /* deltaSize */,
							   op.data.keyValueUpdate.offset,
							   valueUpdate,
							   true /* isSealed */,
							   true /* isUpdate */,
							   op.timestamp,
							   op.socket,
							   op.original, op.reconstructed, op.reconstructedCount,
							   reconstructParity,
							   this->chunks,
							   pidsIndex == len - 1
							);

							delete[] valueUpdate;
						} else {
							printf( "Key not found: %.*s\n", key.size, key.data );
							MasterEvent event;

							event.resUpdate(
								op.socket, pid.parentInstanceId, pid.parentRequestId, key,
								op.data.keyValueUpdate.offset,
								op.data.keyValueUpdate.length,
								false, false, true
							);
							this->dispatch( event );
							op.data.keyValueUpdate.free();
							delete[] ( ( char * ) op.data.keyValueUpdate.ptr );
						}
					} else if ( op.opcode == PROTO_OPCODE_DEGRADED_DELETE ) {
						uint32_t deltaSize = this->buffer.size;
						char *delta = this->buffer.data;

						if ( isKeyValueFound ) {
							uint32_t timestamp;

							// Insert into master DELETE pending set
							op.data.key.ptr = op.socket;
							if ( ! SlaveWorker::pending->insertKey( PT_MASTER_DEL, pid.parentInstanceId, pid.parentRequestId, op.socket, op.data.key ) ) {
								__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot insert into master DELETE pending map." );
							}

							if ( dataChunkReconstructed ) {
								SlaveWorker::degradedChunkBuffer->deleteKey(
									PROTO_OPCODE_DELETE, timestamp,
									key.size, key.data,
									metadata,
									true /* isSealed */,
									deltaSize, delta, chunk
								);
							} else {
								__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Degraded DELETE (data chunk not reconstructed) is not implemented." );
							}

							this->sendModifyChunkRequest(
								pid.parentInstanceId, pid.parentRequestId, key.size, key.data,
								metadata, keyMetadata.offset, deltaSize, 0, delta,
								true /* isSealed */,
								false /* isUpdate */,
								op.timestamp,
								op.socket,
								op.original, op.reconstructed, op.reconstructedCount,
								reconstructParity,
								this->chunks,
								pidsIndex == len - 1
							);
						} else {
							MasterEvent event;

							event.resDelete( op.socket, pid.parentInstanceId, pid.parentRequestId, key, false, true );
							this->dispatch( event );
							op.data.key.free();
						}
					}
				}
			}
		} else {
			Metadata metadata;

			bool hasStripe = SlaveWorker::pending->findReconstruction( pid.parentInstanceId, pid.parentRequestId, stripeId, listId, chunkId );

			// printf( "%u\n", chunkId );
			// fflush( stdout );

			SlaveWorker::stripeList->get( listId, this->paritySlaveSockets, this->dataSlaveSockets );
			SlavePeerSocket *target = ( chunkId < SlaveWorker::dataChunkCount ) ?
				( this->dataSlaveSockets[ chunkId ] ) :
				( this->paritySlaveSockets[ chunkId - SlaveWorker::dataChunkCount ] );

			// __ERROR__( "SlaveWorker", "handleGetChunkResponse", "TODO: Handle the case for reconstruction: (%u, %u, %u); target = %p.", listId, stripeId, chunkId, target );

			if ( hasStripe ) {
				// Send SET_CHUNK request
				uint16_t instanceId = Slave::instanceId;
				uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
				chunkRequest.set(
					listId, stripeId, chunkId, target,
					0 /* chunk */, false /* isDegraded */
				);
				if ( ! SlaveWorker::pending->insertChunkRequest( PT_SLAVE_PEER_SET_CHUNK, instanceId, pid.parentInstanceId, requestId, pid.parentRequestId, target, chunkRequest ) ) {
					__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot insert into slave CHUNK_REQUEST pending map." );
				}

				metadata.set( listId, stripeId, chunkId );

				event.reqSetChunk( target, instanceId, requestId, metadata, this->chunks[ chunkId ], false );
				this->dispatch( event );
			} else {
				__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot found a pending reconstruction request for (%u, %u).\n", listId, stripeId );
			}
		}

		// Return chunks to chunk pool
		for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
			switch( this->chunks[ i ]->status ) {
				case CHUNK_STATUS_FROM_GET_CHUNK:
					SlaveWorker::chunkPool->free( this->chunks[ i ] );
					this->chunks[ i ] = 0;
					break;
				case CHUNK_STATUS_RECONSTRUCTED:
					break;
				default:
					break;
			}
		}
	}
	return true;
}

bool SlaveWorker::handleSetChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	struct ChunkHeader header;
	if ( ! this->protocol.parseChunkHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleSetChunkResponse", "Invalid SET_CHUNK response." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleSetChunkResponse",
		"[SET_CHUNK (%s)] List ID: %u, stripe ID: %u, chunk ID: %u.",
		success ? "success" : "failure",
		header.listId, header.stripeId, header.chunkId
	);

	PendingIdentifier pid;
	ChunkRequest chunkRequest;
	uint16_t instanceId = Slave::instanceId;

	chunkRequest.set(
		header.listId, header.stripeId, header.chunkId,
		event.socket, 0 // ptr
	);

	if ( ! SlaveWorker::pending->eraseChunkRequest( PT_SLAVE_PEER_SET_CHUNK, instanceId, event.requestId, event.socket, &pid, &chunkRequest ) ) {
		__ERROR__( "SlaveWorker", "handleSetChunkResponse", "Cannot find a pending slave SET_CHUNK request that matches the response. This message will be discarded." );
	}

	if ( chunkRequest.isDegraded ) {
		// Release degraded lock
		uint32_t remaining, total;
		if ( ! SlaveWorker::pending->eraseReleaseDegradedLock( pid.parentInstanceId, pid.parentRequestId, 1, remaining, total, &pid ) ) {
			__ERROR__( "SlaveWorker", "handleSetChunkResponse", "Cannot find a pending coordinator release degraded lock request that matches the response. The message will be discarded." );
			return false;
		}

		if ( remaining == 0 ) {
			// Tell the coordinator that all degraded lock is released
			CoordinatorEvent coordinatorEvent;
			coordinatorEvent.resReleaseDegradedLock( ( CoordinatorSocket * ) pid.ptr, pid.instanceId, pid.requestId, total );
			SlaveWorker::eventQueue->insert( coordinatorEvent );
		}
	} else {
		// Reconstruction
		uint32_t remainingChunks, remainingKeys, totalChunks, totalKeys;
		CoordinatorSocket *socket;
		if ( ! SlaveWorker::pending->eraseReconstruction( pid.parentInstanceId, pid.parentRequestId, socket, header.listId, header.stripeId, header.chunkId, remainingChunks, remainingKeys, totalChunks, totalKeys, &pid ) ) {
			__ERROR__( "SlaveWorker", "handleSetChunkResponse", "Cannot find a pending coordinator RECONSTRUCTION request that matches the response. The message will be discarded." );
			return false;
		}

		if ( remainingChunks == 0 ) {
			// Tell the coordinator that the reconstruction task is completed
			CoordinatorEvent coordinatorEvent;
			coordinatorEvent.resReconstruction(
				socket, pid.instanceId, pid.requestId,
				header.listId, header.chunkId, totalChunks /* numStripes */
			);
			SlaveWorker::eventQueue->insert( coordinatorEvent );
		}
	}

	return true;
}

bool SlaveWorker::handleUpdateChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	struct ChunkUpdateHeader header;
	if ( ! this->protocol.parseChunkUpdateHeader( header, false, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleUpdateChunkResponse", "Invalid UPDATE_CHUNK response." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleUpdateChunkResponse",
		"[UPDATE_CHUNK (%s)] List ID: %u, stripe ID: %u, chunk ID: %u; offset = %u, length = %u.",
		success ? "success" : "failed",
		header.listId, header.stripeId, header.chunkId,
		header.offset, header.length
	);

	int pending;
	ChunkUpdate chunkUpdate;
	PendingIdentifier pid;
	uint16_t instanceId = Slave::instanceId;

	if ( ! SlaveWorker::pending->eraseChunkUpdate( PT_SLAVE_PEER_UPDATE_CHUNK, instanceId, event.requestId, event.socket, &pid, &chunkUpdate, true, false ) ) {
		UNLOCK( &SlaveWorker::pending->slavePeers.updateChunkLock );
		__ERROR__( "SlaveWorker", "handleUpdateChunkResponse", "Cannot find a pending slave UPDATE_CHUNK request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	// erase data delta backup
	/* Seems that we don't need the data delta...
	Slave *slave = Slave::getInstance();
	LOCK( &slave->sockets.mastersIdToSocketLock );
	try {
		MasterSocket *masterSocket = slave->sockets.mastersIdToSocketMap.at( event.instanceId );
		masterSocket->backup.removeDataUpdate( event.requestId, event.instanceId, event.socket );
	} catch ( std::out_of_range &e ) {
		__ERROR__( "SlaveWorker", "handleUpdateChunkResponse", "Cannot find a pending parity slave UPDATE backup for instance ID = %hu, request ID = %u. (Socket mapping not found)", event.instanceId, event.requestId );
	}
	UNLOCK( &slave->sockets.mastersIdToSocketLock );
	*/

	// Check pending slave UPDATE requests
	pending = SlaveWorker::pending->count( PT_SLAVE_PEER_UPDATE_CHUNK, pid.instanceId, pid.requestId, false, true );

	__DEBUG__( GREEN, "SlaveWorker", "handleUpdateChunkResponse", "[%u, %u, %u] Pending slave UPDATE_CHUNK requests = %d (%s).", header.listId, header.stripeId, header.chunkId, pending, success ? "success" : "fail" );

	if ( pending == 0 ) {
		// Only send application UPDATE response when the number of pending slave UPDATE_CHUNK requests equal 0
		Key key;
		KeyValueUpdate keyValueUpdate;
		MasterEvent masterEvent;

		if ( ! SlaveWorker::pending->eraseKeyValueUpdate( PT_MASTER_UPDATE, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &keyValueUpdate ) ) {
			__ERROR__( "SlaveWorker", "handleUpdateChunkResponse", "Cannot find a pending master UPDATE request that matches the response. This message will be discarded." );
			return false;
		}

		key.set( keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.ptr );
		masterEvent.resUpdate(
			( MasterSocket * ) pid.ptr, pid.instanceId, pid.requestId, key,
			keyValueUpdate.offset, keyValueUpdate.length,
			success, true,
			keyValueUpdate.isDegraded // isDegraded
		);
		this->dispatch( masterEvent );
	}
	return true;
}

bool SlaveWorker::handleDeleteChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	struct ChunkUpdateHeader header;
	if ( ! this->protocol.parseChunkUpdateHeader( header, false, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Invalid DELETE_CHUNK response." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleDeleteChunkResponse",
		"[DELETE_CHUNK] List ID: %u, stripe ID: %u, chunk ID: %u; offset = %u, length = %u.",
		header.listId, header.stripeId, header.chunkId,
		header.offset, header.length
	);

	int pending;
	ChunkUpdate chunkUpdate;
	PendingIdentifier pid;
	uint16_t instanceId = Slave::instanceId;

	chunkUpdate.set(
		header.listId, header.stripeId, header.updatingChunkId,
		header.offset, header.length
	);
	chunkUpdate.setKeyValueUpdate( 0, 0, 0 );
	chunkUpdate.ptr = ( void * ) event.socket;

	if ( ! SlaveWorker::pending->eraseChunkUpdate( PT_SLAVE_PEER_DEL_CHUNK, instanceId, event.requestId, event.socket, &pid, &chunkUpdate, true, false ) ) {
		UNLOCK( &SlaveWorker::pending->slavePeers.deleteChunkLock );
		__ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Cannot find a pending slave DELETE_CHUNK request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	// erase data delta backup
	Slave *slave = Slave::getInstance();
	LOCK( &slave->sockets.mastersIdToSocketLock );
	try {
		MasterSocket *masterSocket = slave->sockets.mastersIdToSocketMap.at( event.instanceId );
		masterSocket->backup.removeDataDelete( event.requestId, event.instanceId, event.socket );
	} catch ( std::out_of_range &e ) {
		__ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Cannot find a pending parity slave UPDATE backup for instance ID = %hu, request ID = %u. (Socket mapping not found)", event.instanceId, event.requestId );
	}
	UNLOCK( &slave->sockets.mastersIdToSocketLock );

	// Check pending slave UPDATE requests
	pending = SlaveWorker::pending->count( PT_SLAVE_PEER_DEL_CHUNK, pid.instanceId, pid.requestId, false, true );

	// __ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Pending slave DELETE_CHUNK requests = %d.", pending );
	if ( pending == 0 ) {
		// Only send master DELETE response when the number of pending slave DELETE_CHUNK requests equal 0
		MasterEvent masterEvent;
		Key key;

		if ( ! SlaveWorker::pending->eraseKey( PT_MASTER_DEL, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &key ) ) {
			__ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Cannot find a pending master DELETE request that matches the response. This message will be discarded." );
			return false;
		}

		// TODO: Include the timestamp and metadata in the response
		if ( success ) {
			uint32_t timestamp = SlaveWorker::timestamp->nextVal();
			masterEvent.resDelete(
				( MasterSocket * ) pid.ptr, pid.instanceId, pid.requestId,
				timestamp,
				header.listId, header.stripeId, header.chunkId,
				key,
				true, // needsFree
				false // isDegraded
			);
		} else {
			masterEvent.resDelete(
				( MasterSocket * ) pid.ptr, pid.instanceId, pid.requestId,
				key,
				true, // needsFree
				false // isDegraded
			);
		}
		SlaveWorker::eventQueue->insert( masterEvent );
	}
	return true;
}

bool SlaveWorker::handleSealChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	return true;
}

bool SlaveWorker::handleBatchKeyValueResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	struct BatchKeyHeader header;
	if ( ! this->protocol.parseBatchKeyHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleBatchKeyValueResponse", "Invalid BATCH_KEY_VALUE response." );
		return false;
	}

	PendingIdentifier pid;
	CoordinatorSocket *coordinatorSocket;
	if ( ! SlaveWorker::pending->erase( PT_SLAVE_PEER_FORWARD_KEYS, event.instanceId, event.requestId, event.socket, &pid ) ) {
		__ERROR__( "SlaveWorker", "handleBatchKeyValueResponse", "Cannot insert find pending BATCH_KEY_VALUE request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
	}

	// printf( "--- header.count = %u ---\n", header.count );

	uint32_t listId, chunkId;
	uint32_t remainingChunks, remainingKeys, totalChunks, totalKeys;
	for ( uint32_t i = 0, offset = 0; i < header.count; i++ ) {
		uint8_t keySize;
		char *keyStr;

		this->protocol.nextKeyInBatchKeyHeader( header, keySize, keyStr, offset );

		if ( i == 0 ) {
			// Find the corresponding reconstruction request
			if ( ! SlaveWorker::pending->findReconstruction(
				pid.parentInstanceId, pid.parentRequestId,
				keySize, keyStr,
				listId, chunkId
			) ) {
				__ERROR__( "SlaveWorker", "handleBatchKeyValueResponse", "Cannot find a matching reconstruction request. (ID: (%u, %u))", pid.parentInstanceId, pid.parentRequestId );
				return false;
			}
		}

		if ( ! SlaveWorker::pending->eraseReconstruction(
			pid.parentInstanceId, pid.parentRequestId, coordinatorSocket,
			listId, chunkId,
			keySize, keyStr,
			remainingChunks, remainingKeys,
			totalChunks, totalKeys,
			&pid
		) ) {
			__ERROR__(
				"SlaveWorker", "handleBatchKeyValueResponse",
				"Cannot erase the key: %.*s for the corresponding reconstruction request. (ID: (%u, %u))",
				keySize, keyStr,
				pid.parentInstanceId, pid.parentRequestId
			);
		}
	}

	// printf( "handleBatchKeyValueResponse(): Chunks: %u / %u; keys: %u / %u\n", remainingChunks, totalChunks, remainingKeys, totalKeys );

	if ( remainingKeys == 0 ) {
		CoordinatorEvent coordinatorEvent;
		coordinatorEvent.resReconstructionUnsealed(
			coordinatorSocket, pid.instanceId, pid.requestId,
			listId, chunkId, totalKeys
		);
		SlaveWorker::eventQueue->insert( coordinatorEvent );
	}

	return false;
}
