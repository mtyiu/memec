#include "worker.hh"
#include "../main/server.hh"

bool ServerWorker::handleForwardKeyResponse( ServerPeerEvent event, bool success, char *buf, size_t size ) {
	struct ForwardKeyHeader header;
	if ( ! this->protocol.parseForwardKeyResHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleForwardKeyResponse", "Invalid DEGRADED_SET response (size = %lu).", size );
		return false;
	}
	if ( header.opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		__DEBUG__(
			BLUE, "ServerWorker", "handleForwardKeyResponse",
			"[DEGRADED_SET] Degraded opcode: 0x%x; list ID: %u, chunk ID: %u; key: %.*s (size = %u); value size: %u; value update size: %u, offset: %u.",
			header.opcode, header.listId, header.chunkId,
			header.keySize, header.key, header.keySize,
			header.valueSize, header.valueUpdateSize, header.valueUpdateOffset
		);
	} else {
		__DEBUG__(
			BLUE, "ServerWorker", "handleForwardKeyResponse",
			"[DEGRADED_SET] Degraded opcode: 0x%x; list ID: %u, chunk ID: %u; key: %.*s (size = %u); value size: %u.",
			header.opcode, header.listId, header.chunkId,
			header.keySize, header.key, header.keySize,
			header.valueSize
		);
	}

	return this->handleForwardKeyResponse( header, success, false );
}

bool ServerWorker::handleForwardKeyResponse( struct ForwardKeyHeader &header, bool success, bool self ) {
	DegradedMap *dmap = &ServerWorker::degradedChunkBuffer->map;
	Key key;
	PendingIdentifier pid;
	DegradedOp op;
	std::vector<struct pid_s> pids;

	key.set( header.keySize, header.key );

	if ( ! dmap->deleteDegradedKey( key, pids, true ) ) {
		// __ERROR__( "ServerWorker", "handleForwardKeyResponse", "ServerWorker::degradedChunkBuffer->deleteDegradedKey() failed: %.*s (self? %s).", header.keySize, header.key, self ? "yes" : "no" );
		return false;
	}

	KeyValue keyValue;
	if ( map->findValueByKey( header.key, header.keySize, &keyValue, &key ) ) {
	} else {
		__ERROR__( "ServerWorker", "handleForwardKeyResponse", "Cannot find the forwarded object locally." );
	}

	for ( int pidsIndex = 0, len = pids.size(); pidsIndex < len; pidsIndex++ ) {
		if ( pidsIndex == 0 ) {
			// assert( pids[ pidsIndex ].instanceId == pid.instanceId && pids[ pidsIndex ].requestId == pid.requestId );
		}

		if ( ! ServerWorker::pending->eraseDegradedOp( PT_SERVER_PEER_DEGRADED_OPS, pids[ pidsIndex ].instanceId, pids[ pidsIndex ].requestId, 0, &pid, &op ) ) {
			__ERROR__( "ServerWorker", "handleGetResponse", "Cannot find a pending server DEGRADED_OPS request that matches the response. This message will be discarded." );
			continue;
		}

		if ( op.opcode == 0 )
			continue;

		ClientEvent clientEvent;
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

					// Insert into client UPDATE pending set
					op.data.keyValueUpdate.ptr = op.socket;
					op.data.keyValueUpdate.isDegraded = true;
					if ( ! ServerWorker::pending->insertKeyValueUpdate( PT_CLIENT_UPDATE, pid.parentInstanceId, pid.parentRequestId, op.socket, op.data.keyValueUpdate ) ) {
						__ERROR__( "ServerWorker", "handleForwardKeyResponse", "Cannot insert into client UPDATE pending map." );
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

					// Send UPDATE request to the parity servers
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
					clientEvent.resUpdate(
						op.socket, pid.parentInstanceId, pid.parentRequestId, key,
						op.data.keyValueUpdate.offset,
						op.data.keyValueUpdate.length,
						false, false, true
					);
					this->dispatch( clientEvent );

					op.data.keyValueUpdate.free();
					delete[] ( ( char * ) op.data.keyValueUpdate.ptr );
				}
				break;
			case PROTO_OPCODE_DEGRADED_DELETE:
				if ( success ) {
					Metadata metadata;
					metadata.set( op.listId, op.stripeId, op.chunkId );
					// Insert into client DELETE pending set
					op.data.key.ptr = op.socket;
					if ( ! ServerWorker::pending->insertKey( PT_CLIENT_DEL, pid.parentInstanceId, pid.parentRequestId, op.socket, op.data.key ) ) {
						__ERROR__( "ServerWorker", "handleForwardKeyResponse", "Cannot insert into client DELETE pending map." );
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
					clientEvent.resDelete(
						op.socket, pid.parentInstanceId, pid.parentRequestId,
						key,
						false, // needsFree
						true   // isDegraded
					);
					this->dispatch( clientEvent );
				}
				op.data.key.free();
				break;
			default:
				__ERROR__( "ServerWorker", "handleForwardKeyResponse", "Unexpected opcode: 0x%x", op.opcode );
		}
	}

	return true;
}

bool ServerWorker::handleSetResponse( ServerPeerEvent event, bool success, char *buf, size_t size ) {
	PendingIdentifier pid;
	uint32_t requestCount;
	bool found = ServerWorker::pending->decrementRemapDataRequest( event.instanceId, event.requestId, &pid, &requestCount );
	if ( found && requestCount == 0 ){
		CoordinatorEvent coordinatorEvent;
		Server *server = Server::getInstance();
		for ( uint32_t i = 0; i < server->sockets.coordinators.size(); i++ ) {
			CoordinatorSocket *socket = server->sockets.coordinators.values[ i ];
			coordinatorEvent.resRemappedData( socket, pid.parentInstanceId, pid.parentRequestId );
			this->dispatch( coordinatorEvent );
		}
	}
	return found;
}

bool ServerWorker::handleGetResponse( ServerPeerEvent event, bool success, char *buf, size_t size ) {
	Key key;
	KeyValue keyValue;
	Metadata metadata;
	DegradedMap *dmap = &ServerWorker::degradedChunkBuffer->map;
	if ( success ) {
		struct KeyValueHeader header;
		if ( this->protocol.parseKeyValueHeader( header, buf, size ) ) {
			key.set( header.keySize, header.key, ( void * ) event.socket );
			keyValue.dup( header.key, header.keySize, header.value, header.valueSize );
		} else {
			__ERROR__( "ServerWorker", "handleGetResponse", "Invalid GET response." );
			return false;
		}
	} else {
		struct KeyHeader header;
		if ( this->protocol.parseKeyHeader( header, buf, size ) ) {
			key.set( header.keySize, header.key, ( void * ) event.socket );
		} else {
			__ERROR__( "ServerWorker", "handleGetResponse", "Invalid GET response." );
			return false;
		}
	}

	PendingIdentifier pid;
	DegradedOp op;
	std::vector<struct pid_s> pids;
	bool isInserted = false;
	uint32_t timestamp;

	if ( ! ServerWorker::pending->eraseKey( PT_SERVER_PEER_GET, event.instanceId, event.requestId, event.socket, &pid ) ) {
		__ERROR__( "ServerWorker", "handleGetResponse", "Cannot find a pending server UNSEALED_GET request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		if ( success ) keyValue.free();
		return false;
	}

	if ( ! dmap->deleteDegradedKey( key, pids, success ) ) {
		__ERROR__( "ServerWorker", "handleGetResponse", "ServerWorker::degradedChunkBuffer->deleteDegradedKey() failed." );
	}

	for ( int pidsIndex = 0, len = pids.size(); pidsIndex < len; pidsIndex++ ) {
		if ( pidsIndex == 0 ) {
			assert( pids[ pidsIndex ].instanceId == pid.instanceId && pids[ pidsIndex ].requestId == pid.requestId );
		}

		if ( ! ServerWorker::pending->eraseDegradedOp( PT_SERVER_PEER_DEGRADED_OPS, pids[ pidsIndex ].instanceId, pids[ pidsIndex ].requestId, 0, &pid, &op ) ) {
			__ERROR__( "ServerWorker", "handleGetResponse", "Cannot find a pending server DEGRADED_OPS request that matches the response. This message will be discarded." );
			if ( success ) keyValue.free();
			continue;
		}

		if ( op.opcode == 0 )
			continue;

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
					__ERROR__( "ServerWorker", "handleGetResponse", "Cannot insert into degraded chunk buffer values map (key: %.*s).", key.size, key.data );
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

		ClientEvent clientEvent;
		switch( op.opcode ) {
			case PROTO_OPCODE_DEGRADED_GET:
				LOCK( &dmap->unsealed.lock );
				if ( success ) {
					clientEvent.resGet( op.socket, pid.parentInstanceId, pid.parentRequestId, keyValue, true );
				} else {
					clientEvent.resGet( op.socket, pid.parentInstanceId, pid.parentRequestId, key, true );
				}
				op.data.key.free();
				this->dispatch( clientEvent );
				UNLOCK( &dmap->unsealed.lock );
				break;
			case PROTO_OPCODE_DEGRADED_UPDATE:
				if ( success ) {
					LOCK( &dmap->unsealed.lock );

					Metadata metadata;
					metadata.set( op.listId, op.stripeId, op.chunkId );
					uint32_t dataUpdateOffset = KeyValue::getChunkUpdateOffset(
						0,                            // chunkOffset
						op.data.keyValueUpdate.size,  // keySize
						op.data.keyValueUpdate.offset // valueUpdateOffset
					);
					char *valueUpdate = ( char * ) op.data.keyValueUpdate.ptr;

					// Insert into client UPDATE pending set
					op.data.keyValueUpdate.ptr = op.socket;
					op.data.keyValueUpdate.isDegraded = true;
					if ( ! ServerWorker::pending->insertKeyValueUpdate( PT_CLIENT_UPDATE, pid.parentInstanceId, pid.parentRequestId, op.socket, op.data.keyValueUpdate ) ) {
						__ERROR__( "ServerWorker", "handleGetResponse", "Cannot insert into client UPDATE pending map." );
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

					// Send UPDATE request to the parity servers
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
					clientEvent.resUpdate(
						op.socket, pid.parentInstanceId, pid.parentRequestId, key,
						op.data.keyValueUpdate.offset,
						op.data.keyValueUpdate.length,
						false, false, true
					);
					this->dispatch( clientEvent );

					op.data.keyValueUpdate.free();
					delete[] ( ( char * ) op.data.keyValueUpdate.ptr );
				}
				break;
			case PROTO_OPCODE_DEGRADED_DELETE:
				if ( success ) {
					Metadata metadata;
					metadata.set( op.listId, op.stripeId, op.chunkId );
					// Insert into client DELETE pending set
					op.data.key.ptr = op.socket;
					if ( ! ServerWorker::pending->insertKey( PT_CLIENT_DEL, pid.parentInstanceId, pid.parentRequestId, op.socket, op.data.key ) ) {
						__ERROR__( "ServerWorker", "handleGetChunkResponse", "Cannot insert into client DELETE pending map." );
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
					clientEvent.resDelete(
						op.socket, pid.parentInstanceId, pid.parentRequestId,
						key,
						false, // needsFree
						true   // isDegraded
					);
					this->dispatch( clientEvent );
				}
				op.data.key.free();
				break;
		}
	}

	return true;
}

bool ServerWorker::handleUpdateResponse( ServerPeerEvent event, bool success, char *buf, size_t size ) {
	struct ChunkKeyValueUpdateHeader header;
	if ( ! this->protocol.parseChunkKeyValueUpdateHeader( header, false, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleUpdateResponse", "Invalid UPDATE response." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleUpdateResponse",
		"[UPDATE] Key: %.*s (key size = %u); ; update value size = %u at offset: %u; list ID = %u, stripe ID = %u, chunk Id = %u.",
		( int ) header.keySize, header.key, header.keySize,
		header.valueUpdateSize, header.valueUpdateOffset,
		header.listId, header.stripeId, header.chunkId
	);

	int pending;
	KeyValueUpdate keyValueUpdate;
	PendingIdentifier pid;
	uint16_t instanceId = Server::instanceId;

	if ( ! ServerWorker::pending->eraseKeyValueUpdate( PT_SERVER_PEER_UPDATE, instanceId, event.requestId, event.socket, &pid, &keyValueUpdate, true, false ) ) {
		UNLOCK( &ServerWorker::pending->serverPeers.updateLock );
		__ERROR__( "ServerWorker", "handleUpdateResponse", "Cannot find a pending server UPDATE request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	// Check pending server UPDATE requests
	pending = ServerWorker::pending->count( PT_SERVER_PEER_UPDATE, pid.instanceId, pid.requestId, false, true );

	__DEBUG__( YELLOW, "ServerWorker", "handleUpdateResponse", "Pending server UPDATE requests = %d (%s) (Key: %.*s).", pending, success ? "success" : "fail", ( int ) header.keySize, header.key );

	if ( pending == 0 ) {
		// Only send client UPDATE response when the number of pending server UPDATE requests equal 0
		ClientEvent clientEvent;

		if ( ! ServerWorker::pending->eraseKeyValueUpdate( PT_CLIENT_UPDATE, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &keyValueUpdate ) ) {
			__ERROR__( "ServerWorker", "handleUpdateResponse", "Cannot find a pending client UPDATE request that matches the response. This message will be discarded." );
			return false;
		}

		clientEvent.resUpdate(
			( ClientSocket * ) pid.ptr, pid.instanceId, pid.requestId,
			keyValueUpdate,
			header.valueUpdateOffset,
			header.valueUpdateSize,
			success, true, keyValueUpdate.isDegraded
		);
		this->dispatch( clientEvent );
	}
	return true;
}

bool ServerWorker::handleDeleteResponse( ServerPeerEvent event, bool success, char *buf, size_t size ) {
	struct ChunkKeyHeader header;
	if ( ! this->protocol.parseChunkKeyHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleDeleteRequest", "Invalid DELETE response." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleDeleteRequest",
		"[DELETE] Key: %.*s (key size = %u); list ID = %u, stripe ID = %u, chunk Id = %u.",
		( int ) header.keySize, header.key, header.keySize,
		header.listId, header.stripeId, header.chunkId
	);

	int pending;
	Key key;
	PendingIdentifier pid;

	if ( ! ServerWorker::pending->eraseKey( PT_SERVER_PEER_DEL, event.instanceId, event.requestId, event.socket, &pid, &key, true, false ) ) {
		UNLOCK( &ServerWorker::pending->serverPeers.delLock );
		__ERROR__( "ServerWorker", "handleDeleteResponse", "Cannot find a pending server DELETE request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	// erase data delta backup
	Server *server = Server::getInstance();
	LOCK( &server->sockets.clientsIdToSocketLock );
	try {
		ClientSocket *clientSocket = server->sockets.clientsIdToSocketMap.at( event.instanceId );
		clientSocket->backup.removeDataDelete( event.requestId, event.instanceId, event.socket );
	} catch ( std::out_of_range &e ) {
		__ERROR__( "ServerWorker", "handleDeleteResponse", "Cannot find a pending parity server UPDATE backup for instance ID = %hu, request ID = %u. (Socket mapping not found)", event.instanceId, event.requestId );
	}
	UNLOCK( &server->sockets.clientsIdToSocketLock );

	// Check pending server UPDATE requests
	pending = ServerWorker::pending->count( PT_SERVER_PEER_DEL, pid.instanceId, pid.requestId, false, true );

	__DEBUG__( BLUE, "ServerWorker", "handleDeleteResponse", "Pending server DELETE requests = %d (%s).", pending, success ? "success" : "fail" );
	if ( pending == 0 ) {
		// Only send client DELETE response when the number of pending server DELETE requests equal 0
		ClientEvent clientEvent;
		Key key;

		if ( ! ServerWorker::pending->eraseKey( PT_CLIENT_DEL, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &key ) ) {
			__ERROR__( "ServerWorker", "handleDeleteResponse", "Cannot find a pending client DELETE request that matches the response. This message will be discarded." );
			return false;
		}

		if ( success ) {
			__ERROR__( "ServerWorker", "handleDeleteResponse", "TODO: server/worker/server_peer_res_worker.cc - Line 289: Include the timestamp and metadata in the response.\n" );
			// uint32_t timestamp = ServerWorker::timestamp->nextVal();
			clientEvent.resDelete(
				( ClientSocket * ) pid.ptr,
				pid.instanceId, pid.requestId,
				key,
				true, // needsFree
				false // isDegraded
			);
		} else {
			clientEvent.resDelete(
				( ClientSocket * ) pid.ptr,
				pid.instanceId, pid.requestId,
				key,
				true, // needsFree
				false // isDegraded
			);
		}
		ServerWorker::eventQueue->insert( clientEvent );
	}
	return true;
}

bool ServerWorker::handleGetChunkResponse( ServerPeerEvent event, bool success, char *buf, size_t size ) {
	int pending;
	uint32_t listId, stripeId, chunkId;
	std::unordered_map<PendingIdentifier, ChunkRequest>::iterator it, tmp, end, selfIt;
	ChunkRequest chunkRequest;
	PendingIdentifier pid;
	Chunk *toBeFreed = 0;
	DegradedMap *dmap = &ServerWorker::degradedChunkBuffer->map;
	union {
		struct ChunkDataHeader chunkData;
		struct ChunkHeader chunk;
	} header;

	if ( success ) {
		// Parse header
		if ( ! this->protocol.parseChunkDataHeader( header.chunkData, buf, size ) ) {
			__ERROR__( "ServerWorker", "handleGetChunkResponse", "Invalid GET_CHUNK (success) response." );
			return false;
		}
		__DEBUG__(
			YELLOW, "ServerWorker", "handleGetChunkResponse",
			"[GET_CHUNK (success)] List ID: %u, stripe ID: %u, chunk ID: %u; chunk size = %u.",
			header.chunkData.listId,
			header.chunkData.stripeId,
			header.chunkData.chunkId,
			header.chunkData.size
		);
		listId = header.chunkData.listId;
		stripeId = header.chunkData.stripeId;
		chunkId = header.chunkData.chunkId;
	} else {
		if ( ! this->protocol.parseChunkHeader( header.chunk, buf, size ) ) {
			__ERROR__( "ServerWorker", "handleGetChunkResponse", "Invalid GET_CHUNK (failure) response." );
			return false;
		}
		__DEBUG__(
			BLUE, "ServerWorker", "handleGetChunkResponse",
			"[GET_CHUNK (failure)] List ID: %u, stripe ID: %u, chunk ID: %u.",
			header.chunk.listId, header.chunk.stripeId, header.chunk.chunkId
		);
		listId = header.chunk.listId;
		stripeId = header.chunk.stripeId;
		chunkId = header.chunk.chunkId;
	}

	// Find the corresponding GET_CHUNK request from the pending set
	if ( ! ServerWorker::pending->findChunkRequest( PT_SERVER_PEER_GET_CHUNK, event.instanceId, event.requestId, event.socket, it, true, false ) ) {
		UNLOCK( &ServerWorker::pending->serverPeers.getChunkLock );
		__ERROR__( "ServerWorker", "handleGetChunkResponse", "Cannot find a pending server GET_CHUNK request that matches the response. This message will be discarded." );
		return false;
	}

	pid = it->first;
	chunkRequest = it->second;

	// Prepare stripe buffer
	for ( uint32_t i = 0, total = ServerWorker::chunkCount; i < total; i++ ) {
		this->chunks[ i ] = 0;
	}

	for ( uint32_t i = 0; i < ServerWorker::parityChunkCount; i++ ) {
		this->sealIndicators[ i ] = 0;
	}

	// Check remaining server GET_CHUNK requests in the pending set
	pending = 0;
	tmp = it;
	end = ServerWorker::pending->serverPeers.getChunk.end();
	selfIt = ServerWorker::pending->serverPeers.getChunk.end();
	while( tmp != end && tmp->first.instanceId == event.instanceId && tmp->first.requestId == event.requestId ) {
		if ( tmp->second.chunkId == chunkId ) {
			// Store the chunk into the buffer
			if ( success ) {
				if ( header.chunkData.size ) {
					// The requested chunk is sealed
					tmp->second.chunk = this->tempChunkPool.alloc(
						header.chunkData.listId,
						header.chunkData.stripeId,
						header.chunkData.chunkId,
						header.chunkData.size
					);
					ChunkUtil::load(
						tmp->second.chunk,
						header.chunkData.offset,
						header.chunkData.data,
						header.chunkData.size
					);

					tmp->second.isSealed = true;
					if ( header.chunkData.chunkId < ServerWorker::dataChunkCount ) {
						this->sealIndicators[ ServerWorker::parityChunkCount ][ header.chunkData.chunkId ] = true;
					}
				} else {
					// The requested chunk is not yet sealed
					tmp->second.isSealed = false;
					tmp->second.chunk = Coding::zeros;
					if ( header.chunkData.chunkId < ServerWorker::dataChunkCount ) {
						this->sealIndicators[ ServerWorker::parityChunkCount ][ header.chunkData.chunkId ] = false;
					}
				}

				if ( chunkId >= ServerWorker::dataChunkCount ) {
					assert( header.chunkData.sealIndicatorCount );
					if ( header.chunkData.sealIndicatorCount ) {
						tmp->second.setSealStatus(
							header.chunkData.size, // isSealed
							header.chunkData.sealIndicatorCount,
							header.chunkData.sealIndicator
						);
						this->sealIndicators[ header.chunkData.chunkId - ServerWorker::dataChunkCount ] = tmp->second.sealIndicator;
					}
				}
			} else {
				tmp->second.chunk = Coding::zeros;
				tmp->second.isSealed = false;
				this->sealIndicators[ ServerWorker::parityChunkCount ][ chunkId ] = false;
			}

			this->chunks[ chunkId ] = tmp->second.chunk;
		} else if ( tmp->second.self ) {
			// Do nothing
			selfIt = tmp;
			this->chunks[ tmp->second.chunkId ] = Coding::zeros;
		} else if ( ! tmp->second.chunk ) {
			// The chunk is not received yet
			pending++;
		} else {
			this->chunks[ tmp->second.chunkId ] = tmp->second.chunk;
			if ( tmp->second.chunkId < ServerWorker::dataChunkCount ) {
				this->sealIndicators[ ServerWorker::parityChunkCount ][ tmp->second.chunkId ] = tmp->second.isSealed;
			} else {
				this->sealIndicators[ tmp->second.chunkId - ServerWorker::dataChunkCount ] = tmp->second.sealIndicator;
			}
		}
		tmp++;
	}

	if ( pending == 0 ) {
		uint32_t numSurvivingParityChunks = 0;
		for ( uint32_t i = 0; i < ServerWorker::dataChunkCount; i++ ) {
			if ( ! this->chunks[ i ] ) { // To be reconstructed
				this->sealIndicators[ ServerWorker::parityChunkCount ][ i ] = true;
			}
			this->sealIndicators[ ServerWorker::parityChunkCount + 1 ][ i ] = false; // Reset
		}
		for ( uint32_t j = 0; j < ServerWorker::parityChunkCount; j++ ) {
			if ( this->chunks[ j + ServerWorker::dataChunkCount ] && this->chunks[ j + ServerWorker::dataChunkCount ] != Coding::zeros ) {
				numSurvivingParityChunks++;
				for ( uint32_t i = 0; i < ServerWorker::dataChunkCount; i++ ) {
					this->sealIndicators[ ServerWorker::parityChunkCount + 1 ][ i ] |= this->sealIndicators[ j ][ i ];
				}
			}
		}

		bool getChunkAgain = false;
		ServerWorker::stripeList->get( listId, this->parityServerSockets, this->dataServerSockets );
		// Only check if there are at least one surviving parity chunks
		for ( uint32_t i = 0; i < ServerWorker::dataChunkCount && numSurvivingParityChunks; i++ ) {
			if ( i == selfIt->second.chunkId ) // Skip self
				continue;

			if ( ! this->chunks[ i ] ) // To be reconstructed
				continue;

			if (
				this->sealIndicators[ ServerWorker::parityChunkCount + 1 ][ i ] &&
				this->sealIndicators[ ServerWorker::parityChunkCount ][ i ] != this->sealIndicators[ ServerWorker::parityChunkCount + 1 ][ i ]
			) {
				//__INFO__(
				//	GREEN, "ServerWorker", "handleGetChunkResponse",
				//	"Need to retrieve the sealed data chunk (%u, %u, %u): %d vs %d.",
				//	listId, stripeId, i,
				//	this->sealIndicators[ ServerWorker::parityChunkCount ][ i ],
				//	this->sealIndicators[ ServerWorker::parityChunkCount + 1 ][ i ]
				//);

				ServerPeerEvent serverPeerEvent;
				Metadata tmpMetadata;

				tmpMetadata.set( listId, stripeId, i );

				serverPeerEvent.reqGetChunk(
					this->dataServerSockets[ i ],
					event.instanceId,
					event.requestId,
					tmpMetadata
				);
				ServerWorker::eventQueue->insert( serverPeerEvent );

				getChunkAgain = true;
			}
		}

		if ( getChunkAgain ) {
			UNLOCK( &ServerWorker::pending->serverPeers.getChunkLock );
			return false;
		}

		if ( selfIt != ServerWorker::pending->serverPeers.getChunk.end() ) {
			Metadata selfMetadata;
			uint32_t selfChunkId = selfIt->second.chunkId;

			selfMetadata.set( listId, stripeId, selfChunkId );

			Chunk *selfChunk = ServerWorker::map->findChunkById( listId, stripeId, selfChunkId );

			if ( selfChunkId >= ServerWorker::dataChunkCount )
				__ERROR__( "ServerWorker", "handleGetChunkResponse", "selfChunkId >= ServerWorker::dataChunkCount: (%u, %u, %u)", listId, stripeId, selfChunkId );
			assert( selfChunkId < ServerWorker::dataChunkCount );

			// Check whether the chunk is sealed or not
			if ( ! selfChunk ) {
				selfChunk = Coding::zeros;
				this->sealIndicators[ ServerWorker::parityChunkCount ][ selfChunkId ] = false;
			} else {
				MixedChunkBuffer *chunkBuffer = ServerWorker::chunkBuffer->at( listId );
				int chunkBufferIndex = chunkBuffer->lockChunk( selfChunk, true );
				bool isSealed = ( chunkBufferIndex == -1 );
				if ( isSealed ) {
					// Find from backup
					uint8_t sealIndicatorCount;
					bool *sealIndicator;
					bool exists;

					Chunk *backupChunk = ServerWorker::getChunkBuffer->find( selfMetadata, exists, sealIndicatorCount, sealIndicator, true, false );
					if ( exists && backupChunk ) {
						selfChunk = backupChunk;
					} else {
						Chunk *newChunk = this->tempChunkPool.alloc();
						ChunkUtil::dup( newChunk, selfChunk );

						selfChunk = newChunk;
						toBeFreed = newChunk;
					}
					ServerWorker::getChunkBuffer->ack( selfMetadata, false, true, false );
					// chunkRequest.chunk = Coding::zeros;
					this->sealIndicators[ ServerWorker::parityChunkCount ][ selfChunkId ] = true;
					this->sealIndicators[ ServerWorker::parityChunkCount + 1 ][ selfChunkId ] = true;
				} else {
					selfChunk = Coding::zeros;
					this->sealIndicators[ ServerWorker::parityChunkCount ][ selfChunkId ] = false;
					this->sealIndicators[ ServerWorker::parityChunkCount + 1 ][ selfChunkId ] = false;
				}
				chunkBuffer->unlock( chunkBufferIndex );
			}
			this->chunks[ selfChunkId ] = selfChunk;
		}

		// Do not erase before the above if-branch as selfIt is still in-use!
		ServerWorker::pending->serverPeers.getChunk.erase( it, tmp );
		UNLOCK( &ServerWorker::pending->serverPeers.getChunkLock );

		// Check seal indicator
		bool valid = true;
		for ( uint32_t i = 0; i < ServerWorker::dataChunkCount; i++ ) {
			bool isSealed = this->sealIndicators[ ServerWorker::parityChunkCount ][ i ];
			for ( uint32_t j = 0; j < ServerWorker::parityChunkCount; j++ ) {
				if ( this->chunks[ j + ServerWorker::dataChunkCount ] && this->chunks[ j + ServerWorker::dataChunkCount ] != Coding::zeros ) {
					if ( this->sealIndicators[ j ][ i ] != isSealed ) {
						valid = false;
						break;
					}
				}
			}
			if ( ! valid )
				break;
		}
		if ( valid ) {
			// Do nothing
		} else {
			ServerWorker::stripeList->get( listId, this->parityServerSockets, this->dataServerSockets );

			Coding::forceSeal(
				ServerWorker::coding,
				this->chunks,
				this->freeChunks[ 0 ],
				this->sealIndicators,
				ServerWorker::dataChunkCount,
				ServerWorker::parityChunkCount
			);
		}
	} else {
		UNLOCK( &ServerWorker::pending->serverPeers.getChunkLock );
	}

	std::unordered_set<uint32_t> invalidChunks;
	if ( pending == 0 ) {
		// Set up chunk buffer for storing reconstructed chunks
		for ( uint32_t i = 0, j = 0; i < ServerWorker::chunkCount; i++ ) {
			if ( ! this->chunks[ i ] ) {
				ChunkUtil::set(
					this->freeChunks[ j ],
					chunkRequest.listId, chunkRequest.stripeId, i, 0
				);
				this->chunks[ i ] = this->freeChunks[ j ];
				this->chunkStatus->unset( i );
				this->chunkStatusBackup->unset( i );
				j++;
			} else {
				this->chunkStatus->set( i );
				this->chunkStatusBackup->set( i );
			}
		}

		// Decode to reconstruct the lost chunk
		CodingEvent codingEvent;
		codingEvent.decode( this->chunks, this->chunkStatus );
		this->dispatch( codingEvent );

		uint32_t maxChunkSize = 0, chunkSize;
		for ( uint32_t i = 0; i < ServerWorker::dataChunkCount; i++ ) {
			if ( this->chunkStatusBackup->check( i ) ) {
				chunkSize = ChunkUtil::getSize( this->chunks[ i ] );
			} else {
				// Reconstructed
				chunkSize = ChunkUtil::updateSize( this->chunks[ i ] );
			}

			maxChunkSize = ( chunkSize > maxChunkSize ) ? chunkSize : maxChunkSize;
			if ( chunkSize > ChunkUtil::chunkSize ) {
				__ERROR__(
					"ServerWorker", "handleGetChunkResponse",
					"[%s] Invalid chunk size (%u, %u, %u): %u",
					this->chunkStatusBackup->check( i ) ? "Normal" : "Reconstructed",
					chunkRequest.listId, chunkRequest.stripeId, i,
					chunkSize
				);
				for ( uint32_t x = 0; x < ServerWorker::chunkCount; x++ )
					ChunkUtil::print( this->chunks[ x ] );
				printf( "\n" );
				fflush( stdout );

				fprintf( stderr, "Seal indicator for (%u, %u):\n", listId, stripeId );
				for ( uint32_t j = 0; j < ServerWorker::parityChunkCount + 1; j++ ) {
					if ( j == ServerWorker::parityChunkCount || this->chunkStatusBackup->check( j ) ) {
						fprintf( stderr, "\t#%u:", j );
						for ( uint32_t i = 0; i < ServerWorker::dataChunkCount; i++ ) {
							fprintf( stderr, " %d", this->sealIndicators[ j ][ i ] ? 1 : 0 );
						}
						fprintf( stderr, "\n" );
					}
				}

				printf( "Chunk status: " );
				this->chunkStatus->print();
				fflush( stdout );

				ChunkUtil::clear( this->chunks[ i ] );
				invalidChunks.insert( i );
				// return true;
			}
		}

		if ( chunkRequest.isDegraded ) {
			// Respond the original GET/UPDATE/DELETE operation using the reconstructed data
			PendingIdentifier pid;
			DegradedOp op;

			if ( ! ServerWorker::pending->eraseDegradedOp( PT_SERVER_PEER_DEGRADED_OPS, event.instanceId, event.requestId, event.socket, &pid, &op ) ) {
				__ERROR__( "ServerWorker", "handleGetChunkResponse", "Cannot find a pending server DEGRADED_OPS request that matches the response. This message will be discarded." );
			} else {
				bool reconstructParity, reconstructData;
				int index = this->findInRedirectedList(
					op.original, op.reconstructed, op.reconstructedCount,
					op.ongoingAtChunk, reconstructParity, reconstructData,
					op.chunkId, op.isSealed
				);

				// Check whether the reconstructed parity chunks need to be forwarded
				bool isKeyValueFound, isSealed;
				Key key;
				KeyValue keyValue;
				KeyMetadata keyMetadata;
				Metadata metadata;
				std::vector<struct pid_s> pids;

				keyMetadata.offset = 0;

				if ( ! dmap->deleteDegradedChunk( op.listId, op.stripeId, op.chunkId, pids, true /* force */, true /* ignoreChunkId */ ) ) {
					// __ERROR__( "ServerWorker", "handleGetChunkResponse", "dmap->deleteDegradedChunk() failed (%u, %u, %u).", op.listId, op.stripeId, op.chunkId );
				}
				metadata.set( op.listId, op.stripeId, op.chunkId );

				// Forward chunk locally
				for ( uint32_t i = 0; i < op.reconstructedCount; i++ ) {
					ServerPeerSocket *s = ServerWorker::stripeList->get( op.reconstructed[ i * 2 ], op.reconstructed[ i * 2 + 1 ] );

					if ( invalidChunks.count( op.original[ i * 2 + 1 ] ) ) {
						// Do nothing
					} else if ( ! ChunkUtil::getSize( this->chunks[ op.original[ i * 2 + 1 ] ] ) ) {
						continue; // No need to send
					}

					if ( s->self ) {
						struct ChunkDataHeader chunkDataHeader;
						chunkDataHeader.listId = metadata.listId;
						chunkDataHeader.stripeId = metadata.stripeId;
						chunkDataHeader.chunkId = op.original[ i * 2 + 1 ];
						chunkDataHeader.size = ChunkUtil::getSize( this->chunks[ op.original[ i * 2 + 1 ] ] );
						chunkDataHeader.offset = 0;
						chunkDataHeader.data = ChunkUtil::getData( this->chunks[ op.original[ i * 2 + 1 ] ] );
						chunkDataHeader.sealIndicatorCount = 0;
						chunkDataHeader.sealIndicator = 0;
						this->handleForwardChunkRequest( chunkDataHeader, false );
					}
				}

				for ( int pidsIndex = 0, len = pids.size(); pidsIndex < len; pidsIndex++ ) {
					if ( pidsIndex == 0 ) {
						if ( ! ( pids[ pidsIndex ].instanceId == pid.instanceId && pids[ pidsIndex ].requestId == pid.requestId ) ) {
							fprintf(
								stderr,
								"[%u, %u, %u] instanceId: %u vs. %u; "
								"requestId:  %u vs. %u\n",
								op.listId, op.stripeId, op.chunkId,
								pids[ pidsIndex ].instanceId,
								pid.instanceId,
								pids[ pidsIndex ].requestId,
								pid.requestId
							);
						}
					} else {
						if ( ! ServerWorker::pending->eraseDegradedOp( PT_SERVER_PEER_DEGRADED_OPS, pids[ pidsIndex ].instanceId, pids[ pidsIndex ].requestId, 0, &pid, &op ) ) {
							__ERROR__( "ServerWorker", "handleGetChunkResponse", "Cannot find a pending server DEGRADED_OPS request that matches the response. This message will be discarded." );
							continue;
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
						default:
							continue;
					}

					// Find the chunk from the map
					if ( index == -1 ) {
						ClientEvent clientEvent;
						clientEvent.instanceId = pid.parentInstanceId;
						clientEvent.requestId = pid.parentRequestId;
						clientEvent.socket = op.socket;

						if ( op.opcode == PROTO_OPCODE_DEGRADED_GET ) {
							struct KeyHeader header;
							header.keySize = op.data.key.size;
							header.key = op.data.key.data;
							this->handleGetRequest( clientEvent, header, true );
						} else if ( op.opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
							struct KeyValueUpdateHeader header;
							header.keySize = op.data.keyValueUpdate.size;
							header.valueUpdateSize = op.data.keyValueUpdate.length;
							header.valueUpdateOffset = op.data.keyValueUpdate.offset;
							header.key = op.data.keyValueUpdate.data;
							header.valueUpdate = ( char * ) op.data.keyValueUpdate.ptr;

							this->handleUpdateRequest(
								clientEvent, header,
								op.original, op.reconstructed, op.reconstructedCount,
								reconstructParity,
								this->chunks,
								pidsIndex == len - 1,
								true
							);
						} else if ( op.opcode == PROTO_OPCODE_DEGRADED_DELETE ) {
							struct KeyHeader header;
							header.keySize = op.data.key.size;
							header.key = op.data.key.data;
							this->handleDeleteRequest(
								clientEvent, header,
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
					bool dataChunkReconstructed = ( op.chunkId != ServerWorker::chunkBuffer->at( op.listId )->getChunkId() );

					if ( dataChunkReconstructed ) {
						chunk = dmap->findChunkById( op.listId, op.stripeId, op.chunkId );

						if ( ! chunk ) {
							chunk = ServerWorker::tempChunkPool.alloc();
							ChunkUtil::dup( chunk, this->chunks[ op.chunkId ] );

							if ( ! dmap->insertChunk(
								op.listId, op.stripeId, op.chunkId, chunk,
								op.chunkId >= ServerWorker::dataChunkCount
							) ) {
								__ERROR__( "ServerWorker", "handleGetChunkResponse", "Cannot insert into degraded chunk buffer's chunk map." );
							}
						}
					} else {
						Metadata tmp;
						uint32_t tmpSize;

						tmp.set( 0, 0, 0 );

						map->findValueByKey( key.data, key.size, 0, 0, &keyMetadata, 0, &chunk );

						if ( chunk )
							ChunkUtil::get( chunk, tmp.listId, tmp.stripeId, tmp.chunkId, tmpSize );

						if ( ! (
							chunk &&
							tmp.listId == op.listId &&
							tmp.stripeId == op.stripeId &&
							tmp.chunkId == op.chunkId
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
									tmp.listId,
									tmp.stripeId,
									tmp.chunkId
								);
						}
						assert(
							chunk &&
							tmp.listId == op.listId &&
							tmp.stripeId == op.stripeId &&
							tmp.chunkId == op.chunkId
						);
					}

					switch( op.opcode ) {
						case PROTO_OPCODE_DEGRADED_UPDATE:
							key.set( op.data.keyValueUpdate.size, op.data.keyValueUpdate.data );
							break;
						case PROTO_OPCODE_DEGRADED_GET:
						case PROTO_OPCODE_DEGRADED_DELETE:
							key.set( op.data.key.size, op.data.key.data );
							break;
						default:
							continue;
					}

					isKeyValueFound = dmap->findValueByKey( key.data, key.size, isSealed, &keyValue, &key, &keyMetadata );

					// Send response
					if ( op.opcode == PROTO_OPCODE_DEGRADED_GET ) {
						ClientEvent event;

						if ( isKeyValueFound ) {
							event.resGet( op.socket, pid.parentInstanceId, pid.parentRequestId, keyValue, true );
							this->dispatch( event );
						} else {
							fprintf( stderr, "KEY NOT FOUND: %.*s\n", key.size, key.data );
							// event.resGet( op.socket, pid.parentInstanceId, pid.parentRequestId, key, true );
							event.instanceId = pid.parentInstanceId;
							event.requestId = pid.parentRequestId;
							event.socket = op.socket;
							struct KeyHeader header;
							header.key = key.data;
							header.keySize = key.size;
							this->handleGetRequest( event, header, true );
						}
						op.data.key.free();
					} else if ( op.opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
						uint32_t chunkUpdateOffset = KeyValue::getChunkUpdateOffset(
							keyMetadata.offset, // chunkOffset
							key.size, // keySize
							op.data.keyValueUpdate.offset // valueUpdateOffset
						);
						char *valueUpdate = ( char * ) op.data.keyValueUpdate.ptr;
						op.data.keyValueUpdate.ptr = op.socket;
						// Insert into client UPDATE pending set
						if ( ! ServerWorker::pending->insertKeyValueUpdate( PT_CLIENT_UPDATE, pid.parentInstanceId, pid.parentRequestId, op.socket, op.data.keyValueUpdate ) ) {
							__ERROR__( "ServerWorker", "handleGetChunkResponse", "Cannot insert into client UPDATE pending map." );
						}

						if ( isKeyValueFound ) {
							if ( dataChunkReconstructed ) {
								ServerWorker::degradedChunkBuffer->updateKeyValue(
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
								   pidsIndex == len - 1,
								   true
								);

								delete[] valueUpdate;
							} else {
								__ERROR__( "ServerWorker", "handleGetChunkResponse", "Undefined case." );
							}
						} else if ( ! dataChunkReconstructed ) {
							///// vvvvv Copied from handleUpdateRequest() vvvvv /////
						    uint32_t offset = keyMetadata.offset + PROTO_KEY_VALUE_SIZE + key.size + op.data.keyValueUpdate.offset;

						    LOCK_T *keysLock, *cacheLock;
						    std::unordered_map<Key, KeyMetadata> *keys;
						    std::unordered_map<Metadata, Chunk *> *cache;

						    ServerWorker::map->getKeysMap( keys, keysLock );
						    ServerWorker::map->getCacheMap( cache, cacheLock );

						    LOCK( keysLock );
						    LOCK( cacheLock );
						    // Lock the data chunk buffer
						    MixedChunkBuffer *chunkBuffer = ServerWorker::chunkBuffer->at( metadata.listId );
						    int chunkBufferIndex = chunkBuffer->lockChunk( chunk, true );
						    // Compute delta and perform update
							ChunkUtil::computeDelta(
								chunk,
								valueUpdate, // delta
								valueUpdate, // new data
								offset, op.data.keyValueUpdate.length,
								true // perform update
							);
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
							   pidsIndex == len - 1,
							   true
							);

							// Release the locks
							if ( chunkBufferIndex == -1 )
						 		chunkBuffer->unlock();
							else
								chunkBuffer->updateAndUnlockChunk( chunkBufferIndex );
						    UNLOCK( cacheLock );
						    UNLOCK( keysLock );

							delete[] valueUpdate;
						} else {
							ClientEvent event;
							struct KeyValueUpdateHeader header;

							event.instanceId = pid.parentInstanceId;
							event.requestId = pid.parentRequestId;
							event.socket = op.socket;

							header.keySize = op.data.keyValueUpdate.size;
							header.valueUpdateSize = op.data.keyValueUpdate.length;
							header.valueUpdateOffset = op.data.keyValueUpdate.offset;
							header.key = op.data.keyValueUpdate.data;
							header.valueUpdate = ( char * ) op.data.keyValueUpdate.ptr;

							this->handleUpdateRequest(
								event,
								header,
								op.original, op.reconstructed, op.reconstructedCount,
								reconstructParity,
								this->chunks,
								pidsIndex == len - 1,
 								true
							);

							op.data.keyValueUpdate.free();
							delete[] ( ( char * ) op.data.keyValueUpdate.ptr );
						}
					} else if ( op.opcode == PROTO_OPCODE_DEGRADED_DELETE ) {
						uint32_t deltaSize = this->buffer.size;
						char *delta = this->buffer.data;

						if ( isKeyValueFound ) {
							uint32_t timestamp;

							// Insert into client DELETE pending set
							op.data.key.ptr = op.socket;
							if ( ! ServerWorker::pending->insertKey( PT_CLIENT_DEL, pid.parentInstanceId, pid.parentRequestId, op.socket, op.data.key ) ) {
								__ERROR__( "ServerWorker", "handleGetChunkResponse", "Cannot insert into client DELETE pending map." );
							}

							if ( dataChunkReconstructed ) {
								ServerWorker::degradedChunkBuffer->deleteKey(
									PROTO_OPCODE_DELETE, timestamp,
									key.size, key.data,
									metadata,
									true /* isSealed */,
									deltaSize, delta, chunk
								);
							} else {
								__ERROR__( "ServerWorker", "handleGetChunkResponse", "Degraded DELETE (data chunk not reconstructed) is not implemented." );
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
								pidsIndex == len - 1,
								true
							);
						} else {
							ClientEvent event;

							event.resDelete( op.socket, pid.parentInstanceId, pid.parentRequestId, key, false, true );
							this->dispatch( event );
							op.data.key.free();
						}
					}
				}

				////////////////////////////////////////
				// Forward the modified parity chunks //
				////////////////////////////////////////
				ServerPeerEvent event;
				uint32_t requestId = ServerWorker::idGenerator->nextVal( this->workerId );
				for ( uint32_t i = 0; i < op.reconstructedCount; i++ ) {
					ServerPeerSocket *s = ServerWorker::stripeList->get( op.reconstructed[ i * 2 ], op.reconstructed[ i * 2 + 1 ] );

					if ( invalidChunks.count( op.original[ i * 2 + 1 ] ) ) {
						// Do nothing
					} else if ( ! ChunkUtil::getSize( this->chunks[ op.original[ i * 2 + 1 ] ] ) ) {
						continue; // No need to send
					}

					if ( s->self ) {
						continue;
						struct ChunkDataHeader chunkDataHeader;
						chunkDataHeader.listId = metadata.listId;
						chunkDataHeader.stripeId = metadata.stripeId;
						chunkDataHeader.chunkId = op.original[ i * 2 + 1 ];
						chunkDataHeader.size = ChunkUtil::getSize( this->chunks[ op.original[ i * 2 + 1 ] ] );
						chunkDataHeader.offset = 0;
						chunkDataHeader.data = ChunkUtil::getData( this->chunks[ op.original[ i * 2 + 1 ] ] );
						chunkDataHeader.sealIndicatorCount = 0;
						chunkDataHeader.sealIndicator = 0;
						this->handleForwardChunkRequest( chunkDataHeader, false );
					} else {
						metadata.chunkId = op.original[ i * 2 + 1 ];
						event.reqForwardChunk(
							s, Server::instanceId, requestId,
							metadata, this->chunks[ op.original[ i * 2 + 1 ] ], false
						);
						this->dispatch( event );
					}
				}
			}
		} else {
			Metadata metadata;

			bool hasStripe = ServerWorker::pending->findReconstruction( pid.parentInstanceId, pid.parentRequestId, stripeId, listId, chunkId );

			ServerWorker::stripeList->get( listId, this->parityServerSockets, this->dataServerSockets );
			ServerPeerSocket *target = ( chunkId < ServerWorker::dataChunkCount ) ?
				( this->dataServerSockets[ chunkId ] ) :
				( this->parityServerSockets[ chunkId - ServerWorker::dataChunkCount ] );

			if ( hasStripe ) {
				// Send SET_CHUNK request
				uint16_t instanceId = Server::instanceId;
				uint32_t requestId = ServerWorker::idGenerator->nextVal( this->workerId );
				chunkRequest.set(
					listId, stripeId, chunkId, target,
					0 /* chunk */, false /* isDegraded */
				);
				if ( ! ServerWorker::pending->insertChunkRequest( PT_SERVER_PEER_SET_CHUNK, instanceId, pid.parentInstanceId, requestId, pid.parentRequestId, target, chunkRequest ) ) {
					__ERROR__( "ServerWorker", "handleGetChunkResponse", "Cannot insert into server CHUNK_REQUEST pending map." );
				}

				metadata.set( listId, stripeId, chunkId );

				event.reqSetChunk( target, instanceId, requestId, metadata, this->chunks[ chunkId ], false );
				this->dispatch( event );
			} else {
				__ERROR__( "ServerWorker", "handleGetChunkResponse", "Cannot found a pending reconstruction request for (%u, %u).\n", listId, stripeId );
			}
		}

		// Return chunks to chunk pool
		for ( uint32_t i = 0; i < ServerWorker::chunkCount; i++ ) {
			if ( this->chunks[ i ] == toBeFreed )
				continue;

			// Check whether the chunk is reconstructed, forwarded, or from GET_CHUNK requests
			if ( ! ServerWorker::chunkPool->isInChunkPool( this->chunks[ i ] ) ) {
				Metadata m;
				uint32_t s;
				ChunkUtil::get( this->chunks[ i ], m.listId, m.stripeId, m.chunkId, s );
				if ( dmap->findChunkById( m.listId, m.stripeId, m.chunkId ) == this->chunks[ i ] ) {
					// Reconstructed - need to keep the chunk
				} else {
					// Forwarded or from GET_CHUNK requests
					this->tempChunkPool.free( this->chunks[ i ] );
				}
			}
		}
	}
	if ( toBeFreed )
		this->tempChunkPool.free( toBeFreed );
	return true;
}

bool ServerWorker::handleSetChunkResponse( ServerPeerEvent event, bool success, char *buf, size_t size ) {
	struct ChunkHeader header;
	if ( ! this->protocol.parseChunkHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleSetChunkResponse", "Invalid SET_CHUNK response." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleSetChunkResponse",
		"[SET_CHUNK (%s)] List ID: %u, stripe ID: %u, chunk ID: %u.",
		success ? "success" : "failure",
		header.listId, header.stripeId, header.chunkId
	);

	PendingIdentifier pid;
	ChunkRequest chunkRequest;
	uint16_t instanceId = Server::instanceId;

	chunkRequest.set(
		header.listId, header.stripeId, header.chunkId,
		event.socket, 0 // ptr
	);

	if ( ! ServerWorker::pending->eraseChunkRequest( PT_SERVER_PEER_SET_CHUNK, instanceId, event.requestId, event.socket, &pid, &chunkRequest ) ) {
		__ERROR__( "ServerWorker", "handleSetChunkResponse", "Cannot find a pending server SET_CHUNK request that matches the response. This message will be discarded." );
	}

	if ( chunkRequest.isDegraded ) {
		// Release degraded lock
		uint32_t remaining, total;
		if ( ! ServerWorker::pending->eraseReleaseDegradedLock( pid.parentInstanceId, pid.parentRequestId, 1, remaining, total, &pid ) ) {
			__ERROR__( "ServerWorker", "handleSetChunkResponse", "Cannot find a pending coordinator release degraded lock request that matches the response. The message will be discarded." );
			return false;
		}

		if ( remaining == 0 ) {
			// Tell the coordinator that all degraded lock is released
			CoordinatorEvent coordinatorEvent;
			coordinatorEvent.resReleaseDegradedLock( ( CoordinatorSocket * ) pid.ptr, pid.instanceId, pid.requestId, total );
			ServerWorker::eventQueue->insert( coordinatorEvent );
			__DEBUG__( YELLOW, "ServerWorker", "handleSetChunkResponse", "End of release degraded lock requests id = %u, total = %u.", pid.requestId, total );
		}
	} else {
		// Reconstruction
		uint32_t remainingChunks, remainingKeys, totalChunks, totalKeys;
		CoordinatorSocket *socket;
		if ( ! ServerWorker::pending->eraseReconstruction( pid.parentInstanceId, pid.parentRequestId, socket, header.listId, header.stripeId, header.chunkId, remainingChunks, remainingKeys, totalChunks, totalKeys, &pid ) ) {
			__ERROR__( "ServerWorker", "handleSetChunkResponse", "Cannot find a pending coordinator RECONSTRUCTION request that matches the response. The message will be discarded." );
			return false;
		}

		if ( remainingChunks == 0 ) {
			// Tell the coordinator that the reconstruction task is completed
			CoordinatorEvent coordinatorEvent;
			coordinatorEvent.resReconstruction(
				socket, pid.instanceId, pid.requestId,
				header.listId, header.chunkId, totalChunks /* numStripes */
			);
			ServerWorker::eventQueue->insert( coordinatorEvent );
		}
	}

	return true;
}

bool ServerWorker::handleUpdateChunkResponse( ServerPeerEvent event, bool success, char *buf, size_t size ) {
	struct ChunkUpdateHeader header;
	if ( ! this->protocol.parseChunkUpdateHeader( header, false, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleUpdateChunkResponse", "Invalid UPDATE_CHUNK response." );
		return false;
	}
	if ( ! success ) {
		__ERROR__(
			"ServerWorker", "handleUpdateChunkResponse",
			"[UPDATE_CHUNK (%s)] From id: %d List ID: %u, stripe ID: %u, chunk ID: %u; offset = %u, length = %u.",
			success ? "success" : "failed",
			event.socket->instanceId,
			header.listId, header.stripeId, header.chunkId,
			header.offset, header.length
		);
	}

	int pending;
	ChunkUpdate chunkUpdate;
	PendingIdentifier pid;
	uint16_t instanceId = Server::instanceId;

	if ( ! ServerWorker::pending->eraseChunkUpdate( PT_SERVER_PEER_UPDATE_CHUNK, instanceId, event.requestId, event.socket, &pid, &chunkUpdate, true, false ) ) {
		UNLOCK( &ServerWorker::pending->serverPeers.updateChunkLock );
		__ERROR__( "ServerWorker", "handleUpdateChunkResponse", "Cannot find a pending server UPDATE_CHUNK request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	// Check pending server UPDATE requests
	pending = ServerWorker::pending->count( PT_SERVER_PEER_UPDATE_CHUNK, pid.instanceId, pid.requestId, false, true );

	__DEBUG__( GREEN, "ServerWorker", "handleUpdateChunkResponse", "[%u, %u, %u] Pending server UPDATE_CHUNK requests = %d (%s).", header.listId, header.stripeId, header.chunkId, pending, success ? "success" : "fail" );

	if ( pending == 0 ) {
		// Only send application UPDATE response when the number of pending server UPDATE_CHUNK requests equal 0
		Key key;
		KeyValueUpdate keyValueUpdate;
		ClientEvent clientEvent;

		if ( ! ServerWorker::pending->eraseKeyValueUpdate( PT_CLIENT_UPDATE, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &keyValueUpdate ) ) {
			__ERROR__( "ServerWorker", "handleUpdateChunkResponse", "Cannot find a pending client UPDATE request that matches the response. This message will be discarded." );
			return false;
		}

		key.set( keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.ptr );
		clientEvent.resUpdate(
			( ClientSocket * ) pid.ptr, pid.instanceId, pid.requestId, key,
			keyValueUpdate.offset, keyValueUpdate.length,
			success, true,
			keyValueUpdate.isDegraded // isDegraded
		);
		this->dispatch( clientEvent );
	}
	return true;
}

bool ServerWorker::handleDeleteChunkResponse( ServerPeerEvent event, bool success, char *buf, size_t size ) {
	struct ChunkUpdateHeader header;
	if ( ! this->protocol.parseChunkUpdateHeader( header, false, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleDeleteChunkResponse", "Invalid DELETE_CHUNK response." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleDeleteChunkResponse",
		"[DELETE_CHUNK] List ID: %u, stripe ID: %u, chunk ID: %u; offset = %u, length = %u.",
		header.listId, header.stripeId, header.chunkId,
		header.offset, header.length
	);

	int pending;
	ChunkUpdate chunkUpdate;
	PendingIdentifier pid;
	uint16_t instanceId = Server::instanceId;

	chunkUpdate.set(
		header.listId, header.stripeId, header.updatingChunkId,
		header.offset, header.length
	);
	chunkUpdate.setKeyValueUpdate( 0, 0, 0 );
	chunkUpdate.ptr = ( void * ) event.socket;

	if ( ! ServerWorker::pending->eraseChunkUpdate( PT_SERVER_PEER_DEL_CHUNK, instanceId, event.requestId, event.socket, &pid, &chunkUpdate, true, false ) ) {
		UNLOCK( &ServerWorker::pending->serverPeers.deleteChunkLock );
		__ERROR__( "ServerWorker", "handleDeleteChunkResponse", "Cannot find a pending server DELETE_CHUNK request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	// erase data delta backup
	Server *server = Server::getInstance();
	LOCK( &server->sockets.clientsIdToSocketLock );
	try {
		ClientSocket *clientSocket = server->sockets.clientsIdToSocketMap.at( event.instanceId );
		clientSocket->backup.removeDataDelete( event.requestId, event.instanceId, event.socket );
	} catch ( std::out_of_range &e ) {
		__ERROR__( "ServerWorker", "handleDeleteChunkResponse", "Cannot find a pending parity server UPDATE backup for instance ID = %hu, request ID = %u. (Socket mapping not found)", event.instanceId, event.requestId );
	}
	UNLOCK( &server->sockets.clientsIdToSocketLock );

	// Check pending server UPDATE requests
	pending = ServerWorker::pending->count( PT_SERVER_PEER_DEL_CHUNK, pid.instanceId, pid.requestId, false, true );

	// __ERROR__( "ServerWorker", "handleDeleteChunkResponse", "Pending server DELETE_CHUNK requests = %d.", pending );
	if ( pending == 0 ) {
		// Only send client DELETE response when the number of pending server DELETE_CHUNK requests equal 0
		ClientEvent clientEvent;
		Key key;

		if ( ! ServerWorker::pending->eraseKey( PT_CLIENT_DEL, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &key ) ) {
			__ERROR__( "ServerWorker", "handleDeleteChunkResponse", "Cannot find a pending client DELETE request that matches the response. This message will be discarded." );
			return false;
		}

		// TODO: Include the timestamp and metadata in the response
		if ( success ) {
			uint32_t timestamp = ServerWorker::timestamp->nextVal();
			clientEvent.resDelete(
				( ClientSocket * ) pid.ptr, pid.instanceId, pid.requestId,
				timestamp,
				header.listId, header.stripeId, header.chunkId,
				key,
				true, // needsFree
				false // isDegraded
			);
		} else {
			clientEvent.resDelete(
				( ClientSocket * ) pid.ptr, pid.instanceId, pid.requestId,
				key,
				true, // needsFree
				false // isDegraded
			);
		}
		ServerWorker::eventQueue->insert( clientEvent );
	}
	return true;
}

bool ServerWorker::handleSealChunkResponse( ServerPeerEvent event, bool success, char *buf, size_t size ) {
	return true;
}

bool ServerWorker::handleBatchKeyValueResponse( ServerPeerEvent event, bool success, char *buf, size_t size ) {
	struct BatchKeyHeader header;
	if ( ! this->protocol.parseBatchKeyHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleBatchKeyValueResponse", "Invalid BATCH_KEY_VALUE response." );
		return false;
	}

	PendingIdentifier pid;
	CoordinatorSocket *coordinatorSocket;
	if ( ! ServerWorker::pending->erase( PT_SERVER_PEER_FORWARD_KEYS, event.instanceId, event.requestId, event.socket, &pid ) ) {
		__ERROR__( "ServerWorker", "handleBatchKeyValueResponse", "Cannot insert find pending BATCH_KEY_VALUE request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
	}

	uint32_t listId, chunkId;
	uint32_t remainingChunks, remainingKeys, totalChunks, totalKeys;
	for ( uint32_t i = 0, offset = 0; i < header.count; i++ ) {
		uint8_t keySize;
		char *keyStr;

		this->protocol.nextKeyInBatchKeyHeader( header, keySize, keyStr, offset );

		if ( i == 0 ) {
			// Find the corresponding reconstruction request
			if ( ! ServerWorker::pending->findReconstruction(
				pid.parentInstanceId, pid.parentRequestId,
				keySize, keyStr,
				listId, chunkId
			) ) {
				__ERROR__( "ServerWorker", "handleBatchKeyValueResponse", "Cannot find a matching reconstruction request. (ID: (%u, %u))", pid.parentInstanceId, pid.parentRequestId );
				return false;
			}
		}

		if ( ! ServerWorker::pending->eraseReconstruction(
			pid.parentInstanceId, pid.parentRequestId, coordinatorSocket,
			listId, chunkId,
			keySize, keyStr,
			remainingChunks, remainingKeys,
			totalChunks, totalKeys,
			&pid
		) ) {
			__ERROR__(
				"ServerWorker", "handleBatchKeyValueResponse",
				"Cannot erase the key: %.*s for the corresponding reconstruction request. (ID: (%u, %u))",
				keySize, keyStr,
				pid.parentInstanceId, pid.parentRequestId
			);
		}
	}

	if ( remainingKeys == 0 ) {
		CoordinatorEvent coordinatorEvent;
		coordinatorEvent.resReconstructionUnsealed(
			coordinatorSocket, pid.instanceId, pid.requestId,
			listId, chunkId, totalKeys
		);
		ServerWorker::eventQueue->insert( coordinatorEvent );
	}

	return false;
}
