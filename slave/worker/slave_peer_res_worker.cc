#include "worker.hh"
#include "../main/slave.hh"

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

	if ( ! SlaveWorker::pending->eraseKey( PT_SLAVE_PEER_GET, event.instanceId, event.requestId, event.socket, &pid ) ) {
		__ERROR__( "SlaveWorker", "handleGetResponse", "Cannot find a pending slave UNSEALED_GET request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		if ( success ) keyValue.free();
		return false;
	}

	if ( ! dmap->deleteDegradedKey( key, pids ) ) {
		__ERROR__( "SlaveWorker", "handleGetResponse", "SlaveWorker::degradedChunkBuffer->deleteDegradedChunk() failed." );
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
				uint32_t timestamp;
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
				if ( success ) {
					masterEvent.resGet( op.socket, pid.parentInstanceId, pid.parentRequestId, keyValue, true );
				} else {
					masterEvent.resGet( op.socket, pid.parentInstanceId, pid.parentRequestId, key, true );
				}
				op.data.key.free();
				this->dispatch( masterEvent );
				break;
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
						true /* isUpdate */
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
						__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot insert into master DELETE pending map." );
					}

					this->sendModifyChunkRequest(
						pid.parentInstanceId, pid.parentRequestId, key.size, key.data,
						metadata,
						// not needed for deleting a key-value pair in an unsealed chunk:
						0, 0, 0, 0,
						false /* isSealed */,
						false /* isUpdate */
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
	MasterSocket *masterSocket = Slave::getInstance()->sockets.mastersIdToSocketMap.get( event.instanceId );
	if ( masterSocket )
		masterSocket->backup.removeDataUpdate( event.requestId, event.socket );
	else
		__ERROR__( "SlaveWorker", "handleUpdateResponse", "Cannot find a pending parity slave UPDATE backup for instance ID = %hu, request ID = %u", event.instanceId, event.requestId );

	// Check pending slave UPDATE requests
	pending = SlaveWorker::pending->count( PT_SLAVE_PEER_UPDATE, pid.instanceId, pid.requestId, false, true );

	__DEBUG__( BLUE, "SlaveWorker", "handleUpdateResponse", "Pending slave UPDATE requests = %d (%s).", pending, success ? "success" : "fail" );

	if ( pending == 0 ) {
		// Only send master DELETE response when the number of pending slave DELETE requests equal 0
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
		SlaveWorker::eventQueue->insert( masterEvent );
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
	MasterSocket *masterSocket = Slave::getInstance()->sockets.mastersIdToSocketMap.get( event.instanceId );
	if ( masterSocket )
		masterSocket->backup.removeDataDelete( event.requestId, event.socket );
	else
		__ERROR__( "SlaveWorker", "handleDeleteResponse", "Cannot find a pending parity slave UPDATE backup for instance ID = %hu, request ID = %u", event.instanceId, event.requestId );

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
			printf( "TODO: slave/worker/slave_peer_res_worker.cc - Line 289: Include the timestamp and metadata in the response.\n" );
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
				// __ERROR__( "SlaveWorker", "handleGetChunkResponse", "The GET_CHUNK request (%u, %u, %u) is failed.", header.chunk.listId, header.chunk.stripeId, header.chunk.chunkId );
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
	// __ERROR__( "SlaveWorker", "handleGetChunkResponse", "Pending slave GET_CHUNK requests = %d (%s).", pending, success ? "success" : "fail" );
	if ( pending == 0 )
		SlaveWorker::pending->slavePeers.getChunk.erase( it, tmp );
	UNLOCK( &SlaveWorker::pending->slavePeers.getChunkLock );

	if ( pending == 0 ) {
		// printf( "Reconstructing: %u, %u, { ", listId, stripeId );

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
				// printf( "%u ", i );
				this->chunkStatus->set( i );
			}
		}
		// printf( "}\n" );
		// fflush( stdout );

		// Decode to reconstruct the lost chunk
		CodingEvent codingEvent;
		codingEvent.decode( this->chunks, this->chunkStatus );
		this->dispatch( codingEvent );

		uint32_t maxChunkSize = 0, chunkSize;
		for ( uint32_t i = 0; i < SlaveWorker::dataChunkCount; i++ ) {
			chunkSize = this->chunks[ i ]->status == CHUNK_STATUS_RECONSTRUCTED ?
			            this->chunks[ i ]->updateData() :
			            this->chunks[ i ]->getSize();
			if ( chunkSize > maxChunkSize )
				maxChunkSize = chunkSize;
			if ( chunkSize > Chunk::capacity ) {
				__ERROR__(
					"SlaveWorker", "handleGetChunkResponse",
					"[%s] Invalid chunk size (%u, %u, %u): %u",
					this->chunks[ i ]->status == CHUNK_STATUS_RECONSTRUCTED ? "Reconstructed" : "Normal",
					chunkRequest.listId, chunkRequest.stripeId, i,
					chunkSize
				);
			// } else if ( this->chunks[ i ]->status == CHUNK_STATUS_RECONSTRUCTED ) {
			// 	printf( "Successfully reconstructed: (%u, %u, %u): %u.\n", chunkRequest.listId, chunkRequest.stripeId, i, chunkSize );
			}
		}

		for ( uint32_t i = SlaveWorker::dataChunkCount; i < SlaveWorker::chunkCount; i++ ) {
			if ( this->chunks[ i ]->status == CHUNK_STATUS_RECONSTRUCTED ) {
				this->chunks[ i ]->isParity = true;
				this->chunks[ i ]->setSize( maxChunkSize );
			// } else if ( this->chunks[ i ]->getSize() != maxChunkSize ) {
			// 	__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Parity chunk size mismatch (%u, %u, %u): %u vs. %u.", chunkRequest.listId, chunkRequest.stripeId, i, this->chunks[ i ]->getSize(), maxChunkSize );
			}
		}

		if ( chunkRequest.isDegraded ) {
			// Respond the original GET/UPDATE/DELETE operation using the reconstructed data
			PendingIdentifier pid;
			DegradedOp op;

			if ( ! SlaveWorker::pending->eraseDegradedOp( PT_SLAVE_PEER_DEGRADED_OPS, event.instanceId, event.requestId, event.socket, &pid, &op ) ) {
				__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot find a pending slave DEGRADED_OPS request that matches the response. This message will be discarded." );
			} else {
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

					// Find the chunk from the map
					Chunk *chunk = dmap->findChunkById( op.listId, op.stripeId, op.chunkId );

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
						if ( isKeyValueFound ) {
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
								true /* isUpdate */
							);

							delete[] valueUpdate;
						} else {
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

							SlaveWorker::degradedChunkBuffer->deleteKey(
								PROTO_OPCODE_DELETE, timestamp,
								key.size, key.data,
								metadata,
								true /* isSealed */,
								deltaSize, delta, chunk
							);

							this->sendModifyChunkRequest(
								pid.parentInstanceId, pid.parentRequestId, key.size, key.data,
								metadata, keyMetadata.offset, deltaSize, 0, delta,
								true /* isSealed */,
								false /* isUpdate */
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
		uint32_t remaining, total;
		CoordinatorSocket *socket;
		if ( ! SlaveWorker::pending->eraseReconstruction( pid.parentInstanceId, pid.parentRequestId, socket, header.listId, header.stripeId, header.chunkId, remaining, total, &pid ) ) {
			__ERROR__( "SlaveWorker", "handleSetChunkResponse", "Cannot find a pending coordinator RECONSTRUCTION request that matches the response. The message will be discarded." );
			return false;
		}

		if ( remaining == 0 ) {
			// Tell the coordinator that the reconstruction task is completed
			CoordinatorEvent coordinatorEvent;
			coordinatorEvent.resReconstruction(
				socket, pid.instanceId, pid.requestId,
				header.listId, header.chunkId, total /* numStripes */
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
		"[UPDATE_CHUNK] List ID: %u, stripe ID: %u, chunk ID: %u; offset = %u, length = %u.",
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
	MasterSocket *masterSocket = Slave::getInstance()->sockets.mastersIdToSocketMap.get( event.instanceId );
	if ( masterSocket )
		masterSocket->backup.removeDataUpdate( event.requestId, event.socket );
	else
		__ERROR__( "SlaveWorker", "handleUpdateChunkResponse", "Cannot find a pending parity slave UPDATE backup for instance ID = %hu, request ID = %u", event.instanceId, event.requestId );

	// Check pending slave UPDATE requests
	pending = SlaveWorker::pending->count( PT_SLAVE_PEER_UPDATE_CHUNK, pid.instanceId, pid.requestId, false, true );

	__DEBUG__( BLUE, "SlaveWorker", "handleUpdateChunkResponse", "Pending slave UPDATE_CHUNK requests = %d (%s).", pending, success ? "success" : "fail" );

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
			success, true, false
		);
		SlaveWorker::eventQueue->insert( masterEvent );
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
		UNLOCK( &SlaveWorker::pending->slavePeers.delChunkLock );
		__ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Cannot find a pending slave DELETE_CHUNK request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	// erase data delta backup
	MasterSocket *masterSocket = Slave::getInstance()->sockets.mastersIdToSocketMap.get( event.instanceId );
	if ( masterSocket )
		masterSocket->backup.removeDataDelete( event.requestId, event.socket );
	else
		__ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Cannot find a pending parity slave UPDATE backup for instance ID = %hu, request ID = %u", event.instanceId, event.requestId );

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
