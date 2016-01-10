#include "worker.hh"
#include "../main/slave.hh"

void SlaveWorker::dispatch( MasterEvent event ) {
	bool success = true, connected, isSend = true;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	if ( SlaveWorker::delay )
		usleep( SlaveWorker::delay );

	switch( event.type ) {
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_GET_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS_DATA:
		case MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS_PARITY:
		case MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_ACK_METADATA:
		case MASTER_EVENT_TYPE_ACK_PARITY_BACKUP:
		case MASTER_EVENT_TYPE_REVERT_PARITY_DELTA_SUCCESS:
			success = true;
			break;
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_GET_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_REVERT_PARITY_DELTA_FAILURE:
			success = false;
			break;
		default:
			isSend = false;
			break;
	}

	switch( event.type ) {
		// Register
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
			if ( Slave::instanceId == 0 ) {
				// Wait until the slave get an instance ID
				SlaveWorker::eventQueue->insert( event );
				return;
			}
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRegisterMaster( buffer.size, Slave::instanceId, event.requestId, success );
			break;
		// GET
		case MASTER_EVENT_TYPE_GET_RESPONSE_SUCCESS:
		{
			char *key, *value;
			uint8_t keySize;
			uint32_t valueSize;
			event.message.keyValue.deserialize( key, keySize, value, valueSize );
			buffer.data = this->protocol.resGet(
				buffer.size,
				event.instanceId, event.requestId,
				success,
				event.isDegraded,
				keySize, key,
				valueSize, value
			);
		}
			break;
		case MASTER_EVENT_TYPE_GET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resGet(
				buffer.size,
				event.instanceId, event.requestId,
				success,
				event.isDegraded,
				event.message.key.size,
				event.message.key.data
			);
			break;
		// SET
		case MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS_DATA:
			buffer.data = this->protocol.resSet(
				buffer.size,
				event.instanceId, event.requestId,
				event.message.set.timestamp,
				event.message.set.listId,
				event.message.set.stripeId,
				event.message.set.chunkId,
				event.message.set.isSealed,
				event.message.set.sealedListId,
				event.message.set.sealedStripeId,
				event.message.set.sealedChunkId,
				event.message.set.key.size,
				event.message.set.key.data
			);
			break;
		case MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS_PARITY:
		case MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resSet(
				buffer.size,
				event.instanceId, event.requestId,
				success,
				event.message.set.key.size,
				event.message.set.key.data
			);
			break;
		case MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRemappingSet(
				buffer.size,
				true,
				event.instanceId, event.requestId,
				success,
				event.message.remap.listId,
				event.message.remap.chunkId,
				event.message.remap.original,
				event.message.remap.remapped,
				event.message.remap.remappedCount,
				event.message.remap.key.size,
				event.message.remap.key.data
			);

			if ( event.needsFree )
				event.message.keyValueUpdate.key.free();
			break;
		// UPDATE
		case MASTER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
			buffer.data = this->protocol.resUpdate(
				buffer.size,
				event.instanceId, event.requestId,
				success,
				event.isDegraded,
				event.message.keyValueUpdate.key.size,
				event.message.keyValueUpdate.key.data,
				event.message.keyValueUpdate.valueUpdateOffset,
				event.message.keyValueUpdate.valueUpdateSize
			);

			if ( event.needsFree )
				event.message.keyValueUpdate.key.free();
			break;
		// DELETE
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resDelete(
				buffer.size,
				event.instanceId, event.requestId,
				event.isDegraded,
				event.message.del.timestamp,
				event.message.del.listId,
				event.message.del.stripeId,
				event.message.del.chunkId,
				event.message.del.key.size,
				event.message.del.key.data
			);

			if ( event.needsFree )
				event.message.del.key.free();
			break;
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
			buffer.data = this->protocol.resDelete(
				buffer.size,
				event.instanceId, event.requestId,
				event.isDegraded,
				event.message.del.key.size,
				event.message.del.key.data
			);

			if ( event.needsFree )
				event.message.del.key.free();
			break;
		// ACK
		case MASTER_EVENT_TYPE_ACK_METADATA:
			buffer.data = this->protocol.ackMetadata(
				buffer.size,
				event.instanceId, event.requestId,
				event.message.ack.fromTimestamp,
				event.message.ack.toTimestamp
			);
			break;
		case MASTER_EVENT_TYPE_ACK_PARITY_BACKUP:
			buffer.data = this->protocol.ackParityDeltaBackup(
				buffer.size,
				Slave::instanceId, event.requestId,
				event.message.revert.fromTimestamp,
				event.message.revert.toTimestamp,
				event.message.revert.targetId
			);
			break;
		case MASTER_EVENT_TYPE_REVERT_PARITY_DELTA_SUCCESS:
		case MASTER_EVENT_TYPE_REVERT_PARITY_DELTA_FAILURE:
			buffer.data = this->protocol.resRevertParityDelta(
				buffer.size,
				Slave::instanceId, event.requestId,
				success,
				event.message.revert.fromTimestamp,
				event.message.revert.toTimestamp,
				event.message.revert.targetId
			);
			break;
		// Pending
		case MASTER_EVENT_TYPE_PENDING:
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "SlaveWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else {
		// Parse requests from masters
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "SlaveWorker (master)" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.magic != PROTO_MAGIC_REQUEST || header.from != PROTO_MAGIC_FROM_MASTER ) {
				__ERROR__( "SlaveWorker", "dispatch", "Invalid protocol header." );
			} else {
				event.instanceId = header.instanceId;
				event.requestId = header.requestId;
				event.timestamp = header.timestamp;
				switch( header.opcode ) {
					case PROTO_OPCODE_GET:
						this->handleGetRequest( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_SET:
						this->handleSetRequest( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_REMAPPING_SET:
						this->handleRemappingSetRequest( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_UPDATE:
						this->handleUpdateRequest( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_DELETE:
						this->handleDeleteRequest( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_DEGRADED_GET:
						this->handleDegradedGetRequest( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_DEGRADED_UPDATE:
						this->handleDegradedUpdateRequest( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_DEGRADED_DELETE:
						this->handleDegradedDeleteRequest( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_ACK_PARITY_DELTA:
						this->handleAckParityDeltaBackup( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_REVERT_PARITY_DELTA:
						this->handleRevertParityDelta( event, buffer.data, buffer.size );
						break;
					default:
						__ERROR__( "SlaveWorker", "dispatch", "Invalid opcode from master." );
						break;
				}
			}
			buffer.data += header.length;
			buffer.size -= header.length;
		}
		if ( connected ) event.socket->done();
	}
	if ( ! connected )
		__ERROR__( "SlaveWorker", "dispatch", "The master is disconnected." );
}

bool SlaveWorker::handleGetRequest( MasterEvent event, char *buf, size_t size ) {
	struct KeyHeader header;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleGetRequest", "Invalid GET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleGetRequest",
		"[GET] Key: %.*s (key size = %u).",
		( int ) header.keySize, header.key, header.keySize
	);
	return this->handleGetRequest( event, header, false );
}

bool SlaveWorker::handleGetRequest( MasterEvent event, struct KeyHeader &header, bool isDegraded ) {
	Key key;
	KeyValue keyValue;
	RemappedKeyValue remappedKeyValue;
	bool ret;
	if ( map->findValueByKey( header.key, header.keySize, &keyValue, &key ) ) {
		event.resGet( event.socket, event.instanceId, event.requestId, keyValue, isDegraded );
		ret = true;
	} else if ( remappedBuffer->find( header.keySize, header.key, &remappedKeyValue ) ) {
		// Handle remapped keys
		event.resGet( event.socket, event.instanceId, event.requestId, remappedKeyValue.keyValue, isDegraded );
		ret = true;
	} else {
		event.resGet( event.socket, event.instanceId, event.requestId, key, isDegraded );
		ret = false;
	}
	this->dispatch( event );
	return ret;
}

bool SlaveWorker::handleSetRequest( MasterEvent event, char *buf, size_t size, bool needResSet ) {
	struct KeyValueHeader header;
	if ( ! this->protocol.parseKeyValueHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleSetRequest", "Invalid SET request (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleSetRequest",
		"[SET] Key: %.*s (key size = %u); Value: (value size = %u)",
		( int ) header.keySize, header.key, header.keySize, header.valueSize
	);
	return this->handleSetRequest( event, header, needResSet );
}

bool SlaveWorker::handleSetRequest( MasterEvent event, struct KeyValueHeader &header, bool needResSet ) {
	bool isSealed;
	Metadata sealed;
	uint32_t timestamp, listId, stripeId, chunkId;
	SlavePeerSocket *dataSlaveSocket;

	listId = SlaveWorker::stripeList->get( header.key, header.keySize, &dataSlaveSocket, 0, &chunkId );

	if ( SlaveWorker::disableSeal ) {
		SlaveWorker::chunkBuffer->at( listId )->set(
			this,
			header.key, header.keySize,
			header.value, header.valueSize,
			PROTO_OPCODE_SET, timestamp, // generated by DataChunkBuffer
			stripeId, chunkId,
			0, 0,
			this->chunks, this->dataChunk, this->parityChunk
		);
		isSealed = false;
	} else {
		SlaveWorker::chunkBuffer->at( listId )->set(
			this,
			header.key, header.keySize,
			header.value, header.valueSize,
			PROTO_OPCODE_SET, timestamp, // generated by DataChunkBuffer
			stripeId, chunkId,
			&isSealed, &sealed,
			this->chunks, this->dataChunk, this->parityChunk
		);
	}

	if ( ! needResSet )
		return true;

	Key key;
	key.set( header.keySize, header.key );
	if ( ! dataSlaveSocket->self ) {
		event.resSet(
			event.socket, event.instanceId, event.requestId,
			key, true
		);
	} else {
		// Data server responds with metadata
		event.resSet(
			event.socket, event.instanceId, event.requestId,
			timestamp, listId, stripeId, chunkId,
			isSealed, sealed.listId, sealed.stripeId, sealed.chunkId,
			key
		);
	}
	this->dispatch( event );

	return true;
}

bool SlaveWorker::handleUpdateRequest( MasterEvent event, char *buf, size_t size ) {
	struct KeyValueUpdateHeader header;
	if ( ! this->protocol.parseKeyValueUpdateHeader( header, true, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleUpdateRequest", "Invalid UPDATE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleUpdateRequest",
		"[UPDATE] Key: %.*s (key size = %u); Value: (update size = %u, offset = %u).",
		( int ) header.keySize, header.key, header.keySize,
		header.valueUpdateSize, header.valueUpdateOffset
	);
	return this->handleUpdateRequest( event, header );
}

bool SlaveWorker::handleUpdateRequest( MasterEvent event, struct KeyValueUpdateHeader &header ) {
	bool ret;
	Key key;
	KeyValue keyValue;
	KeyValueUpdate keyValueUpdate;
	KeyMetadata keyMetadata;
	Metadata metadata;
	RemappedKeyValue remappedKeyValue;
	Chunk *chunk;
	if ( map->findValueByKey( header.key, header.keySize, &keyValue, &key, &keyMetadata, &metadata, &chunk ) ) {
		uint32_t offset = keyMetadata.offset + PROTO_KEY_VALUE_SIZE + header.keySize + header.valueUpdateOffset;

		if ( SlaveWorker::parityChunkCount ) {
			// Add the current request to the pending set
			keyValueUpdate.dup( header.keySize, header.key, ( void * ) event.socket );
			keyValueUpdate.offset = header.valueUpdateOffset;
			keyValueUpdate.length = header.valueUpdateSize;

			if ( ! SlaveWorker::pending->insertKeyValueUpdate( PT_MASTER_UPDATE, event.instanceId, event.requestId, ( void * ) event.socket, keyValueUpdate ) ) {
				__ERROR__( "SlaveWorker", "handleUpdateRequest", "Cannot insert into master UPDATE pending map (ID = (%u, %u)).", event.instanceId, event.requestId );
			}
		} else {
			key.set( header.keySize, header.key, ( void * ) event.socket );
		}

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
			header.valueUpdate, // delta
			header.valueUpdate, // new data
			offset, header.valueUpdateSize,
			true // perform update
		);
		// Release the locks
		UNLOCK( cacheLock );
		UNLOCK( keysLock );
		if ( chunkBufferIndex == -1 )
			chunkBuffer->unlock();

		if ( SlaveWorker::parityChunkCount ) {
			ret = this->sendModifyChunkRequest(
				event.instanceId, event.requestId, keyValueUpdate.size, keyValueUpdate.data,
				metadata, offset,
				header.valueUpdateSize,    /* deltaSize */
				header.valueUpdateOffset,
				header.valueUpdate,        /* delta */
				chunkBufferIndex == -1,    /* isSealed */
				true,                      /* isUpdate */
				event.timestamp,
				event.socket
			);
		} else {
			event.resUpdate(
				event.socket, event.instanceId, event.requestId, key,
				header.valueUpdateOffset,
				header.valueUpdateSize,
				true, false, false
			);
			this->dispatch( event );
			ret = true;
		}
		if ( chunkBufferIndex != -1 )
			chunkBuffer->updateAndUnlockChunk( chunkBufferIndex );
	} else if ( remappedBuffer->update( header.keySize, header.key, header.valueUpdateSize, header.valueUpdateOffset, header.valueUpdate, &remappedKeyValue ) ) {
		// Handle remapped key
		__DEBUG__( GREEN, "SlaveWorker", "handleUpdateRequest", "Handle remapped key: %.*s!", header.keySize, header.key );

		if ( SlaveWorker::parityChunkCount ) {
			// Add the current request to the pending set
			keyValueUpdate.dup( header.keySize, header.key, ( void * ) event.socket );
			keyValueUpdate.offset = header.valueUpdateOffset;
			keyValueUpdate.length = header.valueUpdateSize;

			if ( ! SlaveWorker::pending->insertKeyValueUpdate( PT_MASTER_UPDATE, event.instanceId, event.requestId, ( void * ) event.socket, keyValueUpdate ) ) {
				__ERROR__( "SlaveWorker", "handleUpdateRequest", "Cannot insert into master UPDATE pending map (ID = (%u, %u)).", event.instanceId, event.requestId );
			}

			// Prepare the list of parity servers
			this->getSlaves( remappedKeyValue.listId );
			for ( uint32_t i = 0; i < remappedKeyValue.remappedCount; i++ ) {
				uint32_t srcChunkId = remappedKeyValue.original[ i * 2 + 1 ];

				if ( srcChunkId >= SlaveWorker::dataChunkCount ) {
					this->paritySlaveSockets[ srcChunkId - SlaveWorker::dataChunkCount ] = SlaveWorker::stripeList->get(
						remappedKeyValue.remapped[ i * 2     ],
						remappedKeyValue.remapped[ i * 2 + 1 ]
					);
				}
			}

			// Prepare UPDATE request
			size_t size;
			uint16_t instanceId = Slave::instanceId;
			uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
			Packet *packet = SlaveWorker::packetPool->malloc();
			packet->setReferenceCount( SlaveWorker::parityChunkCount );
			this->protocol.reqRemappedUpdate(
				size,
				event.instanceId, // master ID
				requestId,        // slave request ID
				header.key,
				header.keySize,
				header.valueUpdate,
				header.valueUpdateOffset,
				header.valueUpdateSize,
				packet->data,
				event.timestamp
			);
			packet->size = ( uint32_t ) size;

			// Insert the UPDATE request to slave pending set
			for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
				// backup data delta, insert a pending record for each parity slave
				// if ( masterSocket != 0 ) {
				// 	Timestamp timestamp ( event.timestamp );
				// 	Value value;
				// 	value.set( deltaSize, delta );
				// 	if ( isUpdate )
				// 		masterSocket->backup.insertDataUpdate( timestamp , key, value, metadata, isSealed, 0 /* TODO */, offset, requestId, this->paritySlaveSockets[ i ] );
				// 	else
				// 		masterSocket->backup.insertDataDelete( timestamp , key, value, metadata, isSealed, 0 /* TODO */, offset, requestId, this->paritySlaveSockets[ i ] );
				// }

				if ( ! SlaveWorker::pending->insertKeyValueUpdate(
					PT_SLAVE_PEER_UPDATE, instanceId, event.instanceId, requestId, event.requestId,
					( void * ) this->paritySlaveSockets[ i ],
					keyValueUpdate
				) ) {
					__ERROR__( "SlaveWorker", "handleUpdateRequest", "Cannot insert into slave UPDATE pending map." );
				}
			}

			// Forward the request to the parity servers
			for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
				// Insert into event queue
				SlavePeerEvent slavePeerEvent;
				slavePeerEvent.send( this->paritySlaveSockets[ i ], packet );

#ifdef SLAVE_WORKER_SEND_REPLICAS_PARALLEL
				if ( i == SlaveWorker::parityChunkCount - 1 )
					this->dispatch( slavePeerEvent );
				else
					SlaveWorker::eventQueue->prioritizedInsert( slavePeerEvent );
#else
				this->dispatch( slavePeerEvent );
#endif
			}
		} else {
			key.set( header.keySize, header.key, ( void * ) event.socket );
			event.resUpdate(
				event.socket, event.instanceId, event.requestId, key,
				header.valueUpdateOffset,
				header.valueUpdateSize,
				true, false, false
			);
		}
		ret = true;
	} else {
		event.resUpdate(
			event.socket, event.instanceId, event.requestId, key,
			header.valueUpdateOffset, header.valueUpdateSize,
			false, false, false
		);
		this->dispatch( event );
		ret = false;
	}

	return ret;
}

bool SlaveWorker::handleDeleteRequest( MasterEvent event, char *buf, size_t size ) {
	struct KeyHeader header;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleDeleteRequest", "Invalid DELETE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleDeleteRequest",
		"[DELETE] Key: %.*s (key size = %u).",
		( int ) header.keySize, header.key, header.keySize
	);
	return this->handleDeleteRequest( event, header );
}

bool SlaveWorker::handleDeleteRequest( MasterEvent event, struct KeyHeader &header ) {
	bool ret;
	Key key;
	KeyValue keyValue;
	KeyMetadata keyMetadata;
	Metadata metadata;
	RemappedKeyValue remappedKeyValue;
	Chunk *chunk;
	if ( map->findValueByKey( header.key, header.keySize, &keyValue, 0, &keyMetadata, &metadata, &chunk ) ) {
		uint32_t timestamp;
		uint32_t deltaSize = 0;
		char *delta = 0;

		// Add the current request to the pending set
		if ( SlaveWorker::parityChunkCount ) {
			key.dup( header.keySize, header.key, ( void * ) event.socket );
			if ( ! SlaveWorker::pending->insertKey( PT_MASTER_DEL, event.instanceId, event.requestId, ( void * ) event.socket, key ) ) {
				__ERROR__( "SlaveWorker", "handleDeleteRequest", "Cannot insert into master DELETE pending map." );
			}
			delta = this->buffer.data;
		} else {
			key.set( header.keySize, header.key, ( void * ) event.socket );
			delta = 0;
		}

		// Update data chunk and map
		LOCK_T *keysLock, *cacheLock;
		std::unordered_map<Key, KeyMetadata> *keys;
		std::unordered_map<Metadata, Chunk *> *cache;
		KeyMetadata keyMetadata;

		SlaveWorker::map->getKeysMap( keys, keysLock );
		SlaveWorker::map->getCacheMap( cache, cacheLock );
		// Lock the data chunk buffer
		MixedChunkBuffer *chunkBuffer = SlaveWorker::chunkBuffer->at( metadata.listId );
		int chunkBufferIndex = chunkBuffer->lockChunk( chunk, true );
		// Lock the keys and cache map
		LOCK( keysLock );
		LOCK( cacheLock );
		// Delete the chunk and perform key-value compaction
		if ( chunkBufferIndex == -1 ) {
			// Only compute data delta if the chunk is not yet sealed
			if ( ! chunkBuffer->reInsert( this, chunk, keyMetadata.length, false, false ) ) {
				// The chunk is compacted before. Need to seal the chunk first
				// Seal from chunk->lastDelPos
				if ( chunk->lastDelPos > 0 && chunk->lastDelPos < chunk->getSize() ) {
					// Only issue seal chunk request when new key-value pairs are received
					this->issueSealChunkRequest( chunk, chunk->lastDelPos );
				}
			}
		}
		SlaveWorker::map->deleteKey( key, PROTO_OPCODE_DELETE, timestamp, keyMetadata, false, false );
		if ( SlaveWorker::parityChunkCount )
			deltaSize = chunk->deleteKeyValue( keys, keyMetadata, delta, this->buffer.size );
		else
			deltaSize = chunk->deleteKeyValue( keys, keyMetadata );
		// Release the locks
		UNLOCK( cacheLock );
		UNLOCK( keysLock );
		if ( chunkBufferIndex == -1 )
			chunkBuffer->unlock();

		if ( SlaveWorker::parityChunkCount ) {
			ret = this->sendModifyChunkRequest(
				event.instanceId, event.requestId, key.size, key.data,
				metadata, keyMetadata.offset, deltaSize, 0, delta,
				chunkBufferIndex == -1 /* isSealed */,
				false /* isUpdate */
			);
		} else {
			uint32_t timestamp = SlaveWorker::timestamp->nextVal();
			event.resDelete(
				event.socket,
				event.instanceId, event.requestId,
				timestamp,
				metadata.listId,
				metadata.stripeId,
				metadata.chunkId,
				key,
				false, // needsFree
				false  // isDegraded
			);
			this->dispatch( event );
			ret = true;
		}
		if ( chunkBufferIndex != -1 )
			chunkBuffer->updateAndUnlockChunk( chunkBufferIndex );
	} else if ( remappedBuffer->find( header.keySize, header.key, &remappedKeyValue ) ) {
		__INFO__( GREEN, "SlaveWorker", "handleDeleteRequest", "Handle remapped key: %.*s!", header.keySize, header.key );
		ret = true;
	} else {
		key.set( header.keySize, header.key, ( void * ) event.socket );
		event.resDelete(
			event.socket,
			event.instanceId, event.requestId,
			key,
			false, // needsFree
			false  // isDegraded
		);
		this->dispatch( event );
		ret = false;
	}

	return ret;
}

bool SlaveWorker::handleAckParityDeltaBackup( MasterEvent event, char *buf, size_t size ) {
	ParityDeltaAcknowledgementHeader header;
	if ( ! this->protocol.parseParityDeltaAcknowledgementHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleAckParityDeltaBackup", "Invalid ACK parity delta backup request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleAckParityDeltaBackup",
		"Ack. from master fd = %u from %u to %u for data slave id = %hu.",
		event.socket->getSocket(), header.fromTimestamp, header.toTimestamp, header.targetId
	);

	Timestamp from( header.fromTimestamp );
	Timestamp to( header.toTimestamp );

	event.socket->backup.removeParityUpdate( from, to, header.targetId );
	event.socket->backup.removeParityDelete( from, to, header.targetId );

	event.resAckParityDelta( event.socket, Slave::instanceId, event.requestId, header.fromTimestamp, header.toTimestamp, header.targetId );
	this->dispatch( event );

	return true;
}

bool SlaveWorker::handleRevertParityDelta( MasterEvent event, char *buf, size_t size ) {

	ParityDeltaAcknowledgementHeader header;
	if ( ! this->protocol.parseParityDeltaAcknowledgementHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleRevertParityDelta", "Invalid REVERT parity delta request." );
		return false;
	}
	__INFO__(
		BLUE, "SlaveWorker", "handleRevertParityDelta",
		"Revert request from master fd = %u from %u to %u for data slave id = %hu.",
		event.socket->getSocket(), header.fromTimestamp, header.toTimestamp, header.targetId
	);

	Timestamp from( header.fromTimestamp );
	Timestamp to( header.toTimestamp );
	uint32_t timestamp;
	bool success = true;

	std::vector<BackupDelta> update = event.socket->backup.removeParityUpdate( from, to, header.targetId, false );
	std::vector<BackupDelta> del = event.socket->backup.removeParityDelete( from, to, header.targetId, false );

	// Revert parity delta
	for ( auto& it : update ) {
		it.print();
		if ( it.isChunkDelta ) {
			SlaveWorker::chunkBuffer->at( it.metadata.listId )->update(
				it.metadata.stripeId, it.metadata.chunkId,
				it.delta.chunkOffset, it.delta.data.size, it.delta.data.data,
				this->chunks, this->dataChunk, this->parityChunk
			);
		} else {
			bool ret = SlaveWorker::chunkBuffer->at( it.metadata.listId )->updateKeyValue(
				it.key.data, it.key.size,
				it.delta.valueOffset, it.delta.data.size, it.delta.data.data
			);
			// apply to chunk if seal after backup
			if ( ! ret ) {
				SlaveWorker::chunkBuffer->at( it.metadata.listId )->update(
					it.metadata.stripeId, it.metadata.chunkId,
					it.delta.chunkOffset, it.delta.data.size, it.delta.data.data,
					this->chunks, this->dataChunk, this->parityChunk
				);
			}
		}
		it.free();
	}

	for ( auto& it : del ) {
		it.print();
		if ( it.isChunkDelta ) {
			SlaveWorker::chunkBuffer->at( it.metadata.listId )->update(
				it.metadata.stripeId, it.metadata.chunkId,
				it.delta.chunkOffset, it.delta.data.size, it.delta.data.data,
				this->chunks, this->dataChunk, this->parityChunk,
				true /* isDelete */
			);
		} else {
			// set key-value pair back
			// TODO what if the chunk is sealed after backup ...
			bool isSealed;
			Metadata sealed;
			SlaveWorker::chunkBuffer->at( it.metadata.listId )->set(
				this,
				it.key.data, it.key.size,
				it.delta.data.data, it.delta.data.size,
				PROTO_OPCODE_SET, timestamp /* generated by DataChunkBuffer */,
				it.metadata.stripeId, it.metadata.chunkId,
				&isSealed, &sealed,
				this->chunks, this->dataChunk, this->parityChunk
			);
		}
		it.free();
	}

	event.resRevertParityDelta( event.socket, Slave::instanceId, event.requestId, success, header.fromTimestamp, header.toTimestamp, header.targetId );
	this->dispatch( event );

	return success;
}
