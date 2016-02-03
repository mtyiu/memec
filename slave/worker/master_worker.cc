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
		case MASTER_EVENT_TYPE_REVERT_DELTA_SUCCESS:
			success = true;
			break;
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_GET_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_REVERT_DELTA_FAILURE:
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
			fprintf( stderr, "[%u, %u] %.*s (key size = %u)\n", event.instanceId, event.requestId, event.message.key.size, event.message.key.data, event.message.key.size );
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
			// assert( success );
			if ( ! success )
				fprintf( stderr, "Failed UPDATE: %.*s\n", event.message.keyValueUpdate.key.size,
				event.message.keyValueUpdate.key.data );
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
		{
			std::vector<uint32_t> *timestamps = event.message.revert.timestamps;
			buffer.data = this->protocol.ackParityDeltaBackup(
				buffer.size,
				Slave::instanceId, event.requestId,
				timestamps ? *timestamps : std::vector<uint32_t>(),
				event.message.revert.targetId
			);
			if ( timestamps )
				delete timestamps;
		}
			break;
		case MASTER_EVENT_TYPE_REVERT_DELTA_SUCCESS:
		case MASTER_EVENT_TYPE_REVERT_DELTA_FAILURE:
		{
			std::vector<uint32_t> *timestamps = event.message.revert.timestamps;
			std::vector<Key> *requests = event.message.revert.requests;
			buffer.data = this->protocol.resRevertDelta(
				buffer.size,
				Slave::instanceId, event.requestId,
				success,
				timestamps ? *timestamps : std::vector<uint32_t>(),
				requests ? *requests : std::vector<Key>(),
				event.message.revert.targetId
			);
			if ( timestamps )
				delete timestamps;
			if ( requests ) {
				for ( Key &k : *requests )
					k.free();
				delete requests;
			}
		}
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
					case PROTO_OPCODE_REVERT_DELTA:
						this->handleRevertDelta( event, buffer.data, buffer.size );
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
	bool exist = false;

	listId = SlaveWorker::stripeList->get( header.key, header.keySize, &dataSlaveSocket, 0, &chunkId );

	if (
		( map->findValueByKey( header.key, header.keySize, 0, 0 ) ) ||
		( SlaveWorker::chunkBuffer->at( listId )->findValueByKey( header.key, header.keySize, 0, 0, false ) )
	) {
		exist = true;
		// printf( "The key already exists: %.*s\n", header.keySize, header.key );
	} else {
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
	}

	if ( ! needResSet )
		return true;

	Key key;
	key.set( header.keySize, header.key );
	if ( exist ) {
		event.resSet(
			event.socket, event.instanceId, event.requestId,
			key, false
		);
	} else if ( ! dataSlaveSocket->self ) {
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

bool SlaveWorker::handleUpdateRequest(
	MasterEvent event, struct KeyValueUpdateHeader &header,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	bool reconstructParity,
	Chunk **chunks, bool endOfDegradedOp,
	bool checkGetChunk
) {
	bool ret;
	Key key;
	KeyValue keyValue;
	KeyValueUpdate keyValueUpdate;
	KeyMetadata keyMetadata;
	Metadata metadata;
	RemappedKeyValue remappedKeyValue;
	RemappingRecord remappingRecord;
	Chunk *chunk;

	if ( map->findValueByKey( header.key, header.keySize, &keyValue, &key, &keyMetadata, &metadata, &chunk ) ) {
		if ( keyMetadata.isParityRemapped ) {
			assert( ! original && ! reconstructed && ! reconstructedCount );

			// Find remapping record and store it to {original, reconstructed, reconstructedCount}
			if ( remappedBuffer->find( header.keySize, header.key, &remappingRecord ) ) {
				original = remappingRecord.original;
				reconstructed = remappingRecord.remapped;
				reconstructedCount = remappingRecord.remappedCount;
			} else {
				__ERROR__( "SlaveWorker", "handleUpdateRequest", "[%u, %u] Cannot find remapping record for key: %.*s.", event.instanceId, event.requestId, header.keySize, header.key );
			}

			reconstructParity = false;
			chunks = 0;
			endOfDegradedOp = false;
		}

		uint32_t offset = keyMetadata.offset + PROTO_KEY_VALUE_SIZE + header.keySize + header.valueUpdateOffset;

		if ( SlaveWorker::parityChunkCount ) {
			// Add the current request to the pending set
			keyValueUpdate.dup( header.keySize, header.key, ( void * ) event.socket );
			keyValueUpdate.offset = header.valueUpdateOffset;
			keyValueUpdate.length = header.valueUpdateSize;
			keyValueUpdate.isDegraded = reconstructedCount > 0;

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

		if ( checkGetChunk ) {
			SlaveWorker::getChunkBuffer->insert(
				metadata,
				chunkBufferIndex == -1 /* isSealed */ ? chunk : 0
			);
		}

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
				event.instanceId, event.requestId,
				keyValueUpdate.size, keyValueUpdate.data,
				metadata, offset,
				header.valueUpdateSize,    // deltaSize
				header.valueUpdateOffset,
				header.valueUpdate,        // delta
				chunkBufferIndex == -1,    // isSealed
				true,                      // isUpdate
				event.timestamp,
				event.socket,              // masterSocket
				original, reconstructed, reconstructedCount,
				reconstructParity,
				chunks, endOfDegradedOp,
				checkGetChunk
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
		// __INFO__( GREEN, "SlaveWorker", "handleUpdateRequest", "Handle remapped key: %.*s!", header.keySize, header.key );

		if ( SlaveWorker::parityChunkCount ) {
			// Add the current request to the pending set
			keyValueUpdate.dup( header.keySize, header.key, ( void * ) event.socket );
			keyValueUpdate.offset = header.valueUpdateOffset;
			keyValueUpdate.length = header.valueUpdateSize;
			keyValueUpdate.isDegraded = reconstructedCount > 0;

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

bool SlaveWorker::handleDeleteRequest(
	MasterEvent event, struct KeyHeader &header,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	bool reconstructParity,
	Chunk **chunks, bool endOfDegradedOp
) {
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
				chunkBufferIndex == -1, // isSealed
				false,                  // isUpdate
				event.timestamp,        // timestamp
				event.socket,           // masterSocket
				original, reconstructed, reconstructedCount,
				reconstructParity,
				chunks, endOfDegradedOp
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
		// __INFO__( GREEN, "SlaveWorker", "handleDeleteRequest", "Handle remapped key: %.*s!", header.keySize, header.key );
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
	DeltaAcknowledgementHeader header;
	std::vector<uint32_t> timestamps;

	if ( ! this->protocol.parseDeltaAcknowledgementHeader( header, &timestamps, 0, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleAckParityDeltaBackup", "Invalid ACK parity delta backup request." );
		return false;
	}

	if ( header.tsCount != 2 ) {
		__ERROR__( "SlaveWorker", "handleAckParityDeltaBackup", "Invalid ACK parity delta backup request (count < 2)." );
	} else {
		__DEBUG__(
			BLUE, "SlaveWorker", "handleAckParityDeltaBackup",
			"Ack. from master fd = %u from %u to %u for data slave id = %hu.",
			event.socket->getSocket(), timestamps.at( 0 ), timestamps.at( 1 ), header.targetId
		);

		Timestamp from( timestamps.at( 0 ) );
		Timestamp to( timestamps.at( 1 ) );

		event.socket->backup.removeParityUpdate( from, to, header.targetId );
		event.socket->backup.removeParityDelete( from, to, header.targetId );
	}

	event.resAckParityDelta( event.socket, Slave::instanceId, event.requestId, std::vector<uint32_t>() , header.targetId );
	this->dispatch( event );

	return true;
}

bool SlaveWorker::handleRevertDelta( MasterEvent event, char *buf, size_t size ) {
	DeltaAcknowledgementHeader header;
	std::vector<uint32_t> timestamps;
	std::set<uint32_t> timestampSet;
	std::vector<Key> requests;

	if ( ! this->protocol.parseDeltaAcknowledgementHeader( header, &timestamps, &requests, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleRevertDelta", "Invalid REVERT delta request." );
		return false;
	}

	if ( header.tsCount == 0 && header.keyCount == 0 && header.targetId == 0 ) {
		__ERROR__( "SlaveWorker", "handleRevertDelta", "Invalid REVERT delta request." );
		event.resRevertDelta( event.socket, Slave::instanceId, event.requestId, false /* success */, std::vector<uint32_t>(), std::vector<Key>(), header.targetId );
		this->dispatch( event );
		return false;
	}

	__INFO__(
		BLUE, "SlaveWorker", "handleRevertDelta",
		"Revert request from master fd = %u (UPDATE/DELETE) from %u to %u size = %u, and (SET) size = %u, for failed slave id = %hu.",
		event.socket->getSocket(),
		header.tsCount > 0 ? timestamps.at( 0 ) : 0,
		header.tsCount > 1 ? timestamps.at( header.tsCount - 1 ) : UINT32_MAX,
		header.tsCount, header.keyCount, header.targetId
	);

	uint32_t timestamp;
	bool success = true;
	std::vector<BackupDelta> update, del;

	// parity revert for UDPATE / DELETE //
	timestampSet.insert( timestamps.begin(), timestamps.end() );
	update = event.socket->backup.removeParityUpdate( timestampSet, header.targetId, false );
	del = event.socket->backup.removeParityDelete( timestampSet, header.targetId, false );

	// prepare to record timestamps of reverted deltas
	timestamps.clear();

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
			/* apply to chunk if seal after backup */
			if ( ! ret ) {
				SlaveWorker::chunkBuffer->at( it.metadata.listId )->update(
					it.metadata.stripeId, it.metadata.chunkId,
					it.delta.chunkOffset, it.delta.data.size, it.delta.data.data,
					this->chunks, this->dataChunk, this->parityChunk
				);
			}
		}
		timestamps.push_back( it.timestamp );
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
			/* set key-value pair back */
			/* TODO what if the chunk is sealed after backup ... */
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
		timestamps.push_back( it.timestamp );
		it.free();
	}

	// data response for UPDATE / DELETE //
#define CHECK_RESPONSE_FOR_FAILED_PARITY( _PT_TYPE_, _PT_MASTER_TYPE_, _OP_TYPE_, _MAP_VALUE_TYPE_ ) \
	LOCK( &SlaveWorker::pending->slavePeers._OP_TYPE_##Lock ); \
	{ \
	std::unordered_multimap<PendingIdentifier, _MAP_VALUE_TYPE_>::iterator it, saveIt; \
	std::unordered_multimap<PendingIdentifier, _MAP_VALUE_TYPE_> *map = &SlaveWorker::pending->slavePeers._OP_TYPE_; \
	for ( it = map->begin(), saveIt = it; it != map->end(); it = saveIt ) { \
		saveIt++; \
		if ( ( ( SlavePeerSocket * ) ( it->first.ptr ) )->instanceId != header.targetId ) \
			continue; \
		if ( SlaveWorker::pending->count( _PT_TYPE_, it->first.instanceId, it->first.requestId, false, false ) == 1 ) { \
			/* response immediately */ \
			MasterEvent masterEvent; \
			KeyValueUpdate keyValueUpdate; \
			Key key; \
			PendingIdentifier pid; \
			if ( strcmp( #_OP_TYPE_, "update" ) == 0 || strcmp( #_OP_TYPE_, "updateChunk" ) == 0 ) { \
				if ( ! SlaveWorker::pending->eraseKeyValueUpdate( _PT_MASTER_TYPE_, it->first.parentInstanceId, it->first.parentRequestId, 0, &pid, &keyValueUpdate ) ) { \
					__ERROR__( "SlaveWorker", "handleRevertDelta", "Cannot find a pending master UPDATE request that matches the response. This message will be discarded." ); \
					continue; \
				} \
				masterEvent.resUpdate( \
					( MasterSocket * ) pid.ptr, pid.instanceId, pid.requestId, \
					keyValueUpdate, \
					keyValueUpdate.offset, \
					keyValueUpdate.length, \
					success, true, false \
				); \
				this->dispatch( masterEvent ); \
				__INFO__( YELLOW, "SlaveWorker", "handleRevertDelta", "Skip waiting for key %.*s for failed slave id=%u", keyValueUpdate.size, keyValueUpdate.data, header.targetId ); \
			} else if ( strcmp( #_OP_TYPE_, "delete" ) == 0 || strcmp( #_OP_TYPE_, "deleteChunk" ) == 0 ) { \
				if ( ! SlaveWorker::pending->eraseKey( _PT_MASTER_TYPE_, it->first.parentInstanceId, it->first.parentRequestId, 0, &pid, &key ) ) { \
					__ERROR__( "SlaveWorker", "handleRevertDelta", "Cannot find a pending master DELETE request that matches the response. This message will be discarded." ); \
					continue; \
				} \
				masterEvent.resDelete( \
					( MasterSocket * ) pid.ptr, \
					pid.instanceId, pid.requestId, \
					key, \
					true, /* needsFree */ \
					false /* isDegraded */ \
				); \
				this->dispatch( masterEvent ); \
				__INFO__( YELLOW, "SlaveWorker", "handleRevertDelta", "Skip waiting for key %.*s for failed slave id=%u", key.size, key.data, header.targetId ); \
			} \
		} \
		map->erase( it ); \
	} \
	UNLOCK( &SlaveWorker::pending->slavePeers._OP_TYPE_##Lock ); \
	}

	CHECK_RESPONSE_FOR_FAILED_PARITY( PT_SLAVE_PEER_UPDATE, PT_MASTER_UPDATE, update, KeyValueUpdate );
	CHECK_RESPONSE_FOR_FAILED_PARITY( PT_SLAVE_PEER_UPDATE_CHUNK, PT_MASTER_UPDATE, updateChunk, ChunkUpdate );
	CHECK_RESPONSE_FOR_FAILED_PARITY( PT_SLAVE_PEER_DEL, PT_MASTER_DEL, del, Key );
	CHECK_RESPONSE_FOR_FAILED_PARITY( PT_SLAVE_PEER_DEL_CHUNK, PT_MASTER_DEL, deleteChunk, ChunkUpdate );

	// parity revert for SET //
	uint32_t chunkId, listId;

	for( Key &k : requests ) {
		listId = SlaveWorker::stripeList->get( k.data, k.size, this->dataSlaveSockets, 0, &chunkId );
		// check if this slave handles chunks for the stripe list
		if ( SlaveWorker::chunkBuffer->size() < listId + 1 || SlaveWorker::chunkBuffer->at( listId ) == 0 )
			continue;

		// revert normal SET via DELETE
		bool ret = SlaveWorker::chunkBuffer->at( listId )->deleteKey( k.data, k.size );
		if ( ret )
			__INFO__( YELLOW, "SlaveWorker", "handleRevertDelta", "reverted delta for key %.*s for failed slave id=%u", k.size, k.data, header.targetId );

		// TODO revert remapped SET (?)

		k.free();
	}
	// UNLOCK PENDING

	event.resRevertDelta( event.socket, Slave::instanceId, event.requestId, success, timestamps, std::vector<Key>(), header.targetId );
	this->dispatch( event );

	return success;
}
