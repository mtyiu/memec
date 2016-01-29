#include "worker.hh"
#include "../main/slave.hh"

bool SlaveWorker::handleSlavePeerRegisterRequest( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, char *buf, size_t size ) {
	struct AddressHeader header;
	if ( ! this->protocol.parseAddressHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleSlavePeerRegisterRequest", "Invalid address header." );
		return false;
	}

	// Find the slave peer socket in the array map
	int index = -1;
	for ( int i = 0, len = slavePeers->values.size(); i < len; i++ ) {
		if ( slavePeers->values[ i ]->equal( header.addr, header.port ) ) {
			index = i;
			break;
		}
	}

	socket->instanceId = instanceId;
	Slave *slave = Slave::getInstance();
	LOCK( &slave->sockets.slavesIdToSocketLock );
	slave->sockets.slavesIdToSocketMap[ instanceId ] = socket;
	UNLOCK( &slave->sockets.slavesIdToSocketLock );

	if ( index == -1 ) {
		__ERROR__( "SlaveWorker", "handleSlavePeerRegisterRequest", "The slave is not in the list. Ignoring this slave..." );
		return false;
	}

	SlavePeerEvent event;
	event.resRegister( slavePeers->values[ index ], Slave::getInstance()->instanceId, requestId, true );
	SlaveWorker::eventQueue->insert( event );

	return true;
}

////////////////////////////////////////////////////////////////////////////////

bool SlaveWorker::handleForwardKeyRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct ForwardKeyHeader header;
	if ( ! this->protocol.parseForwardKeyReqHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleForwardKeyRequest", "Invalid DEGRADED_SET request (size = %lu).", size );
		return false;
	}
	if ( header.opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		__DEBUG__(
			BLUE, "SlaveWorker", "handleForwardKeyRequest",
			"[DEGRADED_SET] Degraded opcode: 0x%x; list ID: %u, chunk ID: %u; key: %.*s (size = %u); value size: %u; value update size: %u, offset: %u.",
			header.opcode, header.listId, header.chunkId,
			header.keySize, header.key, header.keySize,
			header.valueSize, header.valueUpdateSize, header.valueUpdateOffset
		);
	} else {
		__DEBUG__(
			BLUE, "SlaveWorker", "handleForwardKeyRequest",
			"[DEGRADED_SET] Degraded opcode: 0x%x; list ID: %u, chunk ID: %u; key: %.*s (size = %u); value size: %u.",
			header.opcode, header.listId, header.chunkId,
			header.keySize, header.key, header.keySize,
			header.valueSize
		);
	}

	Key key;
	KeyValue keyValue;
	Metadata metadata;
	DegradedMap *dmap = &SlaveWorker::degradedChunkBuffer->map;
	bool isInserted;
	uint32_t timestamp;

	metadata.set( header.listId, header.stripeId, header.chunkId );
	key.set( header.keySize, header.key );

	switch( header.opcode ) {
		case PROTO_OPCODE_DEGRADED_GET:
			keyValue.dup( header.key, header.keySize, header.value, header.valueSize );
			isInserted = dmap->insertValue( keyValue, metadata );
			break;
		case PROTO_OPCODE_DEGRADED_UPDATE:
			keyValue.dup( header.key, header.keySize, header.value, header.valueSize );

			// Modify the key-value pair before inserting into DegradedMap
			isInserted = dmap->insertValue( keyValue, metadata );
			if ( isInserted ) {
				uint32_t dataUpdateOffset = KeyValue::getChunkUpdateOffset( 0, header.valueUpdateSize, header.valueUpdateOffset );
				// Compute data delta
				Coding::bitwiseXOR(
					header.valueUpdate,
					keyValue.data + dataUpdateOffset, // original data
					header.valueUpdate,               // new data
					header.valueUpdateSize
				);
				// Perform actual data update
				Coding::bitwiseXOR(
					keyValue.data + dataUpdateOffset,
					keyValue.data + dataUpdateOffset, // original data
					header.valueUpdate,               // new data
					header.valueUpdateSize
				);
			}
			break;
		case PROTO_OPCODE_DEGRADED_DELETE:
			// TODO: Store the original key-value pair into backup
			// Remove the key-value pair from DegradedMap
			dmap->deleteValue( key, metadata, PROTO_OPCODE_DELETE, timestamp );
			break;
		default:
			__ERROR__( "SlaveWorker", "handleForwardKeyRequest", "Undefined degraded opcode." );
			return false;
	}

	event.resForwardKey(
		event.socket,
		true, // success
		header.opcode,
		event.instanceId, event.requestId,
		header.listId, header.stripeId, header.chunkId,
		header.keySize, header.valueSize,
		header.key,
		header.valueUpdateOffset, header.valueUpdateSize
	);
	this->dispatch( event );

	return true;
}

////////////////////////////////////////////////////////////////////////////////

bool SlaveWorker::handleSetRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct KeyValueHeader header;
	if ( ! this->protocol.parseKeyValueHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleSetRequest (SlavePeer)", "Invalid SET request (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleSetRequest (SlavePeer) ",
		"[SET] Key: %.*s (key size = %u); Value: (value size = %u)",
		( int ) header.keySize, header.key, header.keySize, header.valueSize
	);
	// same flow as set from masters
	MasterEvent masterEvent;
	bool success = this->handleSetRequest( masterEvent, buf, size, false );

	Key key;
	key.set( header.keySize, header.key );
	event.resSet( event.socket, event.instanceId, event.requestId, key, success );
	this->dispatch( event );

	return success;
}

bool SlaveWorker::handleGetRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct ListStripeKeyHeader header;
	bool ret;
	if ( ! this->protocol.parseListStripeKeyHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleGetRequest", "Invalid UNSEALED_GET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleGetRequest",
		"[UNSEALED_GET] List ID: %u, chunk ID: %u; key: %.*s.",
		header.listId, header.chunkId, header.keySize, header.key
	);

	Key key;
	KeyValue keyValue;

	ret = SlaveWorker::chunkBuffer->at( header.listId )->findValueByKey(
		header.key, header.keySize, &keyValue, &key
	);

	if ( ret )
		event.resGet( event.socket, event.instanceId, event.requestId, keyValue );
	else
		event.resGet( event.socket, event.instanceId, event.requestId, key );
	this->dispatch( event );
	return ret;
}

bool SlaveWorker::handleUpdateRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct ChunkKeyValueUpdateHeader header;
	if ( ! this->protocol.parseChunkKeyValueUpdateHeader( header, true, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleUpdateRequest", "Invalid UPDATE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleUpdateRequest",
		"[UPDATE] Key: %.*s (key size = %u); Value: (update size = %u, offset = %u); list ID = %u, stripe ID = %u, chunk Id = %u, chunk update offset = %u.",
		( int ) header.keySize, header.key, header.keySize,
		header.valueUpdateSize, header.valueUpdateOffset,
		header.listId, header.stripeId, header.chunkId,
		header.chunkUpdateOffset
	);


	Key key;
	bool isChunkDelta = false;
	bool ret = SlaveWorker::chunkBuffer->at( header.listId )->updateKeyValue(
		header.key, header.keySize,
		header.valueUpdateOffset, header.valueUpdateSize, header.valueUpdate
	);
	if ( ! ret ) {
		isChunkDelta = true;
		// Use the chunkUpdateOffset
		SlaveWorker::chunkBuffer->at( header.listId )->update(
			header.stripeId, header.chunkId,
			header.chunkUpdateOffset, header.valueUpdateSize, header.valueUpdate,
			this->chunks, this->dataChunk, this->parityChunk
		);
		ret = true;
	}

	key.set( header.keySize, header.key );

	// backup parity delta ( data delta from data slave )
	Timestamp timestamp( event.timestamp );
	Value value;
	value.set( header.valueUpdateSize, header.valueUpdate );
	Metadata metadata;
	metadata.set( header.listId, header.stripeId, header.chunkId );
	Slave *slave = Slave::getInstance();
	LOCK( &slave->sockets.mastersIdToSocketLock );
	try {
		MasterSocket *masterSocket = slave->sockets.mastersIdToSocketMap.at( event.instanceId );
		if ( masterSocket )
			masterSocket->backup.insertParityUpdate( timestamp, key, value, metadata, isChunkDelta, header.valueUpdateOffset, header.chunkUpdateOffset, event.socket->instanceId, event.requestId );
	} catch ( std::out_of_range &e ) {
		__ERROR__( "SlaveWorker", "handleUpdateRequest", "Failed to backup delta at parity slave for instance ID = %hu request ID = %u (Socket mapping not found).", event.instanceId, event.requestId );
	}
	UNLOCK( &slave->sockets.mastersIdToSocketLock );

	event.resUpdate(
		event.socket, event.instanceId, event.requestId,
		header.listId, header.stripeId, header.chunkId, key,
		header.valueUpdateOffset,
		header.valueUpdateSize,
		header.chunkUpdateOffset,
		ret
	);
	this->dispatch( event );

	return ret;
}

bool SlaveWorker::handleDeleteRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct ChunkKeyHeader header;
	if ( ! this->protocol.parseChunkKeyHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleDeleteRequest", "Invalid DELETE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleDeleteRequest",
		"[DELETE] Key: %.*s (key size = %u); list ID = %u, stripe ID = %u, chunk Id = %u.",
		( int ) header.keySize, header.key, header.keySize,
		header.listId, header.stripeId, header.chunkId
	);

	Key key;
	Value value;
	KeyValue keyValue;

	// read for backup before delete
	bool ret = SlaveWorker::chunkBuffer->at( header.listId )->findValueByKey(
		header.key, header.keySize, &keyValue, &key
	);
	if ( ret )
		keyValue.deserialize( key.data, key.size, value.data, value.size );

	// backup parity delta ( data delta from data slave )
	Timestamp timestamp( event.timestamp );
	Metadata metadata;
	metadata.set( header.listId, header.stripeId, header.chunkId );
	Slave *slave = Slave::getInstance();
	LOCK( &slave->sockets.mastersIdToSocketLock );
	try {
		MasterSocket *masterSocket = slave->sockets.mastersIdToSocketMap.at( event.instanceId );
		if ( masterSocket )
			masterSocket->backup.insertParityDelete( timestamp, key, value, metadata, false, 0, 0, event.socket->instanceId, event.requestId );
	} catch ( std::out_of_range &e ) {
		__ERROR__( "SlaveWorker", "handleDeleteRequest", "Failed to backup delta at parity slave for instance ID = %hu request ID = %u (Socket mapping not found).", event.instanceId, event.requestId );
	}
	UNLOCK( &slave->sockets.mastersIdToSocketLock );

	ret = SlaveWorker::chunkBuffer->at( header.listId )->deleteKey( header.key, header.keySize );

	event.resDelete(
		event.socket, event.instanceId, event.requestId,
		header.listId, header.stripeId, header.chunkId,
		key, ret
	);
	this->dispatch( event );

	return ret;
}

////////////////////////////////////////////////////////////////////////////////

bool SlaveWorker::handleGetChunkRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct ChunkHeader header;
	if ( ! this->protocol.parseChunkHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleGetChunkRequest", "Invalid GET_CHUNK request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleGetChunkRequest",
		"[GET_CHUNK] List ID: %u; stripe ID: %u; chunk ID: %u.",
		header.listId, header.stripeId, header.chunkId
	);
	return this->handleGetChunkRequest( event, header );
}

bool SlaveWorker::handleGetChunkRequest( SlavePeerEvent event, struct ChunkHeader &header ) {
	bool ret;

	Metadata metadata;
	Chunk *chunk = map->findChunkById( header.listId, header.stripeId, header.chunkId, &metadata );
	ret = chunk;

	// Check whether the chunk is sealed or not
	MixedChunkBuffer *chunkBuffer = SlaveWorker::chunkBuffer->at( header.listId );
	int chunkBufferIndex = chunkBuffer->lockChunk( chunk, true );
	bool isSealed = ( chunkBufferIndex == -1 );

	// if ( ! chunk ) {
	// 	__ERROR__( "SlaveWorker", "handleGetChunkRequest", "The chunk (%u, %u, %u) does not exist.", header.listId, header.stripeId, header.chunkId );
	// }

	if ( chunk && chunk->status == CHUNK_STATUS_NEEDS_LOAD_FROM_DISK ) {
		// printf( "Loading chunk: (%u, %u, %u) from disk.\n", header.listId, header.stripeId, header.chunkId );
		this->storage->read(
			chunk,
			chunk->metadata.listId,
			chunk->metadata.stripeId,
			chunk->metadata.chunkId,
			chunk->isParity
		);
	}

	event.resGetChunk( event.socket, event.instanceId, event.requestId, metadata, ret, chunkBufferIndex, isSealed ? chunk : 0 );

	this->dispatch( event );
	// Unlock in the dispatcher
	// chunkBuffer->unlock( chunkBufferIndex );

	return ret;
}

bool SlaveWorker::handleSetChunkRequest( SlavePeerEvent event, bool isSealed, char *buf, size_t size ) {
	union {
		struct ChunkDataHeader chunkData;
		struct ChunkKeyValueHeader chunkKeyValue;
	} header;
	Metadata metadata;
	char *ptr = 0;

	if ( isSealed ) {
		if ( ! this->protocol.parseChunkDataHeader( header.chunkData, buf, size ) ) {
			__ERROR__( "SlaveWorker", "handleSetChunkRequest", "Invalid SET_CHUNK request." );
			return false;
		}
		metadata.set( header.chunkData.listId, header.chunkData.stripeId, header.chunkData.chunkId );
		__DEBUG__(
			BLUE, "SlaveWorker", "handleSetChunkRequest",
			"[SET_CHUNK] List ID: %u; stripe ID: %u; chunk ID: %u; chunk size = %u.",
			header.chunkData.listId, header.chunkData.stripeId, header.chunkData.chunkId, header.chunkData.size
		);
	} else {
		if ( ! this->protocol.parseChunkKeyValueHeader( header.chunkKeyValue, ptr, buf, size ) ) {
			__ERROR__( "SlaveWorker", "handleSetChunkRequest", "Invalid SET_CHUNK request." );
			return false;
		}
		metadata.set( header.chunkKeyValue.listId, header.chunkKeyValue.stripeId, header.chunkKeyValue.chunkId );
		__DEBUG__(
			BLUE, "SlaveWorker", "handleSetChunkRequest",
			"[SET_CHUNK] List ID: %u; stripe ID: %u; chunk ID: %u; deleted = %u; count = %u.",
			header.chunkKeyValue.listId, header.chunkKeyValue.stripeId, header.chunkKeyValue.chunkId,
			header.chunkKeyValue.deleted, header.chunkKeyValue.count
		);
	}

	Key key;
	KeyValue keyValue;
	KeyMetadata keyMetadata;
	bool ret;
	uint32_t offset, chunkSize, valueSize, objSize;
	char *valueStr;
	Chunk *chunk;
	LOCK_T *keysLock, *cacheLock;
	std::unordered_map<Key, KeyMetadata> *keys;
	std::unordered_map<Metadata, Chunk *> *cache;
	bool notifyCoordinator = false;
	CoordinatorEvent coordinatorEvent;

	SlaveWorker::map->getKeysMap( keys, keysLock );
	SlaveWorker::map->getCacheMap( cache, cacheLock );

	LOCK( keysLock );
	LOCK( cacheLock );

	chunk = SlaveWorker::map->findChunkById(
		metadata.listId, metadata.stripeId, metadata.chunkId, 0,
		false, // needsLock
		false // needsUnlock
	);
	// Lock the data chunk buffer
	MixedChunkBuffer *chunkBuffer = SlaveWorker::chunkBuffer->at( metadata.listId );
	int chunkBufferIndex = chunkBuffer->lockChunk( chunk, false );

	ret = chunk;
	if ( ! chunk ) {
		// Allocate memory for this chunk
		chunk = SlaveWorker::chunkPool->malloc();
		chunk->clear();
		chunk->metadata.listId = metadata.listId;
		chunk->metadata.stripeId = metadata.stripeId;
		chunk->metadata.chunkId = metadata.chunkId;
		chunk->isParity = metadata.chunkId >= SlaveWorker::dataChunkCount;
		SlaveWorker::map->setChunk(
			metadata.listId, metadata.stripeId, metadata.chunkId,
			chunk, chunk->isParity,
			false, false
		);

		// Remove the chunk from pending chunk set
		uint16_t instanceId;
		uint32_t requestId, addr, remainingChunks, remainingKeys, totalChunks, totalKeys;
		uint16_t port;
		CoordinatorSocket *coordinatorSocket;
		if ( SlaveWorker::pending->eraseRecovery( metadata.listId, metadata.stripeId, metadata.chunkId, instanceId, requestId, coordinatorSocket, addr, port, remainingChunks, remainingKeys, totalChunks, totalKeys ) ) {
			// printf( "Received (%u, %u, %u). Number of remaining pending chunks = %u / %u.\n", metadata.listId, metadata.stripeId, metadata.chunkId, remaining, total );

			if ( remainingChunks == 0 ) {
				notifyCoordinator = true;
				coordinatorEvent.resPromoteBackupSlave( coordinatorSocket, instanceId, requestId, addr, port, totalChunks, 0 );
			}
		} else {
			// __ERROR__( "SlaveWorker", "handleSetChunkRequest", "Cannot find the chunk (%u, %u, %u) from pending chunk set.", metadata.listId, metadata.stripeId, metadata.chunkId );
		}
	}

	if ( metadata.chunkId < SlaveWorker::dataChunkCount ) {
		if ( isSealed ) {
			uint32_t timestamp, originalChunkSize;
			// Delete all keys in the chunk from the map
			offset = 0;
			originalChunkSize = chunkSize = chunk->getSize();
			while ( offset < chunkSize ) {
				keyValue = chunk->getKeyValue( offset );
				keyValue.deserialize( key.data, key.size, valueStr, valueSize );

				key.set( key.size, key.data );
				SlaveWorker::map->deleteKey(
					key, 0, timestamp, keyMetadata,
					false, // needsLock
					false, // needsUnlock
					false  // needsUpdateOpMetadata
				);

				objSize = KEY_VALUE_METADATA_SIZE + key.size + valueSize;
				offset += objSize;
			}

			// Replace chunk contents
			chunk->loadFromSetChunkRequest(
				header.chunkData.data,
				header.chunkData.size,
				header.chunkData.offset,
				header.chunkData.chunkId >= SlaveWorker::dataChunkCount
			);

			// Add all keys in the new chunk to the map
			offset = 0;
			chunkSize = header.chunkData.size;
			while( offset < chunkSize ) {
				uint32_t timestamp;

				keyValue = chunk->getKeyValue( offset );
				keyValue.deserialize( key.data, key.size, valueStr, valueSize );
				objSize = KEY_VALUE_METADATA_SIZE + key.size + valueSize;

				key.set( key.size, key.data );

				keyMetadata.set( metadata.listId, metadata.stripeId, metadata.chunkId );
				keyMetadata.offset = offset;
				keyMetadata.length = objSize;
				keyMetadata.ptr = ( char * ) chunk;

				SlaveWorker::map->insertKey(
					key, 0, timestamp, keyMetadata,
					false, // needsLock
					false, // needsUnlock
					false  // needsUpdateOpMetadata
				);

				offset += objSize;
			}
			chunk->updateData();

			// Re-insert into data chunk buffer
			assert( chunkBufferIndex == -1 );
			if ( ! chunkBuffer->reInsert( this, chunk, originalChunkSize - chunkSize, false, false ) ) {
				// The chunk is compacted before. Need to seal the chunk first
				// Seal from chunk->lastDelPos
				if ( chunk->lastDelPos > 0 && chunk->lastDelPos < chunk->getSize() ) {
					// printf( "chunk->lastDelPos = %u; chunk->getSize() = %u\n", chunk->lastDelPos, chunk->getSize() );
					// Only issue seal chunk request when new key-value pairs are received
					this->issueSealChunkRequest( chunk, chunk->lastDelPos );
				}
			}
		} else {
			struct KeyHeader keyHeader;
			struct KeyValueHeader keyValueHeader;
			uint32_t offset = ptr - buf;
			uint32_t timestamp;

			// Handle removed keys in DEGRADED_DELETE
			for ( uint32_t i = 0; i < header.chunkKeyValue.deleted; i++ ) {
				if ( ! this->protocol.parseKeyHeader( keyHeader, buf, size, offset ) ) {
					__ERROR__( "SlaveWorker", "handleSetChunkRequest", "Invalid key in deleted key list." );
					break;
				}
				key.set( keyHeader.keySize, keyHeader.key );

				// Update key map and chunk
				if ( SlaveWorker::map->deleteKey( key, PROTO_OPCODE_DELETE, timestamp, keyMetadata, false, false, false ) ) {
					chunk->deleteKeyValue( keys, keyMetadata );
				} else {
					__ERROR__( "SlaveWorker", "handleSetChunkRequest", "The deleted key does not exist." );
				}

				offset += PROTO_KEY_SIZE + keyHeader.keySize;
			}

			// Update key-value pairs
			for ( uint32_t i = 0; i < header.chunkKeyValue.count; i++ ) {
				if ( ! this->protocol.parseKeyValueHeader( keyValueHeader, buf, size, offset ) ) {
					__ERROR__( "SlaveWorker", "handleSetChunkRequest", "Invalid key-value pair in updated value list." );
					break;
				}

				// Update the key-value pair
				if ( map->findValueByKey( keyValueHeader.key, keyValueHeader.keySize, &keyValue, 0, 0, 0, 0, false, false ) ) {
					keyValue.deserialize( key.data, key.size, valueStr, valueSize );
					assert( valueSize == keyValueHeader.valueSize );
					memcpy( valueStr, keyValueHeader.value, valueSize );
				} else {
					__ERROR__( "SlaveWorker", "handleSetChunkRequest", "The updated key-value pair does not exist." );
				}

				offset += PROTO_KEY_VALUE_SIZE + keyValueHeader.keySize + keyValueHeader.valueSize;
			}
		}
	} else {
		// Replace chunk contents for parity chunks
		chunk->loadFromSetChunkRequest(
			header.chunkData.data,
			header.chunkData.size,
			header.chunkData.offset,
			header.chunkData.chunkId >= SlaveWorker::dataChunkCount
		);
	}

	UNLOCK( cacheLock );
	UNLOCK( keysLock );
	if ( chunkBufferIndex != -1 )
		chunkBuffer->updateAndUnlockChunk( chunkBufferIndex );

	if ( notifyCoordinator ) {
		SlaveWorker::eventQueue->insert( coordinatorEvent );
	}

	if ( isSealed || header.chunkKeyValue.isCompleted ) {
		event.resSetChunk( event.socket, event.instanceId, event.requestId, metadata, ret );
		this->dispatch( event );
	}

	return ret;
}


bool SlaveWorker::handleUpdateChunkRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct ChunkUpdateHeader header;
	bool ret;
	if ( ! this->protocol.parseChunkUpdateHeader( header, true, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleUpdateChunkRequest", "Invalid UPDATE_CHUNK request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleUpdateChunkRequest",
		"[UPDATE_CHUNK] List ID: %u; stripe ID: %u; chunk ID: %u; offset: %u; length: %u; updating chunk ID: %u",
		header.listId, header.stripeId, header.chunkId,
		header.offset, header.length,
		header.updatingChunkId
	);

	Metadata metadata;
	metadata.set( header.listId, header.stripeId, header.chunkId );

	uint32_t myChunkId = SlaveWorker::chunkBuffer->at( header.listId )->getChunkId();

	if ( header.updatingChunkId == myChunkId ) {
		// Normal UPDATE_CHUNK request //
		ret = SlaveWorker::chunkBuffer->at( header.listId )->update(
			header.stripeId, header.chunkId,
			header.offset, header.length, header.delta,
			this->chunks, this->dataChunk, this->parityChunk
		);

		// backup parity chunk delta ( data chunk delta from data slave )
		Timestamp timestamp( event.timestamp );
		Key key;
		key.set( 0, 0 );
		Value value;
		value.set( header.length, header.delta );
		Slave *slave = Slave::getInstance();
		LOCK( &slave->sockets.mastersIdToSocketLock );
		try {
			MasterSocket *masterSocket = slave->sockets.mastersIdToSocketMap.at( event.instanceId );
			if ( masterSocket )
			masterSocket->backup.insertParityUpdate( timestamp, key, value, metadata, true, 0, header.offset, event.socket->instanceId, event.requestId );
		} catch ( std::out_of_range &e ) {
			__ERROR__( "SlaveWorker", "handleUpdateChunkRequest", "Failed to backup delta at parity slave for instance ID = %hu request ID = %u (Socket mapping not found).", event.instanceId, event.requestId );
		}
		UNLOCK( &slave->sockets.mastersIdToSocketLock );
	} else {
		// Update to reconstructed chunk //
		ret = SlaveWorker::degradedChunkBuffer->update(
			header.listId, header.stripeId, header.chunkId,
			header.updatingChunkId, // For verifying the reconstructed chunk exists
			header.offset, header.length, header.delta,
			this->chunks, this->dataChunk, this->parityChunk
		);

		if ( ! ret ) {
			__ERROR__( "SlaveWorker", "handleUpdateChunkRequest", "Chunk not found: (%u, %u, %u).", header.listId, header.stripeId, header.updatingChunkId );
		}
	}

	event.resUpdateChunk(
		event.socket, event.instanceId, event.requestId, metadata,
		header.offset, header.length,
		header.updatingChunkId, ret
	);
	this->dispatch( event );

	return ret;
}

bool SlaveWorker::handleDeleteChunkRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct ChunkUpdateHeader header;
	bool ret;
	if ( ! this->protocol.parseChunkUpdateHeader( header, true, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleDeleteChunkRequest", "Invalid DELETE_CHUNK request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleDeleteChunkRequest",
		"[DELETE_CHUNK] List ID: %u, stripe ID: %u, chunk ID: %u; offset: %u; length: %u; updating chunk ID: %u.",
		header.listId, header.stripeId, header.chunkId,
		header.offset, header.length, header.updatingChunkId
	);

	Metadata metadata;
	metadata.set( header.listId, header.stripeId, header.chunkId );

	uint32_t myChunkId = SlaveWorker::chunkBuffer->at( header.listId )->getChunkId();

	if ( header.updatingChunkId == myChunkId ) {
		// Normal DELETE_CHUNK request //
		ret = SlaveWorker::chunkBuffer->at( header.listId )->update(
			header.stripeId, header.chunkId,
			header.offset, header.length, header.delta,
			this->chunks, this->dataChunk, this->parityChunk,
			true // isDelete
		);

		// backup parity chunk delta ( data chunk delta from data slave )
		Timestamp timestamp( event.timestamp );
		Key key;
		key.set( 0, 0 );
		Value value;
		value.set( header.length, header.delta );
		Slave *slave = Slave::getInstance();
		LOCK( &slave->sockets.mastersIdToSocketLock );
		try{
			MasterSocket *masterSocket = slave->sockets.mastersIdToSocketMap.at( event.instanceId );
			if ( masterSocket )
				masterSocket->backup.insertParityDelete( timestamp, key, value, metadata, true, 0, header.offset, event.socket->instanceId, event.requestId );
		} catch ( std::out_of_range &e ) {
			__ERROR__( "SlaveWorker", "handleDeleteChunkRequest", "Failed to backup delta at parity slave for instance ID = %hu request ID = %u (Socket mapping not found).", event.instanceId, event.requestId );
		}
		UNLOCK( &slave->sockets.mastersIdToSocketLock );
	} else {
		// Update to reconstructed chunk //
		ret = SlaveWorker::degradedChunkBuffer->update(
			header.listId, header.stripeId, header.chunkId,
			header.updatingChunkId, // For verifying the reconstructed chunk exists
			header.offset, header.length, header.delta,
			this->chunks, this->dataChunk, this->parityChunk
		);

		if ( ! ret ) {
			__ERROR__( "SlaveWorker", "handleUpdateChunkRequest", "Chunk not found: (%u, %u, %u).", header.listId, header.stripeId, header.updatingChunkId );
		}
	}

	event.resDeleteChunk(
		event.socket, event.instanceId, event.requestId, metadata,
		header.offset, header.length,
		header.updatingChunkId, ret
	);

	this->dispatch( event );

	return ret;
}

////////////////////////////////////////////////////////////////////////////////

bool SlaveWorker::handleSealChunkRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct ChunkSealHeader header;
	bool ret;
	if ( ! this->protocol.parseChunkSealHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleSealChunkRequest", "Invalid SEAL_CHUNK request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleSealChunkRequest",
		"[SEAL_CHUNK] List ID: %u, stripe ID: %u, chunk ID: %u; count = %u.",
		header.listId, header.stripeId, header.chunkId, header.count
	);

	ret = SlaveWorker::chunkBuffer->at( header.listId )->seal(
		header.stripeId, header.chunkId, header.count,
		buf + PROTO_CHUNK_SEAL_SIZE,
		size - PROTO_CHUNK_SEAL_SIZE,
		this->chunks, this->dataChunk, this->parityChunk
	);

	if ( ! ret ) {
		event.socket->printAddress();
		printf( " " );
		fflush( stdout );
		__ERROR__( "SlaveWorker", "handleSealChunkRequest", "[%u, %u] Cannot update parity chunk.", event.instanceId, event.requestId );
	}

	return true;
}

bool SlaveWorker::issueSealChunkRequest( Chunk *chunk, uint32_t startPos ) {
	if ( SlaveWorker::disableSeal ) {
		return false;
	}

	// The chunk is locked when this function is called
	// Only issue seal chunk request when new key-value pairs are received
	if ( SlaveWorker::parityChunkCount && startPos < chunk->getSize() ) {
		size_t size;
		uint16_t instanceId = Slave::instanceId;
		uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
		Packet *packet = SlaveWorker::packetPool->malloc();
		packet->setReferenceCount( SlaveWorker::parityChunkCount );

		// printf(
		// 	"[%u, %u] issueSealChunkRequest(): (%u, %u, %u)\n",
		// 	instanceId, requestId,
		// 	chunk->metadata.listId,
		// 	chunk->metadata.stripeId,
		// 	chunk->metadata.chunkId
		// );

		// Find parity slaves
		this->getSlaves( chunk->metadata.listId );

		// Prepare seal chunk request
		this->protocol.reqSealChunk( size, instanceId, requestId, chunk, startPos, packet->data );
		packet->size = ( uint32_t ) size;

		if ( size == PROTO_HEADER_SIZE + PROTO_CHUNK_SEAL_SIZE ) {
			packet->setReferenceCount( 1 );
			SlaveWorker::packetPool->free( packet );
			return true;
		}

		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
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
	}
	return true;
}

bool SlaveWorker::handleBatchKeyValueRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct BatchKeyValueHeader header;
	if ( ! this->protocol.parseBatchKeyValueHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleBatchKeyValueRequest", "Invalid BATCH_KEY_VALUE request." );
		return false;
	}

	uint32_t listId = 0, chunkId = 0;
	uint16_t instanceId, port;
	uint32_t requestId, addr, remainingChunks, remainingKeys, totalChunks, totalKeys;
	CoordinatorSocket *coordinatorSocket;

	for ( uint32_t i = 0, offset = 0; i < header.count; i++ ) {
		uint8_t keySize;
		uint32_t valueSize;
		uint32_t timestamp, stripeId;
		char *keyStr, *valueStr;

		this->protocol.nextKeyValueInBatchKeyValueHeader( header, keySize, valueSize, keyStr, valueStr, offset );

		if ( i == 0 )
			this->getSlaves( keyStr, keySize, listId, chunkId );

		if ( SlaveWorker::disableSeal ) {
			SlaveWorker::chunkBuffer->at( listId )->set(
				this,
				keyStr, keySize,
				valueStr, valueSize,
				PROTO_OPCODE_SET, timestamp, // generated by DataChunkBuffer
				stripeId, chunkId,
				0, 0,
				this->chunks, this->dataChunk, this->parityChunk
			);
		} else {
			SlaveWorker::chunkBuffer->at( listId )->set(
				this,
				keyStr, keySize,
				valueStr, valueSize,
				PROTO_OPCODE_SET, timestamp, // generated by DataChunkBuffer
				stripeId, chunkId,
				0, 0,
				this->chunks, this->dataChunk, this->parityChunk
			);
		}

		// Check if recovery is done
		if ( SlaveWorker::pending->eraseRecovery( keySize, keyStr, instanceId, requestId, coordinatorSocket, addr, port, remainingChunks, remainingKeys, totalChunks, totalKeys ) ) {
			if ( remainingKeys == 0 ) {
				CoordinatorEvent coordinatorEvent;
				coordinatorEvent.resPromoteBackupSlave( coordinatorSocket, instanceId, requestId, addr, port, 0, totalKeys );
				this->dispatch( coordinatorEvent );
			}
		}
	}

	// printf( "handleBatchKeyValueRequest: Inserted objects = %u\n", header.count );

	// Send response to the slave peer
	event.resUnsealedKeys( event.socket, event.instanceId, event.requestId, header, true );
	this->dispatch( event );

	return false;
}

bool SlaveWorker::handleBatchChunksRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct BatchChunkHeader header;
	if ( ! this->protocol.parseBatchChunkHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleBatchChunksRequest", "Invalid BATCH_CHUNKS request." );
		return false;
	}

	uint32_t offset = 0;
	for ( uint32_t i = 0; i < header.count; i++ ) {
		uint32_t responseId;
		struct ChunkHeader chunkHeader;
		if ( ! this->protocol.nextChunkInBatchChunkHeader( header, responseId, chunkHeader, size, offset ) ) {
			__ERROR__( "SlaveWorker", "handleBatchChunksRequest", "Invalid ChunkHeader in the BATCH_CHUNKS request." );
		}

		// Convert into a normal GET_CHUNK event
		event.requestId = responseId;
		this->handleGetChunkRequest( event, chunkHeader );
	}

	return false;
}
