#include "worker.hh"
#include "../main/server.hh"

bool ServerWorker::handleServerPeerRegisterRequest( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, char *buf, size_t size ) {
	struct AddressHeader header;
	if ( ! this->protocol.parseAddressHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleServerPeerRegisterRequest", "Invalid address header." );
		return false;
	}

	// Find the server peer socket in the array map
	int index = -1;
	for ( int i = 0, len = serverPeers->values.size(); i < len; i++ ) {
		if ( serverPeers->values[ i ]->equal( header.addr, header.port ) ) {
			index = i;
			break;
		}
	}

	socket->instanceId = instanceId;
	Server *server = Server::getInstance();
	LOCK( &server->sockets.serversIdToSocketLock );
	server->sockets.serversIdToSocketMap[ instanceId ] = socket;
	UNLOCK( &server->sockets.serversIdToSocketLock );

	if ( index == -1 ) {
		__ERROR__( "ServerWorker", "handleServerPeerRegisterRequest", "The server is not in the list. Ignoring this server..." );
		return false;
	}

	ServerPeerEvent event;
	event.resRegister( serverPeers->values[ index ], Server::getInstance()->instanceId, requestId, true );
	ServerWorker::eventQueue->insert( event );

	return true;
}

////////////////////////////////////////////////////////////////////////////////

bool ServerWorker::handleForwardKeyRequest( ServerPeerEvent event, char *buf, size_t size ) {
	struct ForwardKeyHeader header;
	if ( ! this->protocol.parseForwardKeyReqHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleForwardKeyRequest", "Invalid DEGRADED_SET request (size = %lu).", size );
		return false;
	}
	if ( header.opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		__DEBUG__(
			BLUE, "ServerWorker", "handleForwardKeyRequest",
			"Degraded opcode: 0x%x; list ID: %u, chunk ID: %u; key: %.*s (size = %u); value size: %u; value update size: %u, offset: %u.",
			header.opcode, header.listId, header.chunkId,
			header.keySize, header.key, header.keySize,
			header.valueSize, header.valueUpdateSize, header.valueUpdateOffset
		);
	} else {
		__DEBUG__(
			BLUE, "ServerWorker", "handleForwardKeyRequest",
			"Degraded opcode: 0x%x; list ID: %u, chunk ID: %u; key: %.*s (size = %u); value size: %u.",
			header.opcode, header.listId, header.chunkId,
			header.keySize, header.key, header.keySize,
			header.valueSize
		);
	}
	return this->handleForwardKeyRequest( event, header, false );
}

bool ServerWorker::handleForwardKeyRequest( ServerPeerEvent event, struct ForwardKeyHeader &header, bool self ) {
	Key key;
	KeyValue keyValue;
	Metadata metadata;
	DegradedMap *dmap = &ServerWorker::degradedChunkBuffer->map;
	bool isInserted;
	uint32_t timestamp;

	__DEBUG__(
		BLUE, "ServerWorker", "handleForwardKeyRequest",
		"Degraded opcode: 0x%x; list ID: %u, chunk ID: %u; key: %.*s (size = %u); value size: %u; value update size: %u, offset: %u; self = %s.",
		header.opcode, header.listId, header.chunkId,
		header.keySize, header.key, header.keySize,
		header.valueSize, header.valueUpdateSize, header.valueUpdateOffset,
		self ? "true" : "false"
	);

	metadata.set( header.listId, header.stripeId, header.chunkId );
	key.set( header.keySize, header.key );

	switch( header.opcode ) {
		case PROTO_OPCODE_DEGRADED_GET:
			keyValue._dup( header.key, header.keySize, header.value, header.valueSize );
			isInserted = dmap->insertValue( keyValue, metadata );
			if ( ! isInserted )
				keyValue.free();
			break;
		case PROTO_OPCODE_DEGRADED_UPDATE:
			keyValue._dup( header.key, header.keySize, header.value, header.valueSize );

			// Modify the key-value pair before inserting into DegradedMap
			isInserted = dmap->insertValue( keyValue, metadata );
			if ( isInserted ) {
				uint32_t dataUpdateOffset = KeyValue::getChunkUpdateOffset(
					0,                       // chunkOffset
					header.keySize,          // keySize
					header.valueUpdateOffset // valueUpdateOffset
				);
				memcpy(
					keyValue.data + dataUpdateOffset,
					header.valueUpdate,
					header.valueUpdateSize
				);
			} else {
				keyValue.free();
			}
			break;
		case PROTO_OPCODE_DEGRADED_DELETE:
			// TODO: Store the original key-value pair into backup
			// Remove the key-value pair from DegradedMap
			dmap->deleteValue( key, metadata, PROTO_OPCODE_DELETE, timestamp );
			break;
		default:
			__ERROR__( "ServerWorker", "handleForwardKeyRequest", "Undefined degraded opcode." );
			return false;
	}

	if ( ! self ) {
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
	} else {
		this->handleForwardKeyResponse( header, true, true );
	}

	return true;
}

////////////////////////////////////////////////////////////////////////////////

bool ServerWorker::handleSetRequest( ServerPeerEvent event, char *buf, size_t size ) {
	struct KeyValueHeader header;
	if ( ! this->protocol.parseKeyValueHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleSetRequest (ServerPeer)", "Invalid SET request (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleSetRequest (ServerPeer) ",
		"[SET] Key: %.*s (key size = %u); Value: (value size = %u)",
		( int ) header.keySize, header.key, header.keySize, header.valueSize
	);
	// same flow as set from clients
	ClientEvent clientEvent;
	bool success = this->handleSetRequest( clientEvent, buf, size, false );

	Key key;
	key.set( header.keySize, header.key );
	event.resSet( event.socket, event.instanceId, event.requestId, key, success );
	this->dispatch( event );

	return success;
}

bool ServerWorker::handleGetRequest( ServerPeerEvent event, bool isLarge, char *buf, size_t size ) {
	struct ListStripeKeyHeader header;
	bool ret;
	if ( ! this->protocol.parseListStripeKeyHeader( header, isLarge, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleGetRequest", "Invalid UNSEALED_GET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleGetRequest",
		"[UNSEALED_GET] List ID: %u, chunk ID: %u; key: %.*s; is large? %s.",
		header.listId, header.chunkId, header.keySize, header.key, isLarge ? "true" : "false"
	);

	Key key;
	KeyValue keyValue;

	ret = ServerWorker::chunkBuffer->at( header.listId )->findValueByKey(
		header.key, header.keySize, isLarge, &keyValue, &key
	);

	if ( ret )
		event.resGet( event.socket, event.instanceId, event.requestId, keyValue );
	else
		event.resGet( event.socket, event.instanceId, event.requestId, key );
	this->dispatch( event );
	return ret;
}

bool ServerWorker::handleUpdateRequest( ServerPeerEvent event, char *buf, size_t size ) {
	struct ChunkKeyValueUpdateHeader header;
	if ( ! this->protocol.parseChunkKeyValueUpdateHeader( header, true, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleUpdateRequest", "Invalid UPDATE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleUpdateRequest",
		"[UPDATE] Key: %.*s (key size = %u); Value: (update size = %u, offset = %u); list ID = %u, stripe ID = %u, chunk Id = %u, chunk update offset = %u.",
		( int ) header.keySize, header.key, header.keySize,
		header.valueUpdateSize, header.valueUpdateOffset,
		header.listId, header.stripeId, header.chunkId,
		header.chunkUpdateOffset
	);

	Key key;
	bool isChunkDelta = false;
	Metadata metadata;
	metadata.set( header.listId, header.stripeId, header.chunkId );

	bool ret = ServerWorker::chunkBuffer->at( header.listId )->updateKeyValue(
		header.key, header.keySize, header.isLarge,
		header.valueUpdateOffset, header.valueUpdateSize, header.valueUpdate
	);
	if ( ! ret ) {
		isChunkDelta = true;

		uint32_t myChunkId = ServerWorker::chunkBuffer->at( header.listId )->getChunkId();

		if ( myChunkId < ServerWorker::dataChunkCount ) {
			// Check remap buffer
			ret = true;
		} else {
			// Sealed
			bool checkGetChunk = true; // Force backup
			if ( checkGetChunk ) {
				Chunk *chunk = map->findChunkById( header.listId, header.stripeId, myChunkId );
				MixedChunkBuffer *chunkBuffer = ServerWorker::chunkBuffer->at( header.listId );

				uint8_t sealIndicatorCount = 0;
				bool *sealIndicator = 0;
				LOCK_T *parityChunkBufferLock = 0;

				sealIndicator = chunkBuffer->getSealIndicator( header.stripeId, sealIndicatorCount, true, false, &parityChunkBufferLock );

				metadata.chunkId = myChunkId;
				ServerWorker::getChunkBuffer->insert( metadata, chunk, sealIndicatorCount, sealIndicator );
				metadata.chunkId = header.chunkId;

				if ( parityChunkBufferLock )
					UNLOCK( parityChunkBufferLock );
			}

			// Use the chunkUpdateOffset
			ServerWorker::chunkBuffer->at( header.listId )->update(
				header.stripeId, header.chunkId,
				header.chunkUpdateOffset, header.valueUpdateSize, header.valueUpdate,
				this->chunks, this->dataChunk, this->parityChunk
			);
			ret = true;
		}
	}

	key.set( header.keySize, header.key );

	// backup parity delta ( data delta from data server )
	Timestamp timestamp( event.timestamp );
	Value value;
	value.set( header.valueUpdateSize, header.valueUpdate );
	Server *server = Server::getInstance();
	LOCK( &server->sockets.clientsIdToSocketLock );
	try {
		ClientSocket *clientSocket = server->sockets.clientsIdToSocketMap.at( event.instanceId );
		if ( clientSocket )
			clientSocket->backup.insertParityUpdate( timestamp, key, value, metadata, isChunkDelta, header.valueUpdateOffset, header.chunkUpdateOffset, event.socket->instanceId, event.requestId );
	} catch ( std::out_of_range &e ) {
		__ERROR__( "ServerWorker", "handleUpdateRequest", "Failed to backup delta at parity server for instance ID = %hu request ID = %u (Socket mapping not found).", event.instanceId, event.requestId );
	}
	UNLOCK( &server->sockets.clientsIdToSocketLock );

	event.resUpdate(
		event.socket, event.instanceId, event.requestId,
		header.listId, header.stripeId, header.chunkId, key,
		header.valueUpdateOffset,
		header.valueUpdateSize,
		header.chunkUpdateOffset,
		ret
	);
	if ( ! ret ) printf( "Failed UPDATE request by server peer!\n" );
	this->dispatch( event );

	return ret;
}

bool ServerWorker::handleDeleteRequest( ServerPeerEvent event, char *buf, size_t size ) {
	struct ChunkKeyHeader header;
	if ( ! this->protocol.parseChunkKeyHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleDeleteRequest", "Invalid DELETE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleDeleteRequest",
		"[DELETE] Key: %.*s (key size = %u); list ID = %u, stripe ID = %u, chunk Id = %u.",
		( int ) header.keySize, header.key, header.keySize,
		header.listId, header.stripeId, header.chunkId
	);

	Key key;
	Value value;
	KeyValue keyValue;

	// read for backup before delete
	bool ret = ServerWorker::chunkBuffer->at( header.listId )->findValueByKey(
		header.key, header.keySize, false, &keyValue, &key
	);
	if ( ret )
		keyValue._deserialize( key.data, key.size, value.data, value.size );

	// backup parity delta ( data delta from data server )
	Timestamp timestamp( event.timestamp );
	Metadata metadata;
	metadata.set( header.listId, header.stripeId, header.chunkId );
	Server *server = Server::getInstance();
	LOCK( &server->sockets.clientsIdToSocketLock );
	try {
		ClientSocket *clientSocket = server->sockets.clientsIdToSocketMap.at( event.instanceId );
		if ( clientSocket )
			clientSocket->backup.insertParityDelete( timestamp, key, value, metadata, false, 0, 0, event.socket->instanceId, event.requestId );
	} catch ( std::out_of_range &e ) {
		__ERROR__( "ServerWorker", "handleDeleteRequest", "Failed to backup delta at parity server for instance ID = %hu request ID = %u (Socket mapping not found).", event.instanceId, event.requestId );
	}
	UNLOCK( &server->sockets.clientsIdToSocketLock );

	ret = ServerWorker::chunkBuffer->at( header.listId )->deleteKey( header.key, header.keySize );

	event.resDelete(
		event.socket, event.instanceId, event.requestId,
		header.listId, header.stripeId, header.chunkId,
		key, ret
	);
	this->dispatch( event );

	return ret;
}

////////////////////////////////////////////////////////////////////////////////

bool ServerWorker::handleGetChunkRequest( ServerPeerEvent event, char *buf, size_t size ) {
	struct ChunkHeader header;
	if ( ! this->protocol.parseChunkHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleGetChunkRequest", "Invalid GET_CHUNK request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleGetChunkRequest",
		"[GET_CHUNK] List ID: %u; stripe ID: %u; chunk ID: %u.",
		header.listId, header.stripeId, header.chunkId
	);
	return this->handleGetChunkRequest( event, header );
}

bool ServerWorker::handleGetChunkRequest( ServerPeerEvent event, struct ChunkHeader &header ) {
	bool ret;

	Metadata metadata;
	metadata.set( header.listId, header.stripeId, header.chunkId );

	Chunk *chunk = map->findChunkById( header.listId, header.stripeId, header.chunkId, &metadata );

	ret = chunk;

	// Check whether the chunk is sealed or not
	uint8_t sealIndicatorCount = 0, _sealIndicatorCount = 0;
	bool *sealIndicator = 0, *_sealIndicator, exists;
	LOCK_T *parityChunkBufferLock = 0;

	MixedChunkBuffer *chunkBuffer = ServerWorker::chunkBuffer->at( header.listId );
	int chunkBufferIndex = chunkBuffer->lockChunk( chunk, true );
	bool isSealed = ( chunkBufferIndex == -1 );

	sealIndicator = chunkBuffer->getSealIndicator( header.stripeId, sealIndicatorCount, true, false, &parityChunkBufferLock );

	Chunk *backupChunk = ServerWorker::getChunkBuffer->find( metadata, exists, _sealIndicatorCount, _sealIndicator, true, false );

	__DEBUG__(
		BLUE, "ServerWorker", "handleGetChunkRequest",
		"[GET_CHUNK] List ID: %u; stripe ID: %u; chunk ID: %u; isSealed: %s; exists: %s, backupChunk: 0x%p.",
		header.listId, header.stripeId, header.chunkId,
		isSealed ? "true" : "false",
		exists ?  "true" : "false",
		backupChunk
	);

	if ( exists && backupChunk ) {
		delete[] sealIndicator;
		sealIndicator = _sealIndicator;
		sealIndicatorCount = _sealIndicatorCount;
		chunk = backupChunk;
	}

	event.resGetChunk(
		event.socket, event.instanceId, event.requestId,
		metadata, ret, chunkBufferIndex,
		isSealed ? chunk : 0,
		sealIndicatorCount, sealIndicator
	);

	this->dispatch( event );

	if ( ! exists ) {
		delete[] sealIndicator;
	}

	// ACK GET_CHUNK
	if ( isSealed )
		ServerWorker::getChunkBuffer->ack( metadata, false, true );
	else
		ServerWorker::getChunkBuffer->unlock();
	if ( parityChunkBufferLock )
		UNLOCK( parityChunkBufferLock );

	chunkBuffer->unlock( chunkBufferIndex );

	return ret;
}

bool ServerWorker::handleSetChunkRequest( ServerPeerEvent event, bool isSealed, bool isMigrating, char *buf, size_t size ) {
	union {
		struct ChunkDataHeader chunkData;
		struct ChunkKeyValueHeader chunkKeyValue;
	} header;
	Metadata metadata;
	char *ptr = 0;

	if ( isSealed ) {
		if ( ! this->protocol.parseChunkDataHeader( header.chunkData, buf, size ) ) {
			__ERROR__( "ServerWorker", "handleSetChunkRequest", "Invalid SET_CHUNK request." );
			return false;
		}
		metadata.set( header.chunkData.listId, header.chunkData.stripeId, header.chunkData.chunkId );
		__INFO__(
			BLUE, "ServerWorker", "handleSetChunkRequest",
			"[SET_CHUNK%s] List ID: %u; stripe ID: %u; chunk ID: %u; chunk size = %u.",
			isMigrating ? " (Migrating)" : "",
			header.chunkData.listId, header.chunkData.stripeId, header.chunkData.chunkId, header.chunkData.size
		);
	} else {
		if ( ! this->protocol.parseChunkKeyValueHeader( header.chunkKeyValue, ptr, buf, size ) ) {
			__ERROR__( "ServerWorker", "handleSetChunkRequest", "Invalid SET_CHUNK request." );
			return false;
		}
		metadata.set( header.chunkKeyValue.listId, header.chunkKeyValue.stripeId, header.chunkKeyValue.chunkId );
		__INFO__(
			BLUE, "ServerWorker", "handleSetChunkRequest",
			"[SET_CHUNK%s] List ID: %u; stripe ID: %u; chunk ID: %u; deleted = %u; count = %u.",
			isMigrating ? " (Migrating)" : "",
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
	LOCK_T *keysLock, *chunksLock;
	bool notifyCoordinator = false;
	CoordinatorEvent coordinatorEvent;

	chunk = ServerWorker::map->findChunkById(
		metadata.listId, metadata.stripeId, metadata.chunkId, 0,
		true, // needsLock
		true // needsUnlock
	);

	ServerWorker::map->getKeysMap( 0, &keysLock );
	ServerWorker::map->getChunksMap( 0, &chunksLock );

	// Lock the data chunk buffer
	MixedChunkBuffer *chunkBuffer;
	if ( isMigrating )
		chunkBuffer = ServerWorker::migratingChunkBuffer->at( metadata.listId );
	else
		chunkBuffer = ServerWorker::chunkBuffer->at( metadata.listId );

	int chunkBufferIndex = chunkBuffer->lockChunk( chunk, true );

	LOCK( keysLock );
	LOCK( chunksLock );

	ret = chunk;
	if ( ! chunk ) {
		if ( isMigrating ) ret = true;
		// Allocate memory for this chunk
		chunk = ServerWorker::chunkPool->alloc(
			metadata.listId,
			metadata.stripeId,
			metadata.chunkId
		);
		ServerWorker::map->setChunk(
			metadata.listId, metadata.stripeId, metadata.chunkId,
			chunk, metadata.chunkId >= ServerWorker::dataChunkCount,
			false, false
		);

		// Remove the chunk from pending chunk set
		uint16_t instanceId;
		uint32_t requestId, addr, remainingChunks, remainingKeys, totalChunks, totalKeys;
		uint16_t port;
		CoordinatorSocket *coordinatorSocket;
		if ( ServerWorker::pending->eraseRecovery( metadata.listId, metadata.stripeId, metadata.chunkId, instanceId, requestId, coordinatorSocket, addr, port, remainingChunks, remainingKeys, totalChunks, totalKeys ) ) {
			if ( remainingChunks == 0 ) {
				notifyCoordinator = true;
				coordinatorEvent.resPromoteBackupServer( coordinatorSocket, instanceId, requestId, addr, port, totalChunks, 0 );
			}
		}
	}

	if ( metadata.chunkId < ServerWorker::dataChunkCount ) {
		if ( isSealed ) {
			uint32_t timestamp;
			// Delete all keys in the chunk from the map
			offset = 0;
			chunkSize = ChunkUtil::getSize( chunk );
			while ( offset < chunkSize ) {
				keyValue = ChunkUtil::getObject( chunk, offset );
				keyValue._deserialize( key.data, key.size, valueStr, valueSize );

				key.set( key.size, key.data );
				ServerWorker::map->deleteKey(
					key, 0, timestamp, keyMetadata,
					false, // needsLock
					false, // needsUnlock
					false  // needsUpdateOpMetadata
				);

				objSize = KEY_VALUE_METADATA_SIZE + key.size + valueSize;
				offset += objSize;
			}

			// Replace chunk contents
			ChunkUtil::load(
				chunk,
				header.chunkData.offset,
				header.chunkData.data,
				header.chunkData.size
			);

			// Add all keys in the new chunk to the map
			offset = 0;
			chunkSize = header.chunkData.size;
			while( offset < chunkSize ) {
				uint32_t timestamp;

				keyValue = ChunkUtil::getObject( chunk, offset );
				keyValue._deserialize( key.data, key.size, valueStr, valueSize );
				objSize = KEY_VALUE_METADATA_SIZE + key.size + valueSize;

				key.set( key.size, key.data );

				keyMetadata.set( metadata.listId, metadata.stripeId, metadata.chunkId );
				keyMetadata.offset = offset;
				keyMetadata.length = objSize;

				ServerWorker::map->insertKey(
					key, 0, timestamp, keyMetadata,
					false, // needsLock
					false, // needsUnlock
					false  // needsUpdateOpMetadata
				);

				offset += objSize;
			}

			// Re-insert into data chunk buffer
			/*
			fprintf( stderr, "chunkBufferIndex = %u\n", chunkBufferIndex );
			assert( chunkBufferIndex == -1 );
			if ( ! chunkBuffer->reInsert( this, chunk, originalChunkSize - chunkSize, false, false ) ) {
				// The chunk is compacted before. Need to seal the chunk first
				__ERROR__( "ServerWorker", "handleSetChunkRequest", "TODO: Handle DELETE request." );
			}
			*/
		} else {
			struct KeyHeader keyHeader;
			struct KeyValueHeader keyValueHeader;
			uint32_t offset = ptr - buf;
			uint32_t timestamp;

			// Handle removed keys in DEGRADED_DELETE
			for ( uint32_t i = 0; i < header.chunkKeyValue.deleted; i++ ) {
				if ( ! this->protocol.parseKeyHeader( keyHeader, buf, size, offset ) ) {
					__ERROR__( "ServerWorker", "handleSetChunkRequest", "Invalid key in deleted key list." );
					break;
				}
				key.set( keyHeader.keySize, keyHeader.key );

				// Update key map and chunk
				if ( ServerWorker::map->deleteKey( key, PROTO_OPCODE_DELETE, timestamp, keyMetadata, false, false, false ) ) {
					ChunkUtil::deleteObject( chunk, keyMetadata.offset );
				} else {
					__ERROR__( "ServerWorker", "handleSetChunkRequest", "The deleted key does not exist." );
				}

				offset += PROTO_KEY_SIZE + keyHeader.keySize;
			}

			// Update key-value pairs
			for ( uint32_t i = 0; i < header.chunkKeyValue.count; i++ ) {
				if ( ! this->protocol.parseKeyValueHeader( keyValueHeader, buf, size, offset ) ) {
					__ERROR__( "ServerWorker", "handleSetChunkRequest", "Invalid key-value pair in updated value list." );
					break;
				}

				// Update the key-value pair
				if ( ServerWorker::map->findObject( keyValueHeader.key, keyValueHeader.keySize, &keyValue, 0, false, false ) ) {
					keyValue._deserialize( key.data, key.size, valueStr, valueSize );
					assert( valueSize == keyValueHeader.valueSize );
					memcpy( valueStr, keyValueHeader.value, valueSize );
				} else {
					uint32_t chunkId, listId;
					listId = this->stripeList->get( keyValueHeader.key, keyValueHeader.keySize, 0, 0, &chunkId );
					__ERROR__( "ServerWorker", "handleSetChunkRequest", "The updated key-value pair does not exist %.*s, list = %u chunk %d", keyValueHeader.keySize, keyValueHeader.key, listId, chunkId );
				}

				offset += PROTO_KEY_VALUE_SIZE + keyValueHeader.keySize + keyValueHeader.valueSize;
			}
		}
	} else {
		// Replace chunk contents for parity chunks
		ChunkUtil::load(
			chunk,
			header.chunkData.offset,
			header.chunkData.data,
			header.chunkData.size
		);
	}

	UNLOCK( chunksLock );
	UNLOCK( keysLock );
	if ( chunkBufferIndex != -1 )
		chunkBuffer->updateAndUnlockChunk( chunkBufferIndex );
	else
		chunkBuffer->unlock( chunkBufferIndex );

	if ( notifyCoordinator ) {
		ServerWorker::eventQueue->insert( coordinatorEvent );
	}

	if ( isSealed || header.chunkKeyValue.isCompleted ) {
		event.resSetChunk( event.socket, event.instanceId, event.requestId, metadata, ret, isMigrating );
		this->dispatch( event );
	}

	return ret;
}


bool ServerWorker::handleUpdateChunkRequest( ServerPeerEvent event, char *buf, size_t size, bool checkGetChunk ) {
	struct ChunkUpdateHeader header;
	bool ret;
	if ( ! this->protocol.parseChunkUpdateHeader( header, true, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleUpdateChunkRequest", "Invalid UPDATE_CHUNK request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleUpdateChunkRequest",
		"[UPDATE_CHUNK] List ID: %u; stripe ID: %u; chunk ID: %u; offset: %u; length: %u; updating chunk ID: %u",
		header.listId, header.stripeId, header.chunkId,
		header.offset, header.length,
		header.updatingChunkId
	);

	Metadata metadata;
	metadata.set( header.listId, header.stripeId, header.chunkId );

	uint32_t myChunkId = ServerWorker::chunkBuffer->at( header.listId )->getChunkId();

	if ( Server::getInstance()->stateTransitHandler.useCoordinatedFlow( event.socket->getAddr() ) ) {
		event.resUpdateChunk(
			event.socket, event.instanceId, event.requestId, metadata,
			header.offset, header.length,
			header.updatingChunkId, false
		);
		ServerWorker::eventQueue->insert( event );
		__DEBUG__( YELLOW, "ServerWorker", "handleUpdateChunkRequest", "Reply false to failed server id=%d", event.socket->instanceId );
		return false;
	}

	if ( header.updatingChunkId == myChunkId ) {
		// Normal UPDATE_CHUNK request //
		if ( checkGetChunk ) {
			Chunk *chunk = map->findChunkById( header.listId, header.stripeId, header.updatingChunkId );
			MixedChunkBuffer *chunkBuffer = ServerWorker::chunkBuffer->at( header.listId );
			int chunkBufferIndex = chunkBuffer->lockChunk( chunk, true );
			metadata.chunkId = header.updatingChunkId;

			uint8_t sealIndicatorCount = 0;
			bool *sealIndicator = 0;
			LOCK_T *parityChunkBufferLock = 0;
			if ( metadata.chunkId >= ServerWorker::dataChunkCount ) {
				sealIndicator = chunkBuffer->getSealIndicator( header.stripeId, sealIndicatorCount, true, false, &parityChunkBufferLock );
			}

			ServerWorker::getChunkBuffer->insert( metadata, chunk, sealIndicatorCount, sealIndicator );

			if ( parityChunkBufferLock )
				UNLOCK( parityChunkBufferLock );

			metadata.chunkId = header.chunkId;
			chunkBuffer->unlock( chunkBufferIndex );
		}

		ret = ServerWorker::chunkBuffer->at( header.listId )->update(
			header.stripeId, header.chunkId,
			header.offset, header.length, header.delta,
			this->chunks, this->dataChunk, this->parityChunk
		);

		if ( ! ret ) {
			__ERROR__( "ServerWorker", "handleUpdateChunkRequest", "Failed to update chunk for instance ID = %hu request ID = %u (Socket mapping not found).", event.instanceId, event.requestId );
		}

		// backup parity chunk delta ( data chunk delta from data server )
		Timestamp timestamp( event.timestamp );
		Key key;
		key.set( 0, 0 );
		Value value;
		value.set( header.length, header.delta );
		Server *server = Server::getInstance();
		LOCK( &server->sockets.clientsIdToSocketLock );
		try {
			ClientSocket *clientSocket = server->sockets.clientsIdToSocketMap.at( event.instanceId );
			if ( clientSocket )
			clientSocket->backup.insertParityUpdate( timestamp, key, value, metadata, true, 0, header.offset, event.socket->instanceId, event.requestId );
		} catch ( std::out_of_range &e ) {
			__ERROR__( "ServerWorker", "handleUpdateChunkRequest", "Failed to backup delta at parity server for instance ID = %hu request ID = %u (Socket mapping not found).", event.instanceId, event.requestId );
		}
		UNLOCK( &server->sockets.clientsIdToSocketLock );
	} else {
		// Update to reconstructed chunk //
		ret = ServerWorker::degradedChunkBuffer->update(
			header.listId, header.stripeId, header.chunkId,
			header.updatingChunkId, // For verifying the reconstructed chunk exists
			header.offset, header.length, header.delta,
			this->chunks, this->dataChunk, this->parityChunk
		);

		if ( ! ret ) {
			__ERROR__( "ServerWorker", "handleUpdateChunkRequest", "Chunk not found: (%u, %u, %u).", header.listId, header.stripeId, header.updatingChunkId );
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

bool ServerWorker::handleDeleteChunkRequest( ServerPeerEvent event, char *buf, size_t size, bool checkGetChunk ) {
	struct ChunkUpdateHeader header;
	bool ret;
	if ( ! this->protocol.parseChunkUpdateHeader( header, true, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleDeleteChunkRequest", "Invalid DELETE_CHUNK request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleDeleteChunkRequest",
		"[DELETE_CHUNK] List ID: %u, stripe ID: %u, chunk ID: %u; offset: %u; length: %u; updating chunk ID: %u.",
		header.listId, header.stripeId, header.chunkId,
		header.offset, header.length, header.updatingChunkId
	);

	Metadata metadata;
	metadata.set( header.listId, header.stripeId, header.chunkId );

	uint32_t myChunkId = ServerWorker::chunkBuffer->at( header.listId )->getChunkId();

	if ( header.updatingChunkId == myChunkId ) {
		// Normal DELETE_CHUNK request //
		ret = ServerWorker::chunkBuffer->at( header.listId )->update(
			header.stripeId, header.chunkId,
			header.offset, header.length, header.delta,
			this->chunks, this->dataChunk, this->parityChunk,
			true // isDelete
		);

		// backup parity chunk delta ( data chunk delta from data server )
		Timestamp timestamp( event.timestamp );
		Key key;
		key.set( 0, 0 );
		Value value;
		value.set( header.length, header.delta );
		Server *server = Server::getInstance();
		LOCK( &server->sockets.clientsIdToSocketLock );
		try{
			ClientSocket *clientSocket = server->sockets.clientsIdToSocketMap.at( event.instanceId );
			if ( clientSocket )
				clientSocket->backup.insertParityDelete( timestamp, key, value, metadata, true, 0, header.offset, event.socket->instanceId, event.requestId );
		} catch ( std::out_of_range &e ) {
			__ERROR__( "ServerWorker", "handleDeleteChunkRequest", "Failed to backup delta at parity server for instance ID = %hu request ID = %u (Socket mapping not found).", event.instanceId, event.requestId );
		}
		UNLOCK( &server->sockets.clientsIdToSocketLock );
	} else {
		// Update to reconstructed chunk //
		ret = ServerWorker::degradedChunkBuffer->update(
			header.listId, header.stripeId, header.chunkId,
			header.updatingChunkId, // For verifying the reconstructed chunk exists
			header.offset, header.length, header.delta,
			this->chunks, this->dataChunk, this->parityChunk
		);

		if ( ! ret ) {
			__ERROR__( "ServerWorker", "handleUpdateChunkRequest", "Chunk not found: (%u, %u, %u).", header.listId, header.stripeId, header.updatingChunkId );
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

bool ServerWorker::handleSealChunkRequest( ServerPeerEvent event, char *buf, size_t size ) {
	struct ChunkSealHeader header;
	bool ret;
	if ( ! this->protocol.parseChunkSealHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleSealChunkRequest", "Invalid SEAL_CHUNK request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleSealChunkRequest",
		"[SEAL_CHUNK] List ID: %u, stripe ID: %u, chunk ID: %u; count = %u.",
		header.listId, header.stripeId, header.chunkId, header.count
	);

	ret = ServerWorker::chunkBuffer->at( header.listId )->seal(
		header.stripeId, header.chunkId, header.count,
		buf + PROTO_CHUNK_SEAL_SIZE,
		size - PROTO_CHUNK_SEAL_SIZE,
		this->chunks, this->dataChunk, this->parityChunk
	);

	if ( ! ret ) {
		event.socket->printAddress();
		printf( " " );
		fflush( stdout );
		__ERROR__( "ServerWorker", "handleSealChunkRequest", "[%u, %u] Cannot update parity chunk.", event.instanceId, event.requestId );
	}

	return true;
}

bool ServerWorker::issueSealChunkRequest( Chunk *chunk, uint32_t startPos ) {
	// The chunk is locked when this function is called
	// Only issue seal chunk request when new key-value pairs are received
	if ( ServerWorker::parityChunkCount && startPos < ChunkUtil::getSize( chunk ) ) {
		size_t size;
		uint16_t instanceId = Server::instanceId;
		uint32_t requestId = ServerWorker::idGenerator->nextVal( this->workerId );
		Packet *packet = ServerWorker::packetPool->malloc();
		packet->setReferenceCount( ServerWorker::parityChunkCount );

		// Find parity servers
		this->getServers( ChunkUtil::getListId( chunk ) );

		// Prepare seal chunk request
		this->protocol.reqSealChunk( size, instanceId, requestId, chunk, startPos, packet->data );
		packet->size = ( uint32_t ) size;

		if ( size == PROTO_HEADER_SIZE + PROTO_CHUNK_SEAL_SIZE ) {
			packet->setReferenceCount( 1 );
			ServerWorker::packetPool->free( packet );
			return true;
		}

		for ( uint32_t i = 0; i < ServerWorker::parityChunkCount; i++ ) {
			ServerPeerEvent serverPeerEvent;
			serverPeerEvent.send( this->parityServerSockets[ i ], packet );

#ifdef SERVER_WORKER_SEND_REPLICAS_PARALLEL
			if ( i == ServerWorker::parityChunkCount - 1 )
				this->dispatch( serverPeerEvent );
			else
				ServerWorker::eventQueue->prioritizedInsert( serverPeerEvent );
#else
			this->dispatch( serverPeerEvent );
#endif
		}
	}
	return true;
}

bool ServerWorker::handleBatchKeyValueRequest( ServerPeerEvent event, char *buf, size_t size ) {
	struct BatchKeyValueHeader header;
	if ( ! this->protocol.parseBatchKeyValueHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleBatchKeyValueRequest", "Invalid BATCH_KEY_VALUE request." );
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
			this->getServers( keyStr, keySize, listId, chunkId );

		ServerWorker::chunkBuffer->at( listId )->set(
			this,
			keyStr, keySize,
			valueStr, valueSize,
			PROTO_OPCODE_SET, timestamp, // generated by DataChunkBuffer
			stripeId, chunkId, 0,
			0, 0, 0,
			this->chunks, this->dataChunk, this->parityChunk,
			ServerWorker::getChunkBuffer
		);

		// Check if recovery is done
		if ( ServerWorker::pending->eraseRecovery( keySize, keyStr, instanceId, requestId, coordinatorSocket, addr, port, remainingChunks, remainingKeys, totalChunks, totalKeys ) ) {
			if ( remainingKeys == 0 ) {
				CoordinatorEvent coordinatorEvent;
				coordinatorEvent.resPromoteBackupServer( coordinatorSocket, instanceId, requestId, addr, port, 0, totalKeys );
				this->dispatch( coordinatorEvent );
			}
		}
	}

	// Send response to the server peer
	event.resUnsealedKeys( event.socket, event.instanceId, event.requestId, header, true );
	this->dispatch( event );

	return false;
}

bool ServerWorker::handleBatchChunksRequest( ServerPeerEvent event, char *buf, size_t size ) {
	struct BatchChunkHeader header;
	if ( ! this->protocol.parseBatchChunkHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleBatchChunksRequest", "Invalid BATCH_CHUNKS request." );
		return false;
	}

	uint32_t offset = 0;
	for ( uint32_t i = 0; i < header.count; i++ ) {
		uint32_t responseId;
		struct ChunkHeader chunkHeader;
		if ( ! this->protocol.nextChunkInBatchChunkHeader( header, responseId, chunkHeader, size, offset ) ) {
			__ERROR__( "ServerWorker", "handleBatchChunksRequest", "Invalid ChunkHeader in the BATCH_CHUNKS request." );
		}

		// Convert into a normal GET_CHUNK event
		event.requestId = responseId;
		this->handleGetChunkRequest( event, chunkHeader );
	}

	return false;
}
