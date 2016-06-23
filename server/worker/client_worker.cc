#include "worker.hh"
#include "../main/server.hh"

void ServerWorker::dispatch( ClientEvent event ) {
	bool success = true, connected, isSend = true;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	if ( ServerWorker::delay )
		usleep( ServerWorker::delay );

	switch( event.type ) {
		case CLIENT_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
		case CLIENT_EVENT_TYPE_GET_RESPONSE_SUCCESS:
		case CLIENT_EVENT_TYPE_SET_RESPONSE_SUCCESS_DATA:
		case CLIENT_EVENT_TYPE_SET_RESPONSE_SUCCESS_PARITY:
		case CLIENT_EVENT_TYPE_DEGRADED_SET_RESPONSE_SUCCESS:
		case CLIENT_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
		case CLIENT_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
		case CLIENT_EVENT_TYPE_ACK_METADATA:
		case CLIENT_EVENT_TYPE_ACK_PARITY_BACKUP:
		case CLIENT_EVENT_TYPE_REVERT_DELTA_SUCCESS:
			success = true;
			break;
		case CLIENT_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
		case CLIENT_EVENT_TYPE_GET_RESPONSE_FAILURE:
		case CLIENT_EVENT_TYPE_SET_RESPONSE_FAILURE:
		case CLIENT_EVENT_TYPE_DEGRADED_SET_RESPONSE_FAILURE:
		case CLIENT_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
		case CLIENT_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
		case CLIENT_EVENT_TYPE_REVERT_DELTA_FAILURE:
			success = false;
			break;
		default:
			isSend = false;
			break;
	}

	buffer.data = this->protocol.buffer.send;
	buffer.size = 0;

	switch( event.type ) {
		// Register
		case CLIENT_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
			if ( Server::instanceId == 0 ) {
				// Wait until the server get an instance ID
				ServerWorker::eventQueue->insert( event );
				return;
			}
		case CLIENT_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_CLIENT,
				PROTO_OPCODE_REGISTER,
				0, // length
				Server::instanceId, event.requestId
			);
			break;
		// GET
		case CLIENT_EVENT_TYPE_GET_RESPONSE_SUCCESS:
		{
			char *key, *value;
			uint8_t keySize;
			uint32_t valueSize, splitOffset, splitSize;
			event.message.keyValue.deserialize( key, keySize, value, valueSize, splitOffset );
			bool isLarge = LargeObjectUtil::isLarge( keySize, valueSize, 0, &splitSize );
			if ( ! isLarge ) {
				splitOffset = 0;
				splitSize = 0;
			}
			buffer.size = this->protocol.generateKeyValueHeader(
				PROTO_MAGIC_RESPONSE_SUCCESS,
				PROTO_MAGIC_TO_CLIENT,
				event.isDegraded ? PROTO_OPCODE_DEGRADED_GET : PROTO_OPCODE_GET,
				event.instanceId, event.requestId,
				keySize, key,
				valueSize, value, 0, 0,
				splitOffset, splitSize
			);
		}
			break;
		case CLIENT_EVENT_TYPE_GET_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateKeyHeader(
				PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_CLIENT,
				event.isDegraded ? PROTO_OPCODE_DEGRADED_GET : PROTO_OPCODE_GET,
				event.instanceId, event.requestId,
				event.message.key.size,
				event.message.key.data
			);
			break;
		// SET
		case CLIENT_EVENT_TYPE_SET_RESPONSE_SUCCESS_DATA:
			if ( event.message.set.sealedCount ) {
				buffer.size = this->protocol.generateKeyBackupHeader(
					success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
					PROTO_MAGIC_TO_CLIENT,
					PROTO_OPCODE_SET,
					event.instanceId, event.requestId,
					event.message.set.timestamp,
					event.message.set.listId,
					event.message.set.stripeId,
					event.message.set.chunkId,
					event.message.set.sealed,
					event.message.set.sealedCount,
					event.message.set.key.size,
					event.message.set.key.data,
					event.message.set.key.isLarge
				);
			} else {
				buffer.size = this->protocol.generateKeyBackupHeader(
					success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
					PROTO_MAGIC_TO_CLIENT,
					PROTO_OPCODE_SET,
					event.instanceId, event.requestId,
					event.message.set.timestamp,
					event.message.set.listId,
					event.message.set.stripeId,
					event.message.set.chunkId,
					event.message.set.key.size,
					event.message.set.key.data,
					event.message.set.key.isLarge
				);
			}
			break;
		case CLIENT_EVENT_TYPE_SET_RESPONSE_SUCCESS_PARITY:
		case CLIENT_EVENT_TYPE_SET_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateKeyBackupHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_CLIENT,
				PROTO_OPCODE_SET,
				event.instanceId, event.requestId,
				event.message.set.key.size,
				event.message.set.key.data,
				event.message.set.key.isLarge
			);
			break;
		case CLIENT_EVENT_TYPE_DEGRADED_SET_RESPONSE_SUCCESS:
		case CLIENT_EVENT_TYPE_DEGRADED_SET_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateDegradedSetHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_CLIENT,
				PROTO_OPCODE_DEGRADED_SET,
				event.instanceId, event.requestId,
				event.message.remap.listId,
				event.message.remap.chunkId,
				event.message.remap.original,
				event.message.remap.remapped,
				event.message.remap.remappedCount,
				event.message.remap.key.size,
				event.message.remap.key.data,
				0, // valueSize
				0  // value
			);

			if ( event.needsFree )
				event.message.keyValueUpdate.key.free();
			break;
		// UPDATE
		case CLIENT_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
		case CLIENT_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateKeyValueUpdateHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_CLIENT,
				event.isDegraded ? PROTO_OPCODE_DEGRADED_UPDATE : PROTO_OPCODE_UPDATE,
				event.instanceId, event.requestId,
				event.message.keyValueUpdate.key.size,
				event.message.keyValueUpdate.key.data,
				event.message.keyValueUpdate.valueUpdateOffset,
				event.message.keyValueUpdate.valueUpdateSize
			);

			if ( event.needsFree )
				event.message.keyValueUpdate.key.free();
			break;
		// DELETE
		case CLIENT_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
			buffer.size = this->protocol.generateKeyBackupHeader(
				PROTO_MAGIC_RESPONSE_SUCCESS, PROTO_MAGIC_TO_CLIENT,
				event.isDegraded ? PROTO_OPCODE_DEGRADED_DELETE : PROTO_OPCODE_DELETE,
				event.instanceId, event.requestId,
				event.message.del.timestamp,
				event.message.del.listId,
				event.message.del.stripeId,
				event.message.del.chunkId,
				event.message.del.key.size,
				event.message.del.key.data,
				event.message.del.key.isLarge
			);

			if ( event.needsFree )
				event.message.del.key.free();
			break;
		case CLIENT_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateKeyBackupHeader(
				PROTO_MAGIC_RESPONSE_FAILURE, PROTO_MAGIC_TO_CLIENT,
				event.isDegraded ? PROTO_OPCODE_DEGRADED_DELETE : PROTO_OPCODE_DELETE,
				event.instanceId, event.requestId,
				event.message.del.key.size,
				event.message.del.key.data,
				event.message.del.key.isLarge
			);

			if ( event.needsFree )
				event.message.del.key.free();
			break;
		// ACK
		case CLIENT_EVENT_TYPE_ACK_METADATA:
			buffer.size = this->protocol.generateAcknowledgementHeader(
				PROTO_MAGIC_ACKNOWLEDGEMENT, PROTO_MAGIC_TO_CLIENT,
				PROTO_OPCODE_ACK_METADATA,
				event.instanceId, event.requestId,
				event.message.ack.fromTimestamp,
				event.message.ack.toTimestamp
			);
			break;
		case CLIENT_EVENT_TYPE_ACK_PARITY_BACKUP:
		{
			std::vector<uint32_t> *timestamps = event.message.revert.timestamps;
			buffer.size = this->protocol.generateDeltaAcknowledgementHeader(
				PROTO_MAGIC_ACKNOWLEDGEMENT, PROTO_MAGIC_TO_CLIENT,
				PROTO_OPCODE_ACK_PARITY_DELTA,
				Server::instanceId, event.requestId,
				timestamps ? *timestamps : std::vector<uint32_t>(),
				std::vector<Key>(),
				event.message.revert.targetId
			);
			if ( timestamps )
				delete timestamps;
		}
			break;
		case CLIENT_EVENT_TYPE_REVERT_DELTA_SUCCESS:
		case CLIENT_EVENT_TYPE_REVERT_DELTA_FAILURE:
		{
			std::vector<uint32_t> *timestamps = event.message.revert.timestamps;
			std::vector<Key> *requests = event.message.revert.requests;
			buffer.size = this->protocol.generateDeltaAcknowledgementHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_CLIENT,
				PROTO_OPCODE_REVERT_DELTA,
				Server::instanceId, event.requestId,
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
		case CLIENT_EVENT_TYPE_PENDING:
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "ServerWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else {
		// Parse requests from clients
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "ServerWorker (client)" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.magic != PROTO_MAGIC_REQUEST || header.from != PROTO_MAGIC_FROM_CLIENT ) {
				__ERROR__( "ServerWorker", "dispatch", "Invalid protocol header." );
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
					case PROTO_OPCODE_DEGRADED_SET:
						this->handleDegradedSetRequest( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_UPDATE:
						this->handleUpdateRequest( event, buffer.data, buffer.size, false );
						break;
					case PROTO_OPCODE_UPDATE_CHECK:
						this->handleUpdateRequest( event, buffer.data, buffer.size, true );
						break;
					case PROTO_OPCODE_DELETE:
						this->handleDeleteRequest( event, buffer.data, buffer.size, false );
						break;
					case PROTO_OPCODE_DELETE_CHECK:
						this->handleDeleteRequest( event, buffer.data, buffer.size, true );
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
						__ERROR__( "ServerWorker", "dispatch", "Invalid opcode from client." );
						break;
				}
			}
			buffer.data += header.length;
			buffer.size -= header.length;
		}
		if ( connected ) event.socket->done();
	}
	if ( ! connected )
		__ERROR__( "ServerWorker", "dispatch", "The client is disconnected." );
}

bool ServerWorker::handleGetRequest( ClientEvent event, char *buf, size_t size ) {
	struct KeyHeader header;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleGetRequest", "Invalid GET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleGetRequest",
		"[GET] Key: %.*s (key size = %u).",
		( int ) header.keySize, header.key, header.keySize
	);
	return this->handleGetRequest( event, header, false );
}

bool ServerWorker::handleGetRequest( ClientEvent event, struct KeyHeader &header, bool isDegraded ) {
	Key key;
	KeyValue keyValue;
	RemappedKeyValue remappedKeyValue;
	bool ret;
	if ( ( map->findObject( header.key, header.keySize, &keyValue, &key ) ) ||
	     ( header.keySize > SPLIT_OFFSET_SIZE && map->findLargeObject( header.key, header.keySize - SPLIT_OFFSET_SIZE, &keyValue, &key ) ) ) {
		event.resGet( event.socket, event.instanceId, event.requestId, keyValue, isDegraded );
		ret = true;
	} else if ( remappedBuffer->find( header.keySize, header.key, &remappedKeyValue ) ) {
		// Handle remapped keys
		event.resGet( event.socket, event.instanceId, event.requestId, remappedKeyValue.keyValue, isDegraded );
		ret = true;
	} else {
		// Try to search for large object
		char backup[ SPLIT_OFFSET_SIZE ];
		memcpy( backup, header.key + header.keySize, SPLIT_OFFSET_SIZE );
		memset( header.key + header.keySize, 0, SPLIT_OFFSET_SIZE );
		if ( map->findLargeObject( header.key, header.keySize, &keyValue, &key ) ) {
			event.resGet( event.socket, event.instanceId, event.requestId, keyValue, isDegraded );
			ret = true;
		} else {
			event.resGet( event.socket, event.instanceId, event.requestId, key, isDegraded );
			ret = false;
		}
		memcpy( header.key + header.keySize, backup, SPLIT_OFFSET_SIZE );
	}
	this->dispatch( event );
	return ret;
}

bool ServerWorker::handleSetRequest( ClientEvent event, char *buf, size_t size, bool needResSet ) {
	struct KeyValueHeader header;
	if ( ! this->protocol.parseKeyValueHeader( header, buf, size, 0, true ) ) {
		__ERROR__( "ServerWorker", "handleSetRequest", "Invalid SET request (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleSetRequest",
		"[SET] Key: %.*s (key size = %u); Value: (value size = %u); Split offset = %u",
		( int ) header.keySize, header.key, header.keySize, header.valueSize,
		header.splitOffset
	);
	return this->handleSetRequest( event, header, needResSet );
}

bool ServerWorker::handleSetRequest( ClientEvent event, struct KeyValueHeader &header, bool needResSet ) {
	uint8_t sealedCount;
	Metadata sealed[ 2 ];
	uint32_t timestamp, listId, stripeId, chunkId, splitIndex;
	ServerPeerSocket *dataServerSocket;
	bool exist = false, isLarge;

	listId = ServerWorker::stripeList->get( header.key, header.keySize, &dataServerSocket, 0, &chunkId );

	splitIndex = LargeObjectUtil::getSplitIndex( header.keySize, header.valueSize, header.splitOffset, isLarge );
	if ( isLarge ) {
		chunkId = ( chunkId + splitIndex ) % ServerWorker::dataChunkCount;
		dataServerSocket = ServerWorker::stripeList->get( listId, chunkId );
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleSetRequest",
		"[SET] Key: %.*s (key size = %u); Value: (value size = %u); Split offset = %u (index: %u); Is large? %s",
		( int ) header.keySize, header.key, header.keySize, header.valueSize,
		header.splitOffset, splitIndex,
		isLarge ? "Yes" : "No"
	);

	if (
		( ! isLarge && map->findObject( header.key, header.keySize ) ) ||
		( ServerWorker::chunkBuffer->at( listId )->findValueByKey( header.key, header.keySize, isLarge, 0, 0, false ) )
	) {
		exist = true;
	} else {
		ServerWorker::chunkBuffer->at( listId )->set(
			this,
			header.key, header.keySize,
			header.value, header.valueSize,
			PROTO_OPCODE_SET, timestamp, // generated by DataChunkBuffer
			stripeId, chunkId, header.splitOffset,
			&sealedCount, &sealed[ 0 ], &sealed[ 1 ],
			this->chunks, this->dataChunk, this->parityChunk,
			ServerWorker::getChunkBuffer
		);
	}

	if ( ! needResSet )
		return true;

	Key key;
	key.set( header.keySize, header.key, 0, isLarge );
	if ( exist ) {
		event.resSet(
			event.socket, event.instanceId, event.requestId,
			key, false
		);
	} else if ( ! dataServerSocket->self ) {
		// Parity server
		event.resSet(
			event.socket, event.instanceId, event.requestId,
			key, true
		);
	} else {
		// Data server responds with metadata
		event.resSet(
			event.socket, event.instanceId, event.requestId,
			timestamp, listId, stripeId, chunkId,
			sealedCount, sealed, key
		);
	}
	this->dispatch( event );

	return true;
}

bool ServerWorker::handleUpdateRequest( ClientEvent event, char *buf, size_t size, bool checkGetChunk ) {
	struct KeyValueUpdateHeader header;
	if ( ! this->protocol.parseKeyValueUpdateHeader( header, true, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleUpdateRequest", "Invalid UPDATE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleUpdateRequest",
		"[UPDATE] Key: %.*s (key size = %u); Value: (update size = %u, offset = %u).",
		( int ) header.keySize, header.key, header.keySize,
		header.valueUpdateSize, header.valueUpdateOffset
	);
	return this->handleUpdateRequest(
		event, header,
		0, 0, 0,
		false,
		0,
		false,
		checkGetChunk
	);
}

bool ServerWorker::handleUpdateRequest(
	ClientEvent event, struct KeyValueUpdateHeader &header,
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

	ret = ( map->findObject( header.key, header.keySize, &keyValue, &key ) ) ||
	      ( header.keySize > SPLIT_OFFSET_SIZE && map->findLargeObject( header.key, header.keySize - SPLIT_OFFSET_SIZE, &keyValue, &key ) );

	if ( ! ret ) {
		char backup[ SPLIT_OFFSET_SIZE ];
		memcpy( backup, header.key + header.keySize, SPLIT_OFFSET_SIZE );
		memset( header.key + header.keySize, 0, SPLIT_OFFSET_SIZE );
		ret = ( map->findLargeObject( header.key, header.keySize, &keyValue, &key ) );
		memcpy( header.key + header.keySize, backup, SPLIT_OFFSET_SIZE );
	}

	__INFO__(
		BLUE, "ServerWorker", "handleUpdateRequest",
		"[UPDATE] Key: %.*s (key size = %u); Value: (update size = %u, offset = %u); ret: %s.",
		( int ) header.keySize, header.key, header.keySize,
		header.valueUpdateSize, header.valueUpdateOffset,
		ret ? "true" : "false"
	);

	if ( ! ret ) {
		fprintf( stderr, "splitOffset=%u / %u\n", LargeObjectUtil::readSplitOffset( header.key + header.keySize ), LargeObjectUtil::readSplitOffset( header.key + header.keySize - SPLIT_OFFSET_SIZE ) );
	}

	if ( ret ) {
		uint8_t _keySize;
		uint32_t _valueSize, splitOffset, splitSize;
		char *_key, *_value;
		bool isLarge;
		keyValue.deserialize( _key, _keySize, _value, _valueSize, splitOffset );
		isLarge = LargeObjectUtil::isLarge( _keySize, _valueSize, 0, &splitSize );

		header.keySize = _keySize;

		// Set up KeyMetadata
		keyMetadata.length = keyValue.getSize();
		keyMetadata.obj = keyValue.data;
		chunk = ServerWorker::chunkPool->getChunk( keyValue.data, keyMetadata.offset );

		// Set up Metadata
		metadata = ChunkUtil::getMetadata( chunk );

		// Find remapping record and store it to {original, reconstructed, reconstructedCount}
		if ( remappedBuffer->find( header.keySize, header.key, &remappingRecord ) ) {
			assert( ! original && ! reconstructed && ! reconstructedCount );
			original = remappingRecord.original;
			reconstructed = remappingRecord.remapped;
			reconstructedCount = remappingRecord.remappedCount;
		}

		reconstructParity = false;
		chunks = 0;
		endOfDegradedOp = false;
		checkGetChunk = true;

		uint32_t offset = keyMetadata.offset + KEY_VALUE_METADATA_SIZE + header.keySize + header.valueUpdateOffset + ( isLarge ? SPLIT_OFFSET_SIZE : 0 );

		if ( ServerWorker::parityChunkCount ) {
			// Add the current request to the pending set
			keyValueUpdate.dup( header.keySize, header.key, ( void * ) event.socket, isLarge );
			keyValueUpdate.offset = header.valueUpdateOffset;
			keyValueUpdate.length = header.valueUpdateSize;
			keyValueUpdate.isDegraded = reconstructedCount > 0;

			if ( ! ServerWorker::pending->insertKeyValueUpdate( PT_CLIENT_UPDATE, event.instanceId, event.requestId, ( void * ) event.socket, keyValueUpdate ) ) {
				__ERROR__( "ServerWorker", "handleUpdateRequest", "Cannot insert into client UPDATE pending map (ID = (%u, %u)).", event.instanceId, event.requestId );
			}
		} else {
			key.set( header.keySize, header.key, ( void * ) event.socket, isLarge );
		}

		LOCK_T *keysLock, *chunksLock;

		ServerWorker::map->getKeysMap( 0, &keysLock );
		ServerWorker::map->getChunksMap( 0, &chunksLock );
		// Lock the data chunk buffer
		MixedChunkBuffer *chunkBuffer = ServerWorker::chunkBuffer->at( metadata.listId );
		int chunkBufferIndex = chunkBuffer->lockChunk( chunk, true );

		LOCK( keysLock );
		LOCK( chunksLock );
		// Compute delta and perform update

		if ( checkGetChunk ) {
			ServerWorker::getChunkBuffer->insert(
				metadata,
				chunkBufferIndex == -1 /* isSealed */ ? chunk : 0
			);
		}

		ChunkUtil::computeDelta(
			chunk,
			header.valueUpdate, // delta
			header.valueUpdate, // new data
			offset, header.valueUpdateSize,
			true // perform update
		);
		// Release the locks
		UNLOCK( chunksLock );
		UNLOCK( keysLock );
		if ( chunkBufferIndex == -1 )
			chunkBuffer->unlock();

		if ( ServerWorker::parityChunkCount ) {
			ret = this->sendModifyChunkRequest(
				event.instanceId, event.requestId,
				keyValueUpdate.size, keyValueUpdate.isLarge, keyValueUpdate.data,
				metadata, offset,
				header.valueUpdateSize,    // deltaSize
				header.valueUpdateOffset,
				header.valueUpdate,        // delta
				chunkBufferIndex == -1,    // isSealed
				true,                      // isUpdate
				event.timestamp,
				event.socket,              // clientSocket
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
		// __INFO__( GREEN, "ServerWorker", "handleUpdateRequest", "Handle remapped key: %.*s!", header.keySize, header.key );

		if ( ServerWorker::parityChunkCount ) {
			// Add the current request to the pending set
			keyValueUpdate.dup( header.keySize, header.key, ( void * ) event.socket );
			keyValueUpdate.offset = header.valueUpdateOffset;
			keyValueUpdate.length = header.valueUpdateSize;
			keyValueUpdate.isDegraded = reconstructedCount > 0;

			if ( ! ServerWorker::pending->insertKeyValueUpdate( PT_CLIENT_UPDATE, event.instanceId, event.requestId, ( void * ) event.socket, keyValueUpdate ) ) {
				__ERROR__( "ServerWorker", "handleUpdateRequest", "Cannot insert into client UPDATE pending map (ID = (%u, %u)).", event.instanceId, event.requestId );
			}

			// Prepare the list of parity servers
			this->getServers( remappedKeyValue.listId );
			for ( uint32_t i = 0; i < remappedKeyValue.remappedCount; i++ ) {
				uint32_t srcChunkId = remappedKeyValue.original[ i * 2 + 1 ];

				if ( srcChunkId >= ServerWorker::dataChunkCount ) {
					this->parityServerSockets[ srcChunkId - ServerWorker::dataChunkCount ] = ServerWorker::stripeList->get(
						remappedKeyValue.remapped[ i * 2     ],
						remappedKeyValue.remapped[ i * 2 + 1 ]
					);
				}
			}

			// Prepare UPDATE request
			size_t size;
			uint16_t instanceId = Server::instanceId;
			uint32_t requestId = ServerWorker::idGenerator->nextVal( this->workerId );
			Packet *packet = ServerWorker::packetPool->malloc();
			packet->setReferenceCount( ServerWorker::parityChunkCount );
			size = this->protocol.generateKeyValueUpdateHeader(
				PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_REMAPPED_UPDATE,
				event.instanceId, requestId,
				header.keySize,
				header.key,
				header.valueUpdateOffset,
				header.valueUpdateSize,
				header.valueUpdate,
				packet->data, event.timestamp
			);
			packet->size = ( uint32_t ) size;

			// Insert the UPDATE request to server pending set
			for ( uint32_t i = 0; i < ServerWorker::parityChunkCount; i++ ) {
				if ( ! ServerWorker::pending->insertKeyValueUpdate(
					PT_SERVER_PEER_UPDATE, instanceId, event.instanceId, requestId, event.requestId,
					( void * ) this->parityServerSockets[ i ],
					keyValueUpdate
				) ) {
					__ERROR__( "ServerWorker", "handleUpdateRequest", "Cannot insert into server UPDATE pending map." );
				}
			}

			// Forward the request to the parity servers
			for ( uint32_t i = 0; i < ServerWorker::parityChunkCount; i++ ) {
				// Insert into event queue
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

bool ServerWorker::handleDeleteRequest( ClientEvent event, char *buf, size_t size, bool checkGetChunk ) {
	struct KeyHeader header;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleDeleteRequest", "Invalid DELETE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleDeleteRequest",
		"[DELETE] Key: %.*s (key size = %u).",
		( int ) header.keySize, header.key, header.keySize
	);
	return this->handleDeleteRequest(
		event, header,
		0, 0, 0,
		false,
		0,
		false,
		checkGetChunk
	);
}

bool ServerWorker::handleDeleteRequest(
	ClientEvent event, struct KeyHeader &header,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	bool reconstructParity,
	Chunk **chunks, bool endOfDegradedOp,
	bool checkGetChunk
) {
	bool ret;
	Key key;
	KeyValue keyValue;
	KeyMetadata keyMetadata;
	Metadata metadata;
	RemappedKeyValue remappedKeyValue;
	Chunk *chunk;

	if ( map->findObject( header.key, header.keySize, &keyValue, &key ) ) {
		// Set up KeyMetadata
		keyMetadata.length = keyValue.getSize();
		keyMetadata.obj = keyValue.data;
		chunk = ServerWorker::chunkPool->getChunk( keyValue.data, keyMetadata.offset );

		// Set up Metadata
		metadata = ChunkUtil::getMetadata( chunk );

		uint32_t timestamp;
		uint32_t deltaSize = 0;
		char *delta = 0;

		// Add the current request to the pending set
		if ( ServerWorker::parityChunkCount ) {
			key.dup( header.keySize, header.key, ( void * ) event.socket );
			if ( ! ServerWorker::pending->insertKey( PT_CLIENT_DEL, event.instanceId, event.requestId, ( void * ) event.socket, key ) ) {
				__ERROR__( "ServerWorker", "handleDeleteRequest", "Cannot insert into client DELETE pending map." );
			}
			delta = this->buffer.data;
		} else {
			key.set( header.keySize, header.key, ( void * ) event.socket );
			delta = 0;
		}

		// Update data chunk and map
		LOCK_T *keysLock, *chunksLock;
		KeyMetadata keyMetadata;

		ServerWorker::map->getKeysMap( 0, &keysLock );
		ServerWorker::map->getChunksMap( 0, &chunksLock );
		// Lock the data chunk buffer
		MixedChunkBuffer *chunkBuffer = ServerWorker::chunkBuffer->at( metadata.listId );
		int chunkBufferIndex = chunkBuffer->lockChunk( chunk, true );
		// Lock the keys and cache map
		LOCK( keysLock );
		LOCK( chunksLock );
		// Delete the chunk and perform key-value compaction
		if ( chunkBufferIndex == -1 ) {
			// Only compute data delta if the chunk is not yet sealed
			if ( ! chunkBuffer->reInsert( this, chunk, keyMetadata.length, false, false ) ) {
				// The chunk is compacted before. Need to seal the chunk first
				// Seal from chunk->lastDelPos
				/*
				if ( chunk->lastDelPos > 0 && chunk->lastDelPos < chunk->getSize() ) {
					// Only issue seal chunk request when new key-value pairs are received
					this->issueSealChunkRequest( chunk, chunk->lastDelPos );
				}
				*/
				__ERROR__( "ServerWorker", "handleDeleteRequest", "TODO: Handle DELETE request." );
			}
		}
		ServerWorker::map->deleteKey( key, PROTO_OPCODE_DELETE, timestamp, keyMetadata, false, false );
		deltaSize = ChunkUtil::deleteObject( chunk, keyMetadata.offset, delta );
		// Release the locks
		UNLOCK( chunksLock );
		UNLOCK( keysLock );
		if ( chunkBufferIndex == -1 )
			chunkBuffer->unlock();

		if ( ServerWorker::parityChunkCount ) {
			ret = this->sendModifyChunkRequest(
				event.instanceId, event.requestId, key.size, key.isLarge, key.data,
				metadata, keyMetadata.offset, deltaSize, 0, delta,
				chunkBufferIndex == -1, // isSealed
				false,                  // isUpdate
				event.timestamp,        // timestamp
				event.socket,           // clientSocket
				original, reconstructed, reconstructedCount,
				reconstructParity,
				chunks, endOfDegradedOp
			);
		} else {
			uint32_t timestamp = Server::getInstance()->timestamp.nextVal();
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
		// __INFO__( GREEN, "ServerWorker", "handleDeleteRequest", "Handle remapped key: %.*s!", header.keySize, header.key );
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

bool ServerWorker::handleAckParityDeltaBackup( ClientEvent event, char *buf, size_t size ) {
	DeltaAcknowledgementHeader header;
	std::vector<uint32_t> timestamps;

	if ( ! this->protocol.parseDeltaAcknowledgementHeader( header, &timestamps, 0, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleAckParityDeltaBackup", "Invalid ACK parity delta backup request." );
		return false;
	}

	if ( header.tsCount != 2 ) {
		__ERROR__( "ServerWorker", "handleAckParityDeltaBackup", "Invalid ACK parity delta backup request (count < 2)." );
	} else {
		__DEBUG__(
			BLUE, "ServerWorker", "handleAckParityDeltaBackup",
			"Ack. from client fd = %u from %u to %u for data server id = %hu.",
			event.socket->getSocket(), timestamps.at( 0 ), timestamps.at( 1 ), header.targetId
		);

		Timestamp from( timestamps.at( 0 ) );
		Timestamp to( timestamps.at( 1 ) );

		event.socket->backup.removeParityUpdate( from, to, header.targetId );
		event.socket->backup.removeParityDelete( from, to, header.targetId );
	}

	event.resAckParityDelta( event.socket, Server::instanceId, event.requestId, std::vector<uint32_t>() , header.targetId );
	this->dispatch( event );

	return true;
}

bool ServerWorker::handleRevertDelta( ClientEvent event, char *buf, size_t size ) {
	DeltaAcknowledgementHeader header;
	std::vector<uint32_t> timestamps;
	std::set<uint32_t> timestampSet;
	std::vector<Key> requests;

	if ( ! this->protocol.parseDeltaAcknowledgementHeader( header, &timestamps, &requests, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleRevertDelta", "Invalid REVERT delta request." );
		return false;
	}

	if ( header.tsCount == 0 && header.keyCount == 0 && header.targetId == 0 ) {
		__ERROR__( "ServerWorker", "handleRevertDelta", "Invalid REVERT delta request." );
		event.resRevertDelta( event.socket, Server::instanceId, event.requestId, false /* success */, std::vector<uint32_t>(), std::vector<Key>(), header.targetId );
		this->dispatch( event );
		return true;
	}

	ServerPeerSocket *targetSocket = 0;
	LOCK( &this->serverPeers->lock );
	for ( uint32_t i = 0; i < this->serverPeers->size(); i++) {
		if ( this->serverPeers->values[ i ]->instanceId == header.targetId ) {
			targetSocket = this->serverPeers->values[ i ];
			break;
		}
	}
	UNLOCK( &this->serverPeers->lock );

	if ( targetSocket == 0 ) {
		__ERROR__( "ServerWorker", "handleRevertDelta", "Cannot find server with targetId %d.", header.targetId );
		return false;
	}

	while( ! Server::getInstance()->stateTransitHandler.useCoordinatedFlow( targetSocket->getAddr() ) )
		usleep(1);

	__INFO__(
		BLUE, "ServerWorker", "handleRevertDelta",
		"Revert request from client fd = %u (UPDATE/DELETE) from %u to %u size = %u, and (SET) size = %u, for failed server id = %hu.",
		event.socket->getSocket(),
		header.tsCount > 0 ? timestamps.at( 0 ) : 0,
		header.tsCount > 1 ? timestamps.at( header.tsCount - 1 ) : UINT32_MAX,
		header.tsCount, header.keyCount, header.targetId
	);

	uint32_t timestamp;
	std::vector<BackupDelta> update, del;

	// parity revert for UDPATE / DELETE //
	timestampSet.insert( timestamps.begin(), timestamps.end() );
	update = event.socket->backup.removeParityUpdate( timestampSet, header.targetId, false );
	del = event.socket->backup.removeParityDelete( timestampSet, header.targetId, false );

	// prepare to record timestamps of reverted deltas
	timestamps.clear();

	for ( auto& it : update ) {
		//it.print();
		if ( it.isChunkDelta ) {
			ServerWorker::chunkBuffer->at( it.metadata.listId )->update(
				it.metadata.stripeId, it.metadata.chunkId,
				it.delta.chunkOffset, it.delta.data.size, it.delta.data.data,
				this->chunks, this->dataChunk, this->parityChunk
			);
		} else {
			bool ret = ServerWorker::chunkBuffer->at( it.metadata.listId )->updateKeyValue(
				it.key.data, it.key.size, false,
				it.delta.valueOffset, it.delta.data.size, it.delta.data.data
			);
			/* apply to chunk if seal after backup */
			if ( ! ret ) {
				ServerWorker::chunkBuffer->at( it.metadata.listId )->update(
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
		if ( it.isChunkDelta ) {
			ServerWorker::chunkBuffer->at( it.metadata.listId )->update(
				it.metadata.stripeId, it.metadata.chunkId,
				it.delta.chunkOffset, it.delta.data.size, it.delta.data.data,
				this->chunks, this->dataChunk, this->parityChunk,
				true /* isDelete */
			);
		} else {
			/* set key-value pair back */
			/* TODO what if the chunk is sealed after backup ... */
			uint8_t sealedCount;
			Metadata sealed[ 2 ];
			ServerWorker::chunkBuffer->at( it.metadata.listId )->set(
				this,
				it.key.data, it.key.size,
				it.delta.data.data, it.delta.data.size,
				PROTO_OPCODE_SET, timestamp /* generated by DataChunkBuffer */,
				it.metadata.stripeId, it.metadata.chunkId, 0,
				&sealedCount, &sealed[ 0 ], &sealed[ 1 ],
				this->chunks, this->dataChunk, this->parityChunk,
				ServerWorker::getChunkBuffer
			);
		}
		timestamps.push_back( it.timestamp );
		it.free();
	}

	// data response for UPDATE / DELETE //
#define CHECK_RESPONSE_FOR_FAILED_PARITY( _PT_TYPE_, _PT_CLIENT_TYPE_, _OP_TYPE_, _MAP_VALUE_TYPE_ ) \
	LOCK( &ServerWorker::pending->serverPeers._OP_TYPE_##Lock ); \
	{ \
	std::unordered_multimap<PendingIdentifier, _MAP_VALUE_TYPE_>::iterator it, saveIt; \
	std::unordered_multimap<PendingIdentifier, _MAP_VALUE_TYPE_> *map = &ServerWorker::pending->serverPeers._OP_TYPE_; \
	for ( it = map->begin(), saveIt = it; it != map->end(); it = saveIt ) { \
		saveIt++; \
		if ( ( ( ServerPeerSocket * ) ( it->first.ptr ) )->instanceId != header.targetId ) \
			continue; \
		if ( ServerWorker::pending->count( _PT_TYPE_, it->first.instanceId, it->first.requestId, false, false ) == 1 ) { \
			/* response immediately */ \
			ClientEvent clientEvent; \
			KeyValueUpdate keyValueUpdate; \
			Key key; \
			PendingIdentifier pid; \
			if ( strcmp( #_OP_TYPE_, "update" ) == 0 || strcmp( #_OP_TYPE_, "updateChunk" ) == 0 ) { \
				if ( ! ServerWorker::pending->eraseKeyValueUpdate( _PT_CLIENT_TYPE_, it->first.parentInstanceId, it->first.parentRequestId, 0, &pid, &keyValueUpdate ) ) { \
					__ERROR__( "ServerWorker", "handleRevertDelta", "Cannot find a pending client UPDATE request that matches the response. This message will be discarded." ); \
					continue; \
				} \
				clientEvent.resUpdate( \
					( ClientSocket * ) pid.ptr, pid.instanceId, pid.requestId, \
					keyValueUpdate, \
					keyValueUpdate.offset, \
					keyValueUpdate.length, \
					true, true, false \
				); \
				this->dispatch( clientEvent ); \
				/*__INFO__( YELLOW, "ServerWorker", "handleRevertDelta", "Skip waiting for key %.*s for failed server id=%u", keyValueUpdate.size, keyValueUpdate.data, header.targetId ); */\
			} else if ( strcmp( #_OP_TYPE_, "delete" ) == 0 || strcmp( #_OP_TYPE_, "deleteChunk" ) == 0 ) { \
				if ( ! ServerWorker::pending->eraseKey( _PT_CLIENT_TYPE_, it->first.parentInstanceId, it->first.parentRequestId, 0, &pid, &key ) ) { \
					__ERROR__( "ServerWorker", "handleRevertDelta", "Cannot find a pending client DELETE request that matches the response. This message will be discarded." ); \
					continue; \
				} \
				clientEvent.resDelete( \
					( ClientSocket * ) pid.ptr, \
					pid.instanceId, pid.requestId, \
					key, \
					true, /* needsFree */ \
					false /* isDegraded */ \
				); \
				this->dispatch( clientEvent ); \
				/* __INFO__( YELLOW, "ServerWorker", "handleRevertDelta", "Skip waiting for key %.*s for failed server id=%u", key.size, key.data, header.targetId ); */\
			} \
		} \
		map->erase( it ); \
	} \
	UNLOCK( &ServerWorker::pending->serverPeers._OP_TYPE_##Lock ); \
	}

	CHECK_RESPONSE_FOR_FAILED_PARITY( PT_SERVER_PEER_UPDATE, PT_CLIENT_UPDATE, update, KeyValueUpdate );
	CHECK_RESPONSE_FOR_FAILED_PARITY( PT_SERVER_PEER_UPDATE_CHUNK, PT_CLIENT_UPDATE, updateChunk, ChunkUpdate );
	CHECK_RESPONSE_FOR_FAILED_PARITY( PT_SERVER_PEER_DEL, PT_CLIENT_DEL, del, Key );
	CHECK_RESPONSE_FOR_FAILED_PARITY( PT_SERVER_PEER_DEL_CHUNK, PT_CLIENT_DEL, deleteChunk, ChunkUpdate );

	// parity revert for SET //
	uint32_t chunkId, listId;

	for( Key &k : requests ) {
		listId = ServerWorker::stripeList->get( k.data, k.size, this->dataServerSockets, 0, &chunkId );
		// check if this server handles chunks for the stripe list
		if ( ServerWorker::chunkBuffer->size() < listId + 1 || ServerWorker::chunkBuffer->at( listId ) == 0 )
			continue;
		k.free();
	}

	event.resRevertDelta( event.socket, Server::instanceId, event.requestId, true /* success */, timestamps, std::vector<Key>(), header.targetId );
	this->dispatch( event );

	return true;
}
