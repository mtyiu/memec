#include "worker.hh"
#include "../main/server.hh"

void ServerWorker::dispatch( ServerPeerEvent event ) {
	bool success, connected, isSend, isCompleted = true;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	isSend = ( event.type != SERVER_PEER_EVENT_TYPE_PENDING && event.type != SERVER_PEER_EVENT_TYPE_DEFERRED );
	success = false;

	buffer.data = this->protocol.buffer.send;

	switch( event.type ) {
		//////////////
		// Requests //
		//////////////
		case SERVER_PEER_EVENT_TYPE_REGISTER_REQUEST:
			buffer.size = this->protocol.generateAddressHeader(
				PROTO_MAGIC_REQUEST,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_REGISTER,
				Server::instanceId,
				ServerWorker::idGenerator->nextVal( this->workerId ),
				ServerWorker::serverServerAddr->addr,
				ServerWorker::serverServerAddr->port
			);
			break;
		case SERVER_PEER_EVENT_TYPE_GET_CHUNK_REQUEST:
			buffer.size = this->protocol.generateChunkHeader(
				PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_GET_CHUNK,
				event.instanceId, event.requestId,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId
			);
			break;
		case SERVER_PEER_EVENT_TYPE_SET_CHUNK_REQUEST:
			if ( event.message.chunk.chunk ) {
				uint32_t offset, size;
				char *data;

				data = ChunkUtil::getData( event.message.chunk.chunk, offset, size );
				// The chunk is sealed
				buffer.size = this->protocol.generateChunkDataHeader(
					PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
					PROTO_OPCODE_SET_CHUNK,
					event.instanceId, event.requestId,
					event.message.chunk.metadata.listId,
					event.message.chunk.metadata.stripeId,
					event.message.chunk.metadata.chunkId,
					size, offset, data, 0, 0
				);

				if ( event.message.chunk.needsFree ) {
					this->tempChunkPool.free( event.message.chunk.chunk );
				}
			} else {
				DegradedMap &map = ServerWorker::degradedChunkBuffer->map;
				buffer.size = this->protocol.generateChunkKeyValueHeader(
					PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
					PROTO_OPCODE_SET_CHUNK_UNSEALED,
					event.instanceId, event.requestId,
					event.message.chunk.metadata.listId,
					event.message.chunk.metadata.stripeId,
					event.message.chunk.metadata.chunkId,
					&map.unsealed.values,
					&map.unsealed.metadataRev,
					&map.unsealed.deleted,
					&map.unsealed.lock,
					isCompleted
				);
				if ( ! isCompleted ) {
					ServerPeerEvent newEvent;
					newEvent.reqSetChunk(
						event.socket,
						event.instanceId, event.requestId,
						event.message.chunk.metadata,
						0, // unsealed chunk
						false
					);
				}
			}
			break;
		case SERVER_PEER_EVENT_TYPE_SET_REQUEST:
			buffer.size = this->protocol.generateKeyValueHeader(
				PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_SET,
				event.instanceId, event.requestId,
				event.message.set.key.size,
				event.message.set.key.data,
				event.message.set.value.size,
				event.message.set.value.data
			);
			break;
		case SERVER_PEER_EVENT_TYPE_SET_RESPONSE_SUCCESS:
			success = true;
		case SERVER_PEER_EVENT_TYPE_SET_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateKeyBackupHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_SET,
				event.instanceId, event.requestId,
				event.message.set.key.size,
				event.message.set.key.data
			);
			break;
		case SERVER_PEER_EVENT_TYPE_FORWARD_KEY_REQUEST:
			buffer.size = this->protocol.generateForwardKeyReqHeader(
				PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_FORWARD_KEY,
				event.instanceId, event.requestId,
				event.message.forwardKey.opcode,
				event.message.forwardKey.listId,
				event.message.forwardKey.stripeId,
				event.message.forwardKey.chunkId,
				event.message.forwardKey.keySize,
				event.message.forwardKey.key,
				event.message.forwardKey.valueSize,
				event.message.forwardKey.value,
				event.message.forwardKey.update.length,
				event.message.forwardKey.update.offset,
				event.message.forwardKey.update.data
			);
			break;
		case SERVER_PEER_EVENT_TYPE_FORWARD_KEY_RESPONSE_SUCCESS:
			success = true;
		case SERVER_PEER_EVENT_TYPE_FORWARD_KEY_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateForwardKeyResHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_FORWARD_KEY,
				event.instanceId, event.requestId,
				event.message.forwardKey.opcode,
				event.message.forwardKey.listId,
				event.message.forwardKey.stripeId,
				event.message.forwardKey.chunkId,
				event.message.forwardKey.keySize,
				event.message.forwardKey.key,
				event.message.forwardKey.valueSize,
				event.message.forwardKey.update.length,
				event.message.forwardKey.update.offset
			);
			break;
		case SERVER_PEER_EVENT_TYPE_FORWARD_CHUNK_REQUEST:
			buffer.size = this->protocol.generateChunkDataHeader(
				PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_FORWARD_CHUNK,
				event.instanceId, event.requestId,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId,
				ChunkUtil::getSize( event.message.chunk.chunk ),
				0,
				ChunkUtil::getData( event.message.chunk.chunk ),
				0, 0
			);
			break;
		case SERVER_PEER_EVENT_TYPE_SEAL_CHUNK_REQUEST:
			this->issueSealChunkRequest( event.message.chunk.chunk );
			return;
		case SERVER_PEER_EVENT_TYPE_GET_REQUEST:
			buffer.size = this->protocol.generateListStripeKeyHeader(
				PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_GET,
				event.instanceId, event.requestId,
				event.message.get.listId,
				event.message.get.chunkId,
				event.message.get.key.size,
				event.message.get.key.data
			);
			break;
		///////////////
		// Responses //
		///////////////
		// Register
		case SERVER_PEER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
			success = true; // default is false
		case SERVER_PEER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
		{
			Server *server = Server::getInstance();
			bool isRecovering;

			LOCK( &server->status.lock );
			isRecovering = server->status.isRecovering;
			UNLOCK( &server->status.lock );

			if ( isRecovering ) {
				// Hold all register requests
				ServerWorker::pending->insertServerPeerRegistration( event.requestId, event.socket, success );
				return;
			}
		}

			buffer.size = this->protocol.generateHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_REGISTER,
				0, // length
				Server::instanceId, event.requestId
			);
			break;
		case SERVER_PEER_EVENT_TYPE_DEGRADED_SET_RESPONSE_SUCCESS:
			__ERROR__( "ServerWorker", "dispatch", "SERVER_PEER_EVENT_TYPE_DEGRADED_SET_RESPONSE_SUCCESS is not supported." );
			success = true; // default is false
			break;
		case SERVER_PEER_EVENT_TYPE_DEGRADED_SET_RESPONSE_FAILURE:
			__ERROR__( "ServerWorker", "dispatch", "SERVER_PEER_EVENT_TYPE_DEGRADED_SET_RESPONSE_FAILURE is not supported." );
			break;
		// GET
		case SERVER_PEER_EVENT_TYPE_GET_RESPONSE_SUCCESS:
		{
			char *key, *value;
			uint8_t keySize;
			uint32_t valueSize;
			event.message.get.keyValue.deserialize( key, keySize, value, valueSize );
			buffer.size = this->protocol.generateKeyValueHeader(
				PROTO_MAGIC_RESPONSE_SUCCESS,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_GET,
				event.instanceId, event.requestId,
				keySize, key,
				valueSize, value
			);
		}
			break;
		case SERVER_PEER_EVENT_TYPE_GET_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateKeyHeader(
				PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_GET,
				event.instanceId, event.requestId,
				event.message.get.key.size,
				event.message.get.key.data
			);
			break;
		// UPDATE_CHUNK
		case SERVER_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS:
			success = true; // default is false
		case SERVER_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateChunkUpdateHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_UPDATE_CHUNK,
				event.instanceId, event.requestId,
				event.message.chunkUpdate.metadata.listId,
				event.message.chunkUpdate.metadata.stripeId,
				event.message.chunkUpdate.metadata.chunkId,
				event.message.chunkUpdate.offset,
				event.message.chunkUpdate.length,
				event.message.chunkUpdate.updatingChunkId
			);
			break;
		// UPDATE
		case SERVER_PEER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
			success = true; // default is false
		case SERVER_PEER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateChunkKeyValueUpdateHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_UPDATE,
				event.instanceId, event.requestId,
				event.message.update.listId,
				event.message.update.stripeId,
				event.message.update.chunkId,
				event.message.update.key.size,
				event.message.update.key.data,
				event.message.update.valueUpdateOffset,
				event.message.update.length,
				event.message.update.chunkUpdateOffset, 0
			);
			break;
		// DELETE
		case SERVER_PEER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
			success = true; // default is false
		case SERVER_PEER_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateChunkKeyHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_DELETE,
				event.instanceId, event.requestId,
				event.message.del.listId,
				event.message.del.stripeId,
				event.message.del.chunkId,
				event.message.del.key.size,
				event.message.del.key.data
			);
			break;
		// REMAPPING_UPDATE
		case SERVER_PEER_EVENT_TYPE_REMAPPED_UPDATE_RESPONSE_SUCCESS:
			success = true;
		case SERVER_PEER_EVENT_TYPE_REMAPPED_UPDATE_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateKeyValueUpdateHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_REMAPPED_UPDATE,
				event.instanceId, event.requestId,
				event.message.remappingUpdate.key.size,
				event.message.remappingUpdate.key.data,
				event.message.remappingUpdate.valueUpdateOffset,
				event.message.remappingUpdate.valueUpdateSize
			);
			break;
		// REMAPPING_DELETE
		case SERVER_PEER_EVENT_TYPE_REMAPPED_DELETE_RESPONSE_SUCCESS:
			success = true;
		case SERVER_PEER_EVENT_TYPE_REMAPPED_DELETE_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateKeyHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_REMAPPED_DELETE,
				event.instanceId, event.requestId,
				event.message.remappingDel.key.size,
				event.message.remappingDel.key.data
			);
			break;
		// DELETE_CHUNK
		case SERVER_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS:
			success = true; // default is false
		case SERVER_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateChunkUpdateHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_DELETE_CHUNK,
				event.instanceId, event.requestId,
				event.message.chunkUpdate.metadata.listId,
				event.message.chunkUpdate.metadata.stripeId,
				event.message.chunkUpdate.metadata.chunkId,
				event.message.chunkUpdate.offset,
				event.message.chunkUpdate.length,
				event.message.chunkUpdate.updatingChunkId
			);
			break;
		// GET_CHUNK
		case SERVER_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_SUCCESS:
		{
			char *data = 0;
			uint32_t size = 0, offset = 0;

			if ( event.message.chunk.chunk )
				data = ChunkUtil::getData( event.message.chunk.chunk, offset, size );

			buffer.size = this->protocol.generateChunkDataHeader(
				PROTO_MAGIC_RESPONSE_SUCCESS, PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_GET_CHUNK,
				event.instanceId, event.requestId,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId,
				size, offset, data,
				event.message.chunk.sealIndicatorCount,
				event.message.chunk.sealIndicator
			);
		}
			break;
		case SERVER_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateChunkHeader(
				PROTO_MAGIC_RESPONSE_FAILURE, PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_GET_CHUNK,
				event.instanceId, event.requestId,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId
			);
			ServerWorker::chunkBuffer
				->at( event.message.chunk.metadata.listId )
				->unlock( event.message.chunk.chunkBufferIndex );
			break;
		// SET_CHUNK
		case SERVER_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_SUCCESS:
			success = true; // default is false
		case SERVER_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateChunkHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_SET_CHUNK,
				event.instanceId, event.requestId,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId
			);
			break;
		// FORWARD_CHUNK
		case SERVER_PEER_EVENT_TYPE_FORWARD_CHUNK_RESPONSE_SUCCESS:
			success = true;
		case SERVER_PEER_EVENT_TYPE_FORWARD_CHUNK_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateChunkHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_FORWARD_CHUNK,
				event.instanceId, event.requestId,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId
			);
			break;
		// SEAL_CHUNK
		case SERVER_PEER_EVENT_TYPE_SEAL_CHUNK_RESPONSE_SUCCESS:
			success = true; // default is false
		case SERVER_PEER_EVENT_TYPE_SEAL_CHUNK_RESPONSE_FAILURE:
			// TODO: Is a response message for SEAL_CHUNK request required?
			return;
		/////////////////////////////////////
		// Seal chunks in the chunk buffer //
		/////////////////////////////////////
		case SERVER_PEER_EVENT_TYPE_SEAL_CHUNKS:
			printf( "\tSealing %lu chunks...\n", event.message.chunkBuffer->seal( this ) );
			return;
		/////////////////////////////////
		// Reconstructed unsealed keys //
		/////////////////////////////////
		case SERVER_PEER_EVENT_TYPE_UNSEALED_KEYS_RESPONSE_SUCCESS:
			success = true;
		case SERVER_PEER_EVENT_TYPE_UNSEALED_KEYS_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateBatchKeyHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_BATCH_KEY_VALUES,
				event.instanceId, event.requestId,
				event.message.unsealedKeys.header
			);
			break;
		////////////////////
		// Deferred event //
		////////////////////
		case SERVER_PEER_EVENT_TYPE_DEFERRED:
			switch( event.message.defer.opcode ) {
				case PROTO_OPCODE_BATCH_CHUNKS:
					this->handleBatchChunksRequest(
						event,
						event.message.defer.buf,
						event.message.defer.size
					);
					break;
				default:
					__ERROR__( "ServerWorker", "dispatch", "Undefined deferred event." );
			}
			delete[] event.message.defer.buf;
			break;
		//////////
		// Send //
		//////////
		case SERVER_PEER_EVENT_TYPE_SEND:
			event.message.send.packet->read( buffer.data, buffer.size );
			break;
		///////////
		// Batch //
		///////////
		case SERVER_PEER_EVENT_TYPE_BATCH_GET_CHUNKS:
		{
			uint16_t instanceId = Server::instanceId;
			uint32_t chunksCount = 0;
			bool isCompleted = false;

			buffer.size = this->protocol.generateBatchChunkHeader(
				PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_BATCH_CHUNKS,
				instanceId, ServerWorker::idGenerator->nextVal( this->workerId ),
				event.message.batchGetChunks.requestIds,
				event.message.batchGetChunks.metadata,
				chunksCount,
				isCompleted
			);

			if ( ! isCompleted ) {
				event.batchGetChunks(
					event.socket,
					event.message.batchGetChunks.requestIds,
					event.message.batchGetChunks.metadata
				);
				ServerWorker::eventQueue->insert( event ); // Cannot use dispatch() because the send buffer is dirty
			}
		}
			break;
		/////////////
		// Pending //
		/////////////
		case SERVER_PEER_EVENT_TYPE_PENDING:
			break;
		default:
			return;
	}

	if ( isSend ) {
		assert( ! event.socket->self );
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "ServerWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );

		if ( event.type == SERVER_PEER_EVENT_TYPE_SEND ) {
			ServerWorker::packetPool->free( event.message.send.packet );
		}
	} else {
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "ServerWorker (server peer)" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_SERVER ) {
				__ERROR__( "ServerWorker", "dispatch", "Invalid protocol header." );
				goto quit_1;
			}
			event.instanceId = header.instanceId;
			event.requestId = header.requestId;
			event.timestamp = header.timestamp;
			switch ( header.opcode ) {
				case PROTO_OPCODE_REGISTER:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleServerPeerRegisterRequest( event.socket, header.instanceId, header.requestId, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
						{
							event.socket->registered = true;
							event.socket->instanceId = header.instanceId;
							Server *server = Server::getInstance();
							LOCK( &server->sockets.serversIdToSocketLock );
							server->sockets.serversIdToSocketMap[ header.instanceId ] = event.socket;
							UNLOCK( &server->sockets.serversIdToSocketLock );
						}
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							__ERROR__( "ServerWorker", "dispatch", "Failed to register with server." );
							break;
						default:
							__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from server: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_SEAL_CHUNK:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleSealChunkRequest( event, buffer.data, header.length );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleSealChunkResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleSealChunkResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from server: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_FORWARD_KEY:
					switch ( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleForwardKeyRequest( event, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleForwardKeyResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleForwardKeyResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from server: 0x%x for SET.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_SET:
					switch ( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleSetRequest( event, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleSetResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleSetResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from server: 0x%x for SET.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_GET:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleGetRequest( event, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleGetResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleGetResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from server: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_UPDATE:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleUpdateRequest( event, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleUpdateResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleUpdateResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from server: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_DELETE:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleDeleteRequest( event, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleDeleteResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleDeleteResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from server: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_REMAPPED_UPDATE:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleRemappedUpdateRequest( event, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleRemappedUpdateResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleRemappedUpdateResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from server: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_REMAPPED_DELETE:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleRemappedDeleteRequest( event, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleRemappedDeleteResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleRemappedDeleteResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from server: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_UPDATE_CHUNK:
				case PROTO_OPCODE_UPDATE_CHUNK_CHECK:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleUpdateChunkRequest( event, buffer.data, buffer.size, header.opcode == PROTO_OPCODE_UPDATE_CHUNK_CHECK );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleUpdateChunkResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleUpdateChunkResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from server: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_DELETE_CHUNK:
				case PROTO_OPCODE_DELETE_CHUNK_CHECK:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleDeleteChunkRequest( event, buffer.data, buffer.size, header.opcode == PROTO_OPCODE_DELETE_CHUNK_CHECK );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleDeleteChunkResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleDeleteChunkResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from server: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_GET_CHUNK:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleGetChunkRequest( event, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleGetChunkResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleGetChunkResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from server: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_SET_CHUNK:
				case PROTO_OPCODE_SET_CHUNK_UNSEALED:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleSetChunkRequest( event, header.opcode == PROTO_OPCODE_SET_CHUNK, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleSetChunkResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleSetChunkResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from server." );
							break;
					}
					break;
				case PROTO_OPCODE_FORWARD_CHUNK:
					switch ( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleForwardChunkRequest( event, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleForwardChunkResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleForwardChunkResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from server." );
							break;
					}
					break;
				case PROTO_OPCODE_BATCH_CHUNKS:
					switch ( header.magic ) {
						case PROTO_MAGIC_REQUEST:
						{
							// Dump the buffer
							ServerPeerEvent deferredEvent;
							deferredEvent.defer(
								event.socket,
								event.instanceId,
								event.requestId,
								header.opcode,
								buffer.data,
								buffer.size
							);
							ServerWorker::eventQueue->insert( deferredEvent );

							// this->handleBatchChunksRequest( event, buffer.data, buffer.size );
						}
							break;
						default:
							__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from server." );
							break;
					}
					break;
				case PROTO_OPCODE_BATCH_KEY_VALUES:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleBatchKeyValueRequest( event, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleBatchKeyValueResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleBatchKeyValueResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from server." );
							break;
					}
					break;
				default:
					__ERROR__( "ServerWorker", "dispatch", "Invalid opcode from server. opcode = %x", header.opcode );
					goto quit_1;
			}
quit_1:
			buffer.data += header.length;
			buffer.size -= header.length;
		}
		if ( connected ) event.socket->done();
	}
	if ( ! connected ) {
		event.socket->print();
		__ERROR__( "ServerWorker", "dispatch", "The server is disconnected. Event type: %d.", event.type );
	}
}
