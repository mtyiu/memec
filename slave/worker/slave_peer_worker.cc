#include "worker.hh"
#include "../main/slave.hh"

void SlaveWorker::dispatch( SlavePeerEvent event ) {
	bool success, connected, isSend, isCompleted = true;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	isSend = ( event.type != SLAVE_PEER_EVENT_TYPE_PENDING );
	success = false;
	switch( event.type ) {
		//////////////
		// Requests //
		//////////////
		case SLAVE_PEER_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterSlavePeer(
				buffer.size,
				Slave::instanceId,
				SlaveWorker::idGenerator->nextVal( this->workerId ),
				SlaveWorker::slaveServerAddr
			);
			break;
		case SLAVE_PEER_EVENT_TYPE_GET_CHUNK_REQUEST:
			buffer.data = this->protocol.reqGetChunk(
				buffer.size,
				event.instanceId, event.requestId,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId
			);
			break;
		case SLAVE_PEER_EVENT_TYPE_SET_CHUNK_REQUEST:
			if ( event.message.chunk.chunk ) {
				uint32_t offset, size;
				char *data;

				data = event.message.chunk.chunk->getData( offset, size );
				// The chunk is sealed
				buffer.data = this->protocol.reqSetChunk(
					buffer.size,
					event.instanceId, event.requestId,
					event.message.chunk.metadata.listId,
					event.message.chunk.metadata.stripeId,
					event.message.chunk.metadata.chunkId,
					size, offset, data
				);

				if ( event.message.chunk.needsFree ) {
					SlaveWorker::chunkPool->free( event.message.chunk.chunk );
				}
			} else {
				DegradedMap &map = SlaveWorker::degradedChunkBuffer->map;
				buffer.data = this->protocol.reqSetChunk(
					buffer.size,
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
					SlavePeerEvent newEvent;
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
		case SLAVE_PEER_EVENT_TYPE_SET_REQUEST:
			buffer.data = this->protocol.reqSet(
				buffer.size,
				event.instanceId, event.requestId,
				event.message.parity.key.data,
				event.message.parity.key.size,
				event.message.parity.value.data,
				event.message.parity.value.size
			);
			break;
		case SLAVE_PEER_EVENT_TYPE_SET_RESPONSE_SUCCESS:
			success = true;
		case SLAVE_PEER_EVENT_TYPE_SET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resSet(
				buffer.size,
				event.instanceId, event.requestId,
				success, /* success */
				event.message.parity.key.size,
				event.message.parity.key.data,
				false /* to master */
			);
			break;
		case SLAVE_PEER_EVENT_TYPE_SEAL_CHUNK_REQUEST:
			this->issueSealChunkRequest( event.message.chunk.chunk );
			return;
		case SLAVE_PEER_EVENT_TYPE_GET_REQUEST:
			buffer.data = this->protocol.reqGet(
				buffer.size,
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
		case SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
			success = true; // default is false
		case SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRegisterSlavePeer(
				buffer.size,
				event.instanceId, event.requestId,
				success
			);
			break;
		case SLAVE_PEER_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS:
			__ERROR__( "SlaveWorker", "dispatch", "SLAVE_PEER_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS is not supported." );
			success = true; // default is false
			break;
		case SLAVE_PEER_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE:
			__ERROR__( "SlaveWorker", "dispatch", "SLAVE_PEER_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE is not supported." );
			// buffer.data = this->protocol.resRemappingSet(
			// 	buffer.size,
			// 	false, // toMaster
			// 	event.instanceId, event.requestId,
			// 	success,
			// 	event.message.remap.listId,
			// 	event.message.remap.chunkId,
			// 	event.message.remap.original,
			// 	event.message.remap.remapped,
			// 	event.message.remap.remappedCount,
			// 	event.message.remap.key.size,
			// 	event.message.remap.key.data
			// );
			break;
		// GET
		case SLAVE_PEER_EVENT_TYPE_GET_RESPONSE_SUCCESS:
		{
			char *key, *value;
			uint8_t keySize;
			uint32_t valueSize;
			event.message.get.keyValue.deserialize( key, keySize, value, valueSize );
			buffer.data = this->protocol.resGet(
				buffer.size,
				event.instanceId, event.requestId,
				true /* success */,
				false /* isDegraded */,
				keySize, key,
				valueSize, value,
				false /* toMaster */
			);
		}
			break;
		case SLAVE_PEER_EVENT_TYPE_GET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resGet(
				buffer.size,
				event.instanceId, event.requestId,
				false /* success */,
				false /* isDegraded */,
				event.message.get.key.size,
				event.message.get.key.data,
				0, 0,
				false /* toMaster */
			);
			break;
		// UPDATE_CHUNK
		case SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS:
			success = true; // default is false
		case SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE:
			buffer.data = this->protocol.resUpdateChunk(
				buffer.size,
				event.instanceId, event.requestId,
				success,
				event.message.chunkUpdate.metadata.listId,
				event.message.chunkUpdate.metadata.stripeId,
				event.message.chunkUpdate.metadata.chunkId,
				event.message.chunkUpdate.offset,
				event.message.chunkUpdate.length,
				event.message.chunkUpdate.updatingChunkId
			);
			break;
		// UPDATE
		case SLAVE_PEER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
			success = true; // default is false
		case SLAVE_PEER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
			buffer.data = this->protocol.resUpdate(
				buffer.size,
				event.instanceId, event.requestId,
				success,
				event.message.update.listId,
				event.message.update.stripeId,
				event.message.update.chunkId,
				event.message.update.key.data,
				event.message.update.key.size,
				event.message.update.valueUpdateOffset,
				event.message.update.length,
				event.message.update.chunkUpdateOffset
			);
			break;
		// DELETE
		case SLAVE_PEER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
			success = true; // default is false
		case SLAVE_PEER_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
			buffer.data = this->protocol.resDelete(
				buffer.size,
				event.instanceId, event.requestId,
				success,
				event.message.del.listId,
				event.message.del.stripeId,
				event.message.del.chunkId,
				event.message.del.key.data,
				event.message.del.key.size
			);
			break;
		// DELETE_CHUNK
		case SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS:
			success = true; // default is false
		case SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE:
			buffer.data = this->protocol.resDeleteChunk(
				buffer.size,
				event.instanceId, event.requestId,
				success,
				event.message.chunkUpdate.metadata.listId,
				event.message.chunkUpdate.metadata.stripeId,
				event.message.chunkUpdate.metadata.chunkId,
				event.message.chunkUpdate.offset,
				event.message.chunkUpdate.length,
				event.message.chunkUpdate.updatingChunkId
			);
			break;
		// GET_CHUNK
		case SLAVE_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_SUCCESS:
		{
			char *data = 0;
			uint32_t size = 0, offset = 0;

			if ( event.message.chunk.chunk )
				data = event.message.chunk.chunk->getData( offset, size );

			buffer.data = this->protocol.resGetChunk(
				buffer.size,
				event.instanceId, event.requestId,
				true,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId,
				size, offset, data
			);
		}
			break;
		case SLAVE_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_FAILURE:
			buffer.data = this->protocol.resGetChunk(
				buffer.size,
				event.instanceId, event.requestId,
				false,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId
			);
			break;
		// SET_CHUNK
		case SLAVE_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_SUCCESS:
			success = true; // default is false
		case SLAVE_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_FAILURE:
			buffer.data = this->protocol.resSetChunk(
				buffer.size,
				event.instanceId, event.requestId,
				success,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId
			);
			break;
		// SEAL_CHUNK
		case SLAVE_PEER_EVENT_TYPE_SEAL_CHUNK_RESPONSE_SUCCESS:
			success = true; // default is false
		case SLAVE_PEER_EVENT_TYPE_SEAL_CHUNK_RESPONSE_FAILURE:
			// TODO: Is a response message for SEAL_CHUNK request required?
			return;
		/////////////////////////////////////
		// Seal chunks in the chunk buffer //
		/////////////////////////////////////
		case SLAVE_PEER_EVENT_TYPE_SEAL_CHUNKS:
			printf( "\tSealing %lu chunks...\n", event.message.chunkBuffer->seal( this ) );
			return;
		//////////
		// Send //
		//////////
		case SLAVE_PEER_EVENT_TYPE_SEND:
			event.message.send.packet->read( buffer.data, buffer.size );
			break;
		///////////
		// Batch //
		///////////
		case SLAVE_PEER_EVENT_TYPE_BATCH_GET_CHUNKS:
		{
			uint16_t instanceId = Slave::instanceId;
			uint32_t offset = 0;
			size_t tmpSize, current, len;

			buffer.data = this->protocol.buffer.send;
			buffer.size = 0;
			std::vector<uint32_t> *requestIds = event.message.batchGetChunks.requestIds;
			std::vector<Metadata> *metadata = event.message.batchGetChunks.metadata;

			for ( current = 0, len = metadata->size(); current < len; current++ ) {
				if ( buffer.size + PROTO_CHUNK_SIZE > this->protocol.buffer.size )
					break;

				this->protocol.reqGetChunk(
					tmpSize,
					instanceId,
					requestIds->at( current ),
					metadata->at( current ).listId,
					metadata->at( current ).stripeId,
					metadata->at( current ).chunkId,
					buffer.data + offset
				);

				offset += tmpSize;
				buffer.size += tmpSize;
			}
			if ( current == len ) {
				// All sent
				delete requestIds;
				delete metadata;
			} else {
				requestIds->erase( requestIds->begin(), requestIds->begin() + current );
				metadata->erase( metadata->begin(), metadata->begin() + current );
				event.batchGetChunks( event.socket, requestIds, metadata );
				this->dispatch( event );
			}
		}
			break;
		/////////////
		// Pending //
		/////////////
		case SLAVE_PEER_EVENT_TYPE_PENDING:
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "SlaveWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );

		if ( event.type == SLAVE_PEER_EVENT_TYPE_SEND ) {
			SlaveWorker::packetPool->free( event.message.send.packet );
		}
	} else {
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "SlaveWorker (slave peer)" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_SLAVE ) {
				__ERROR__( "SlaveWorker", "dispatch", "Invalid protocol header." );
				goto quit_1;
			}
			event.instanceId = header.instanceId;
			event.requestId = header.requestId;
			event.timestamp = header.timestamp;
			switch ( header.opcode ) {
				case PROTO_OPCODE_REGISTER:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleSlavePeerRegisterRequest( event.socket, header.instanceId, header.requestId, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							event.socket->registered = true;
							event.socket->instanceId = header.instanceId;
							Slave::getInstance()->sockets.slavesIdToSocketMap.set( header.instanceId, event.socket, true );
							__DEBUG__( BLUE, "SlaveWorker", "dispatch", "Slave fd = %u id = %hu", event.socket->getSocket(), header.instanceId );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							__ERROR__( "SlaveWorker", "dispatch", "Failed to register with slave." );
							break;
						default:
							__ERROR__( "SlaveWorker", "dispatch", "Invalid magic code from slave: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_SEAL_CHUNK:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleSealChunkRequest( event, buffer.data, header.length );
							this->load.sealChunk();
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleSealChunkResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleSealChunkResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "SlaveWorker", "dispatch", "Invalid magic code from slave: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_REMAPPING_SET:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleRemappingSetRequest( event, buffer.data, header.length );
							this->load.set();
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleRemappingSetResponse( event, true, buffer.data, header.length );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleRemappingSetResponse( event, false, buffer.data, header.length );
							break;
						default:
							__ERROR__( "SlaveWorker", "dispatch", "Invalid magic code from slave: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_SET:
					switch ( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleSetRequest( event, buffer.data, buffer.size );
							this->load.set();
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleSetResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleSetResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "SlaveWorker", "dispatch", "Invalid magic code from slave: 0x%x for SET.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_GET:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleGetRequest( event, buffer.data, buffer.size );
							this->load.get();
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleGetResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleGetResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "SlaveWorker", "dispatch", "Invalid magic code from slave: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_UPDATE:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleUpdateRequest( event, buffer.data, buffer.size );
							this->load.update();
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleUpdateResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleUpdateResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "SlaveWorker", "dispatch", "Invalid magic code from slave: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_DELETE:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleDeleteRequest( event, buffer.data, buffer.size );
							this->load.del();
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleDeleteResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleDeleteResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "SlaveWorker", "dispatch", "Invalid magic code from slave: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_UPDATE_CHUNK:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleUpdateChunkRequest( event, buffer.data, buffer.size );
							this->load.updateChunk();
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleUpdateChunkResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleUpdateChunkResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "SlaveWorker", "dispatch", "Invalid magic code from slave: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_DELETE_CHUNK:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleDeleteChunkRequest( event, buffer.data, buffer.size );
							this->load.delChunk();
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleDeleteChunkResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleDeleteChunkResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "SlaveWorker", "dispatch", "Invalid magic code from slave: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_GET_CHUNK:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleGetChunkRequest( event, buffer.data, buffer.size );
							this->load.getChunk();
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleGetChunkResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleGetChunkResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "SlaveWorker", "dispatch", "Invalid magic code from slave: 0x%x.", header.magic );
							break;
					}
					break;
				case PROTO_OPCODE_SET_CHUNK:
				case PROTO_OPCODE_SET_CHUNK_UNSEALED:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleSetChunkRequest( event, header.opcode == PROTO_OPCODE_SET_CHUNK, buffer.data, buffer.size );
							this->load.setChunk();
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleSetChunkResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							this->handleSetChunkResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "SlaveWorker", "dispatch", "Invalid magic code from slave." );
							break;
					}
					break;
				default:
					__ERROR__( "SlaveWorker", "dispatch", "Invalid opcode from slave. opcode = %x", header.opcode );
					goto quit_1;
			}
quit_1:
			buffer.data += header.length;
			buffer.size -= header.length;
		}
		if ( connected ) event.socket->done();
	}
	if ( ! connected )
		__ERROR__( "SlaveWorker", "dispatch", "The slave is disconnected." );
}
