#include "worker.hh"
#include "../main/slave.hh"
#include "../../common/util/debug.hh"
#include "../../common/util/time.hh"

#define WORKER_COLOR	YELLOW

uint32_t SlaveWorker::dataChunkCount;
uint32_t SlaveWorker::parityChunkCount;
uint32_t SlaveWorker::chunkCount;
IDGenerator *SlaveWorker::idGenerator;
ArrayMap<int, SlavePeerSocket> *SlaveWorker::slavePeers;
Pending *SlaveWorker::pending;
ServerAddr *SlaveWorker::slaveServerAddr;
Coding *SlaveWorker::coding;
SlaveEventQueue *SlaveWorker::eventQueue;
StripeList<SlavePeerSocket> *SlaveWorker::stripeList;
std::vector<StripeListIndex> *SlaveWorker::stripeListIndex;
Map *SlaveWorker::map;
MemoryPool<Chunk> *SlaveWorker::chunkPool;
std::vector<MixedChunkBuffer *> *SlaveWorker::chunkBuffer;
DegradedChunkBuffer *SlaveWorker::degradedChunkBuffer;
PacketPool *SlaveWorker::packetPool;

void SlaveWorker::dispatch( MixedEvent event ) {
	switch( event.type ) {
		case EVENT_TYPE_CODING:
			this->dispatch( event.event.coding );
			break;
		case EVENT_TYPE_COORDINATOR:
			this->dispatch( event.event.coordinator );
			break;
		case EVENT_TYPE_IO:
			this->dispatch( event.event.io );
			break;
		case EVENT_TYPE_MASTER:
			this->dispatch( event.event.master );
			break;
		case EVENT_TYPE_SLAVE:
			this->dispatch( event.event.slave );
			break;
		case EVENT_TYPE_SLAVE_PEER:
			this->dispatch( event.event.slavePeer );
			break;
		default:
			break;
	}
}

void SlaveWorker::dispatch( CodingEvent event ) {
	switch( event.type ) {
		case CODING_EVENT_TYPE_DECODE:
			SlaveWorker::coding->decode( event.message.decode.chunks, event.message.decode.status );
			break;
		default:
			return;
	}
}

void SlaveWorker::dispatch( CoordinatorEvent event ) {
	bool connected, isSend;
	uint32_t requestId;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;
	std::unordered_map<Key, RemappingRecord>::iterator it, safeNextIt;

	if ( event.type == COORDINATOR_EVENT_TYPE_SYNC )
		// esp. in response to request from coordinator
		requestId = event.id;
	else if ( event.type == COORDINATOR_EVENT_TYPE_PENDING )
		requestId = 0;
	else
		requestId = SlaveWorker::idGenerator->nextVal( this->workerId );

	switch( event.type ) {
		case COORDINATOR_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterCoordinator(
				buffer.size,
				requestId,
				event.message.address.addr,
				event.message.address.port
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_SYNC:
		{
			uint32_t sealedCount, opsCount;
			bool isCompleted;

			buffer.data = this->protocol.sendHeartbeat(
				buffer.size,
				requestId,
				&SlaveWorker::map->sealedLock, SlaveWorker::map->sealed, sealedCount,
				&SlaveWorker::map->opsLock, SlaveWorker::map->ops, opsCount,
				isCompleted
			);

			if ( ! isCompleted )
				SlaveWorker::eventQueue->insert( event );

			isSend = true;
		}
			break;
		case COORDINATOR_EVENT_TYPE_REMAP_SYNC:
		{
			size_t remapCount;
			buffer.data = this->protocol.sendRemappingRecords(
				buffer.size,
				requestId,
				SlaveWorker::map->remap,
				&SlaveWorker::map->remapLock,
				remapCount
			);

			// move the remapping record sent to another set to avoid looping through all records over and over ..
			LOCK ( &SlaveWorker::map->remapLock );
			if ( remapCount == SlaveWorker::map->remap.size() ) {
				SlaveWorker::map->remapSent.insert(
					SlaveWorker::map->remap.begin(),
					SlaveWorker::map->remap.end()
				);
				SlaveWorker::map->remap.clear();
			} else {
				for ( it = SlaveWorker::map->remap.begin(); it != SlaveWorker::map->remap.end(); it = safeNextIt ) {
					safeNextIt = it;
					safeNextIt++;
					if ( it->second.sent ) {
						SlaveWorker::map->remapSent[ it->first ] = it->second;
						SlaveWorker::map->remap.erase( it );
					}
				}
			}
			UNLOCK ( &SlaveWorker::map->remapLock );
			isSend = true;

			if ( SlaveWorker::map->remap.size() )
				SlaveWorker::eventQueue->insert( event );
		}
			break;
		case COORDINATOR_EVENT_TYPE_RELEASE_DEGRADED_LOCK:
			buffer.data = this->protocol.resReleaseDegradedLock(
				buffer.size,
				event.id,
				event.message.degraded.count
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_PENDING:
			isSend = false;
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "SlaveWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else {
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "SlaveWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_COORDINATOR ) {
				__ERROR__( "SlaveWorker", "dispatch", "Invalid message source from coordinator." );
			} else {
				event.id = header.id;
				switch( header.opcode ) {
					case PROTO_OPCODE_REGISTER:
						switch( header.magic ) {
							case PROTO_MAGIC_RESPONSE_SUCCESS:
								event.socket->registered = true;
								break;
							case PROTO_MAGIC_RESPONSE_FAILURE:
								__ERROR__( "SlaveWorker", "dispatch", "Failed to register with coordinator." );
								break;
							default:
								__ERROR__( "SlaveWorker", "dispatch", "Invalid magic code from coordinator." );
								break;
						}
						break;
					case PROTO_OPCODE_SLAVE_CONNECTED:
						this->handleSlaveConnectedMsg( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_SLAVE_RECONSTRUCTED:
						this->handleSlaveReconstructedMsg( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_SEAL_CHUNKS:
						Slave::getInstance()->seal();
						break;
					case PROTO_OPCODE_FLUSH_CHUNKS:
						Slave::getInstance()->flush();
						break;
					case PROTO_OPCODE_SYNC_META:
						Slave::getInstance()->sync( header.id );
						break;
					case PROTO_OPCODE_RELEASE_DEGRADED_LOCKS:
						this->handleReleaseDegradedLockRequest( event, buffer.data, header.length );
						break;
					case PROTO_OPCODE_RECOVERY:
						this->handleRecoveryRequest( event, buffer.data, header.length );
						break;
					default:
						__ERROR__( "SlaveWorker", "dispatch", "Invalid opcode from coordinator." );
						break;
				}
			}

			buffer.data += header.length;
			buffer.size -= header.length;
		}
		if ( connected ) event.socket->done();
	}
	if ( ! connected )
		__ERROR__( "SlaveWorker", "dispatch", "The coordinator is disconnected." );
}

void SlaveWorker::dispatch( IOEvent event ) {
	switch( event.type ) {
		case IO_EVENT_TYPE_FLUSH_CHUNK:
			this->storage->write(
				event.message.chunk,
				false
			);
			break;
	}
}

void SlaveWorker::dispatch( MasterEvent event ) {
	bool success = true, connected, isSend = true;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	switch( event.type ) {
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_GET_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
			success = true;
			break;
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_GET_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_REDIRECT_RESPONSE:
			success = false;
			break;
		default:
			isSend = false;
			break;
	}

	switch( event.type ) {
		// Register
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRegisterMaster( buffer.size, event.id, success );
			break;
		// GET
		case MASTER_EVENT_TYPE_GET_RESPONSE_SUCCESS:
		{
			char *key, *value;
			uint8_t keySize;
			uint32_t valueSize;
			event.message.keyValue.deserialize( key, keySize, value, valueSize );
			if ( valueSize > 4096 ) {
				printf( "keySize = %u; valueSize = %u\n", keySize, valueSize );
				assert( valueSize <= 4096 );
			}
			buffer.data = this->protocol.resGet(
				buffer.size,
				event.id,
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
				event.id,
				success,
				event.isDegraded,
				event.message.key.size,
				event.message.key.data
			);
			break;
		// SET
		case MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resSet(
				buffer.size,
				event.id,
				success,
				event.message.key.size,
				event.message.key.data
			);
			break;
		case MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRemappingSet(
				buffer.size,
				true,
				event.id,
				success,
				event.message.remap.listId,
				event.message.remap.chunkId,
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
				event.id,
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
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
			buffer.data = this->protocol.resDelete(
				buffer.size,
				event.id,
				success,
				event.isDegraded,
				event.message.key.size,
				event.message.key.data
			);

			if ( event.needsFree )
				event.message.key.free();
			break;
		// Redirect
		case MASTER_EVENT_TYPE_REDIRECT_RESPONSE:
			buffer.data = this->protocol.resRedirect(
				buffer.size,
				event.id,
				event.message.remap.opcode,
				event.message.remap.key.size,
				event.message.remap.key.data,
				event.message.remap.listId,
				event.message.remap.chunkId
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
		// if ( event.type == MASTER_EVENT_TYPE_REDIRECT_RESPONSE )
		// 	fprintf( stderr, "redirect %u\n", event.id );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "SlaveWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );

		if ( ret > 0 )
			this->load.sentBytes( ret );
	} else {
		// Parse requests from masters
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		if ( ret > 0 )
			this->load.recvBytes( ret );
		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "SlaveWorker (master)" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.magic != PROTO_MAGIC_REQUEST || header.from != PROTO_MAGIC_FROM_MASTER ) {
				__ERROR__( "SlaveWorker", "dispatch", "Invalid protocol header." );
			} else {
				event.id = header.id;
				switch( header.opcode ) {
					case PROTO_OPCODE_GET:
						this->handleGetRequest( event, buffer.data, buffer.size );
						this->load.get();
						break;
					case PROTO_OPCODE_SET:
						this->handleSetRequest( event, buffer.data, buffer.size );
						this->load.set();
						break;
					case PROTO_OPCODE_REMAPPING_SET:
						this->handleRemappingSetRequest( event, buffer.data, buffer.size );
						this->load.set();
						break;
					case PROTO_OPCODE_UPDATE:
						this->handleUpdateRequest( event, buffer.data, buffer.size );
						this->load.update();
						break;
					case PROTO_OPCODE_DELETE:
						this->handleDeleteRequest( event, buffer.data, buffer.size );
						this->load.del();
						break;
					case PROTO_OPCODE_DEGRADED_GET:
						this->handleDegradedGetRequest( event, buffer.data, buffer.size );
						this->load.get();
						break;
					case PROTO_OPCODE_DEGRADED_UPDATE:
						this->handleDegradedUpdateRequest( event, buffer.data, buffer.size );
						this->load.update();
						break;
					case PROTO_OPCODE_DEGRADED_DELETE:
						this->handleDegradedDeleteRequest( event, buffer.data, buffer.size );
						this->load.del();
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

void SlaveWorker::dispatch( SlaveEvent event ) {
}

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
				SlaveWorker::idGenerator->nextVal( this->workerId ),
				SlaveWorker::slaveServerAddr
			);
			break;
		case SLAVE_PEER_EVENT_TYPE_GET_CHUNK_REQUEST:
			buffer.data = this->protocol.reqGetChunk(
				buffer.size,
				event.id,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId
			);
			break;
		case SLAVE_PEER_EVENT_TYPE_SET_CHUNK_REQUEST:
			if ( event.message.chunk.chunk ) {
				// The chunk is sealed
				buffer.data = this->protocol.reqSetChunk(
					buffer.size,
					event.id,
					event.message.chunk.metadata.listId,
					event.message.chunk.metadata.stripeId,
					event.message.chunk.metadata.chunkId,
					event.message.chunk.chunk->getSize(),
					event.message.chunk.chunk->getData()
				);

				if ( event.message.chunk.needsFree ) {
					SlaveWorker::chunkPool->free( event.message.chunk.chunk );
				}
			} else {
				DegradedMap &map = SlaveWorker::degradedChunkBuffer->map;
				buffer.data = this->protocol.reqSetChunk(
					buffer.size,
					event.id,
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
						event.id,
						event.message.chunk.metadata,
						0, // unsealed chunk
						false
					);
				}
			}
			break;
		case SLAVE_PEER_EVENT_TYPE_SEAL_CHUNK_REQUEST:
			this->issueSealChunkRequest( event.message.chunk.chunk );
			return;
		case SLAVE_PEER_EVENT_TYPE_GET_REQUEST:
			buffer.data = this->protocol.reqGet(
				buffer.size,
				event.id,
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
				event.id,
				success
			);
			break;
		case SLAVE_PEER_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS:
			success = true; // default is false
		case SLAVE_PEER_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRemappingSet(
				buffer.size,
				false, // toMaster
				event.id,
				success,
				event.message.remap.listId,
				event.message.remap.chunkId,
				event.message.remap.key.size,
				event.message.remap.key.data
			);
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
				event.id,
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
				event.id,
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
				event.id,
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
				event.id,
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
				event.id,
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
				event.id,
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
			char *chunkData = 0;
			uint32_t chunkSize = 0;

			if ( event.message.chunk.chunk ) {
				chunkData = event.message.chunk.chunk->getData();
				chunkSize = event.message.chunk.chunk->getSize();
			}

			buffer.data = this->protocol.resGetChunk(
				buffer.size,
				event.id,
				true,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId,
				chunkSize,
				chunkData
			);
		}
			break;
		case SLAVE_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_FAILURE:
			buffer.data = this->protocol.resGetChunk(
				buffer.size,
				event.id,
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
				event.id,
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
			event.id = header.id;
			switch ( header.opcode ) {
				case PROTO_OPCODE_REGISTER:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleSlavePeerRegisterRequest( event.socket, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							event.socket->registered = true;
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

SlavePeerSocket *SlaveWorker::getSlaves( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId ) {
	SlavePeerSocket *ret;
	listId = SlaveWorker::stripeList->get(
		data, ( size_t ) size,
		this->dataSlaveSockets,
		this->paritySlaveSockets,
		&chunkId, false
	);

	ret = *this->dataSlaveSockets;

	return ret->ready() ? ret : 0;
}

bool SlaveWorker::getSlaves( uint32_t listId ) {
	SlaveWorker::stripeList->get( listId, this->paritySlaveSockets, this->dataSlaveSockets );

	for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ )
		if ( ! this->paritySlaveSockets[ i ]->ready() )
			return false;
	for ( uint32_t i = 0; i < SlaveWorker::dataChunkCount; i++ )
		if ( ! this->dataSlaveSockets[ i ]->ready() )
			return false;
	return true;
}

bool SlaveWorker::issueSealChunkRequest( Chunk *chunk, uint32_t startPos ) {
	// The chunk is locked when this function is called
	// Only issue seal chunk request when new key-value pairs are received
	if ( SlaveWorker::parityChunkCount && startPos < chunk->getSize() ) {
		size_t size;
		uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
		Packet *packet = SlaveWorker::packetPool->malloc();
		packet->setReferenceCount( SlaveWorker::parityChunkCount );

		// Find parity slaves
		this->getSlaves( chunk->metadata.listId );

		// Prepare seal chunk request
		this->protocol.reqSealChunk( size, requestId, chunk, startPos, packet->data );
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

bool SlaveWorker::handleSlaveConnectedMsg( CoordinatorEvent event, char *buf, size_t size ) {
	struct AddressHeader header;
	if ( ! this->protocol.parseAddressHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleSlaveConnectedMsg", "Invalid address header." );
		return false;
	}

	char tmp[ 22 ];
	Socket::ntoh_ip( header.addr, tmp, 16 );
	Socket::ntoh_port( header.port, tmp + 16, 6 );
	__DEBUG__( YELLOW, "SlaveWorker", "handleSlaveConnectedMsg", "Slave: %s:%s is connected.", tmp, tmp + 16 );

	// Find the slave peer socket in the array map
	int index = -1;
	for ( int i = 0, len = slavePeers->size(); i < len; i++ ) {
		if ( slavePeers->values[ i ]->equal( header.addr, header.port ) ) {
			index = i;
			break;
		}
	}
	if ( index == -1 ) {
		__ERROR__( "SlaveWorker", "handleSlaveConnectedMsg", "The slave is not in the list. Ignoring this slave..." );
		return false;
	}

	// Update sockfd in the array Map
	int sockfd = slavePeers->values[ index ]->init();
	slavePeers->keys[ index ] = sockfd;

	// Connect to the slave peer
	slavePeers->values[ index ]->start();

	return true;
}

bool SlaveWorker::handleSlaveReconstructedMsg( CoordinatorEvent event, char *buf, size_t size ) {
	struct AddressHeader srcHeader, dstHeader;
	if ( ! this->protocol.parseSrcDstAddressHeader( srcHeader, dstHeader, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleSlaveReconstructedMsg", "Invalid address header." );
		return false;
	}

	char srcTmp[ 22 ], dstTmp[ 22 ];
	Socket::ntoh_ip( srcHeader.addr, srcTmp, 16 );
	Socket::ntoh_port( srcHeader.port, srcTmp + 16, 6 );
	Socket::ntoh_ip( dstHeader.addr, dstTmp, 16 );
	Socket::ntoh_port( dstHeader.port, dstTmp + 16, 6 );
	// __DEBUG__(
	// 	YELLOW,
	__ERROR__(
		"SlaveWorker", "handleSlaveReconstructedMsg",
		"Slave: %s:%s is reconstructed at %s:%s.",
		srcTmp, srcTmp + 16, dstTmp, dstTmp + 16
	);

	// Find the slave peer socket in the array map
	int index = -1, sockfd = -1;
	SlavePeerSocket *original, *s;
	bool self = false;

	for ( int i = 0, len = slavePeers->size(); i < len; i++ ) {
		if ( slavePeers->values[ i ]->equal( srcHeader.addr, srcHeader.port ) ) {
			index = i;
			original = slavePeers->values[ i ];
			break;
		}
	}
	if ( index == -1 ) {
		__ERROR__( "SlaveWorker", "handleSlaveReconstructedMsg", "The slave is not in the list. Ignoring this slave..." );
		return false;
	}
	original->stop();

	ServerAddr serverAddr( slavePeers->values[ index ]->identifier, dstHeader.addr, dstHeader.port );
	ServerAddr &me = Slave::getInstance()->config.slave.slave.addr;

	// Check if this is a self-socket
	self = ( dstHeader.addr == me.addr && dstHeader.port == me.port );

	s = new SlavePeerSocket();
	s->init(
		sockfd, serverAddr,
		&Slave::getInstance()->sockets.epoll,
		self // self-socket
	);

	// Update sockfd in the array Map
	if ( self ) {
		sockfd = original->getSocket();
	} else {
		sockfd = s->init();
	}
	slavePeers->set( index, sockfd, s );
	SlaveWorker::stripeList->update();
	delete original;

	// Connect to the slave peer
	if ( ! self )
		s->start();

	return true;
}

bool SlaveWorker::handleReleaseDegradedLockRequest( CoordinatorEvent event, char *buf, size_t size ) {
	struct ChunkHeader header;
	uint32_t count = 0, requestId;

	Metadata metadata;
	ChunkRequest chunkRequest;
	std::vector<Metadata> chunks;
	SlavePeerEvent slavePeerEvent;
	SlavePeerSocket *socket;
	Chunk *chunk;

	while( size ) {
		if ( ! this->protocol.parseDegradedReleaseReqHeader( header, buf, size ) ) {
			__ERROR__( "SlaveWorker", "handleReleaseDegradedLockRequest", "Invalid DEGRADED_RELEASE request." );
			return false;
		}
		__DEBUG__(
			BLUE, "SlaveWorker", "handleGetRequest",
			"[DEGRADED_RELEASE] (%u, %u, %u) (remaining = %lu).",
			header.listId, header.stripeId, header.chunkId
		);
		buf += PROTO_DEGRADED_RELEASE_REQ_SIZE;
		size -= PROTO_DEGRADED_RELEASE_REQ_SIZE;

		metadata.set( header.listId, header.stripeId, header.chunkId );
		chunks.push_back( metadata );

		count++;
	}

	SlaveWorker::pending->insertReleaseDegradedLock( event.id, event.socket, count );

	for ( size_t i = 0, len = chunks.size(); i < len; i++ ) {
		// Determine the src
		if ( i == 0 ) {
			// The target is the same for all chunks in this request
			this->getSlaves( chunks[ i ].listId );
			socket =   chunks[ i ].chunkId < SlaveWorker::dataChunkCount
			         ? this->dataSlaveSockets[ chunks[ i ].chunkId ]
			         : this->paritySlaveSockets[ chunks[ i ].chunkId - SlaveWorker::dataChunkCount ];
		}

		requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
		chunk = SlaveWorker::degradedChunkBuffer->map.deleteChunk(
			chunks[ i ].listId, chunks[ i ].stripeId, chunks[ i ].chunkId,
			&metadata
		);

		chunkRequest.set(
			chunks[ i ].listId, chunks[ i ].stripeId, chunks[ i ].chunkId,
			socket, chunk, true /* isDegraded */
		);
		if ( ! SlaveWorker::pending->insertChunkRequest( PT_SLAVE_PEER_SET_CHUNK, requestId, event.id, socket, chunkRequest ) ) {
			__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave CHUNK_REQUEST pending map." );
		}

		// If chunk is NULL, then the unsealed version of SET_CHUNK will be used
		slavePeerEvent.reqSetChunk( socket, requestId, metadata, chunk, true );
		SlaveWorker::eventQueue->insert( slavePeerEvent );
	}

	return true;
}

bool SlaveWorker::handleRecoveryRequest( CoordinatorEvent event, char *buf, size_t size ) {
	struct RecoveryHeader header;
	std::unordered_set<uint32_t> stripeIds;
	std::unordered_set<uint32_t>::iterator it;
	if ( ! this->protocol.parseRecoveryHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleRecoveryRequest", "Invalid RECOVERY request." );
		return false;
	}
	// __DEBUG__(
	// 	BLUE,
	__ERROR__(
		"SlaveWorker", "handleRecoveryRequest",
		"[RECOVERY] List ID: %u; chunk ID: %u; number of stripes: %u.",
		header.listId, header.chunkId, header.numStripes
	);

	uint32_t chunkId = 0, myChunkId = SlaveWorker::chunkCount, chunkCount, requestId;
	ChunkRequest chunkRequest;
	Metadata metadata;
	SlavePeerEvent *events;
	Chunk *chunk;
	SlavePeerSocket *socket = 0;

	// Check whether the number of surviving nodes >= k
	SlaveWorker::stripeList->get( header.listId, this->paritySlaveSockets, this->dataSlaveSockets );
	chunkCount = 0;
	for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
		if ( i == header.chunkId )
			continue;
		socket = ( i < SlaveWorker::dataChunkCount ) ?
				 ( this->dataSlaveSockets[ i ] ) :
				 ( this->paritySlaveSockets[ i - SlaveWorker::dataChunkCount ] );
		if ( socket->ready() ) chunkCount++;
		if ( socket->self ) myChunkId = i;
	}
	assert( myChunkId < SlaveWorker::chunkCount );
	if ( chunkCount < SlaveWorker::dataChunkCount ) {
		__ERROR__( "SlaveWorker", "handleRecoveryRequest", "The number of surviving nodes is less than k. The data cannot be recovered." );
		// TODO
		return false;
	}

	// Insert into pending set
	for ( uint32_t i = 0; i < header.numStripes; i++ )
		stripeIds.insert( header.stripeIds[ i ] );
	if ( ! SlaveWorker::pending->insertRecovery(
		event.id,
		header.chunkId < SlaveWorker::dataChunkCount ? this->dataSlaveSockets[ header.chunkId ] : this->paritySlaveSockets[ header.chunkId - SlaveWorker::dataChunkCount ],
		header.listId,
		header.chunkId,
		stripeIds
	) ) {
		__ERROR__( "SlaveWorker", "handleRecoveryRequest", "Cannot insert into coordinator RECOVERY pending map." );
	}

	// Send GET_CHUNK requests to surviving nodes
	events = new SlavePeerEvent[ SlaveWorker::dataChunkCount - 1 ];
	for ( it = stripeIds.begin(); it != stripeIds.end(); it++ ) {
		// printf( "Processing (%u, %u, %u)...\n", header.listId, *it, header.chunkId );
		chunkCount = 0;
		requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
		metadata.listId = header.listId;
		metadata.stripeId = *it;
		while( chunkCount < SlaveWorker::dataChunkCount - 1 ) {
			if ( chunkId != header.chunkId ) { // skip the chunk to be reconstructed
				socket = ( chunkId < SlaveWorker::dataChunkCount ) ?
						 ( this->dataSlaveSockets[ chunkId ] ) :
						 ( this->paritySlaveSockets[ chunkId - SlaveWorker::dataChunkCount ] );
				if ( socket->ready() && ! socket->self ) { // use this slave
					metadata.chunkId = chunkId;
					chunkRequest.set(
						metadata.listId, metadata.stripeId, metadata.chunkId,
						socket, 0, false
					);
					if ( ! SlaveWorker::pending->insertChunkRequest( PT_SLAVE_PEER_GET_CHUNK, requestId, event.id, socket, chunkRequest ) ) {
						__ERROR__( "SlaveWorker", "handleRecoveryRequest", "Cannot insert into slave CHUNK_REQUEST pending map." );
					} else {
						events[ chunkCount ].reqGetChunk( socket, requestId, metadata );
					}
					chunkCount++;
				}
			}
			chunkId = ( chunkId + 1 ) % SlaveWorker::chunkCount;
		}
		// Use own chunk
		chunk = SlaveWorker::map->findChunkById( metadata.listId, metadata.stripeId, myChunkId );
		if ( ! chunk ) {
			chunk = Coding::zeros;
		} else {
			// Check whether the chunk is sealed or not
			MixedChunkBuffer *chunkBuffer = SlaveWorker::chunkBuffer->at( metadata.listId );
			int chunkBufferIndex = chunkBuffer->lockChunk( chunk, true );
			bool isSealed = ( chunkBufferIndex == -1 );
			if ( ! isSealed )
				chunk = Coding::zeros;
			chunkBuffer->unlock( chunkBufferIndex );
		}
		chunkRequest.set(
			metadata.listId, metadata.stripeId, myChunkId,
			socket, chunk, false
		);
		if ( ! SlaveWorker::pending->insertChunkRequest( PT_SLAVE_PEER_GET_CHUNK, requestId, event.id, 0, chunkRequest ) ) {
			__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave CHUNK_REQUEST pending map." );
		}
		// Send GET_CHUNK requests now
		for ( uint32_t i = 0; i < SlaveWorker::dataChunkCount - 1; i++ ) {
			SlaveWorker::eventQueue->insert( events[ i ] );
		}
	}
	delete[] events;

	return false;
}

bool SlaveWorker::handleGetRequest( MasterEvent event, char *buf, size_t size ) {
	struct KeyHeader header;
	bool ret;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleGetRequest", "Invalid GET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleGetRequest",
		"[GET] Key: %.*s (key size = %u).",
		( int ) header.keySize, header.key, header.keySize
	);

	Key key;
	KeyValue keyValue;
	RemappingRecord remappingRecord;
	if ( map->findValueByKey( header.key, header.keySize, &keyValue, &key ) ) {
		event.resGet( event.socket, event.id, keyValue, false );
		ret = true;
	} else if ( map->findRemappingRecordByKey( header.key, header.keySize, &remappingRecord, &key ) ) {
		// Redirect request to remapped slave
		event.resRedirect( event.socket, event.id, PROTO_OPCODE_GET, key, remappingRecord );
		ret = false;
	} else {
		event.resGet( event.socket, event.id, key, false );
		ret = false;
	}
	this->dispatch( event );
	return ret;
}

bool SlaveWorker::handleSetRequest( MasterEvent event, char *buf, size_t size ) {
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

	// Detect degraded SET
	uint32_t listId, chunkId;
	listId = SlaveWorker::stripeList->get( header.key, header.keySize, 0, 0, &chunkId );

	SlaveWorker::chunkBuffer->at( listId )->set(
		this,
		header.key, header.keySize,
		header.value, header.valueSize,
		PROTO_OPCODE_SET, chunkId,
		this->chunks, this->dataChunk, this->parityChunk
	);

	Key key;
	key.set( header.keySize, header.key );
	event.resSet( event.socket, event.id, key, true );
	this->dispatch( event );

	return true;
}

bool SlaveWorker::handleRemappingSetRequest( MasterEvent event, char *buf, size_t size ) {
	struct RemappingSetHeader header;
	if ( ! this->protocol.parseRemappingSetHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleRemappingSetRequest", "Invalid REMAPPING_SET request (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleRemappingSetRequest",
		"[SET] Key: %.*s (key size = %u); Value: (value size = %u); list ID = %u, chunk ID = %u; needs forwarding? %s (ID: %u)",
		( int ) header.keySize, header.key, header.keySize, header.valueSize,
		header.listId, header.chunkId, header.needsForwarding ? "true" : "false",
		event.id
	);

	SlaveWorker::chunkBuffer->at( header.listId )->set(
		this,
		header.key, header.keySize,
		header.value, header.valueSize,
		PROTO_OPCODE_REMAPPING_SET, header.chunkId,
		this->chunks, this->dataChunk, this->parityChunk
	);

	if ( header.needsForwarding && SlaveWorker::parityChunkCount > 0 ) {
		uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );

		// Insert into master REMAPPING_SET pending set
		struct RemappingRecordKey record;
		record.remap.set( header.listId, header.chunkId );
		record.key.dup( header.keySize, header.key );

		if ( ! SlaveWorker::pending->insertRemappingRecordKey( PT_MASTER_REMAPPING_SET, event.id, ( void * ) event.socket, record ) ) {
			__ERROR__( "SlaveWorker", "handleRemappingSetRequest", "Cannot insert into master REMAPPING_SET pending map (ID: %u).", event.id );
			return false;
		}

		// Get parity slaves
		if ( ! this->getSlaves( header.listId ) ) {
			__ERROR__( "SlaveWorker", "handleRemappingSetRequest", "Cannot find a slave for performing degraded operation." );
		}

		// Prepare a packet buffer
		Packet *packet = SlaveWorker::packetPool->malloc();
		packet->setReferenceCount( SlaveWorker::parityChunkCount );

		size_t size;
		this->protocol.reqRemappingSet(
			size, requestId,
			header.listId, header.chunkId, false,
			header.key, header.keySize,
			header.value, header.valueSize,
			packet->data
		);
		packet->size = ( uint32_t ) size;

		// Forward the whole packet to the parity slaves
		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			// Insert into slave SET pending set
			if ( ! SlaveWorker::pending->insertRemappingRecordKey( PT_SLAVE_PEER_REMAPPING_SET, requestId, event.id, this->paritySlaveSockets[ i ], record ) ) {
				__ERROR__( "SlaveWorker", "handleRemappingSetRequest", "Cannot insert into slave SET pending map (ID: %u).", event.id );
			}
		}

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
		Key key;
		key.set( header.keySize, header.key );
		event.resRemappingSet( event.socket, event.id, key, header.listId, header.chunkId, true, false );
		this->dispatch( event );
	}

	return true;
}

bool SlaveWorker::handleRemappingSetRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct RemappingSetHeader header;
	if ( ! this->protocol.parseRemappingSetHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleRemappingSetRequest", "Invalid REMAPPING_SET request (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleRemappingSetRequest",
		"[SET] Key: %.*s (key size = %u); Value: (value size = %u); list ID = %u, chunk ID = %u; needs forwarding? %s",
		( int ) header.keySize, header.key, header.keySize, header.valueSize,
		header.listId, header.chunkId, header.needsForwarding ? "true" : "false"
	);

	SlaveWorker::chunkBuffer->at( header.listId )->set(
		this,
		header.key, header.keySize,
		header.value, header.valueSize,
		PROTO_OPCODE_REMAPPING_SET,
		SlaveWorker::chunkBuffer->at( header.listId )->getChunkId(),
		this->chunks, this->dataChunk, this->parityChunk
	);

	Key key;
	key.set( header.keySize, header.key );
	event.resRemappingSet( event.socket, event.id, key, header.listId, header.chunkId, true );
	this->dispatch( event );

	return true;
}

bool SlaveWorker::handleRemappingSetResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	struct RemappingLockHeader header;
	if ( ! this->protocol.parseRemappingLockHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleRemappingSetResponse", "Invalid REMAPPING_SET response (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleRemappingSetResponse",
		"[REMAPPING_SET] Key: %.*s (key size = %u); list ID: %u, chunk ID: %u",
		( int ) header.keySize, header.key, header.keySize, header.listId, header.chunkId
	);

	int pending;
	PendingIdentifier pid;
	RemappingRecordKey record;

	if ( ! SlaveWorker::pending->eraseRemappingRecordKey( PT_SLAVE_PEER_REMAPPING_SET, event.id, event.socket, &pid, &record, true, false ) ) {
		UNLOCK( &SlaveWorker::pending->slavePeers.remappingSetLock );
		__ERROR__( "SlaveWorker", "handleRemappingSetResponse", "Cannot find a pending slave REMAPPING_SET request that matches the response. This message will be discarded. (ID: %u)", event.id );
		return false;
	}
	// Check pending slave UPDATE requests
	pending = SlaveWorker::pending->count( PT_SLAVE_PEER_REMAPPING_SET, pid.id, false, true );

	__DEBUG__( BLUE, "SlaveWorker", "handleRemappingSetResponse", "Pending slave REMAPPING_SET requests = %d (%s).", pending, success ? "success" : "fail" );
	if ( pending == 0 ) {
		// Only send master REMAPPING_SET response when the number of pending slave REMAPPING_SET requests equal 0
		MasterEvent masterEvent;

		if ( ! SlaveWorker::pending->eraseRemappingRecordKey( PT_MASTER_REMAPPING_SET, pid.parentId, 0, &pid, &record ) ) {
			__ERROR__( "SlaveWorker", "handleRemappingSetResponse", "Cannot find a pending master REMAPPING_SET request that matches the response. This message will be discarded." );
			return false;
		}

		masterEvent.resRemappingSet(
			( MasterSocket * ) pid.ptr, pid.id, record.key,
			record.remap.listId, record.remap.chunkId, success, true
		);
		SlaveWorker::eventQueue->insert( masterEvent );
	}

	return true;
}

bool SlaveWorker::handleUpdateRequest( MasterEvent event, char *buf, size_t size ) {
	struct KeyValueUpdateHeader header;
	bool ret;
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

	Key key;
	KeyValue keyValue;
	KeyValueUpdate keyValueUpdate;
	KeyMetadata keyMetadata;
	Metadata metadata;
	Chunk *chunk;
	RemappingRecord remappingRecord;
	if ( map->findValueByKey( header.key, header.keySize, &keyValue, &key, &keyMetadata, &metadata, &chunk ) ) {
		uint32_t offset = keyMetadata.offset + PROTO_KEY_VALUE_SIZE + header.keySize + header.valueUpdateOffset;

		if ( SlaveWorker::parityChunkCount ) {
			// Add the current request to the pending set
			keyValueUpdate.dup( header.keySize, header.key, ( void * ) event.socket );
			keyValueUpdate.offset = header.valueUpdateOffset;
			keyValueUpdate.length = header.valueUpdateSize;

			if ( ! SlaveWorker::pending->insertKeyValueUpdate( PT_MASTER_UPDATE, event.id, ( void * ) event.socket, keyValueUpdate ) ) {
				__ERROR__( "SlaveWorker", "handleUpdateRequest", "Cannot insert into master UPDATE pending map (ID = %u).", event.id );
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
				event.id, keyValueUpdate.size, keyValueUpdate.data,
				metadata, offset,
				header.valueUpdateSize,    /* deltaSize */
				header.valueUpdateOffset,
				header.valueUpdate,        /* delta */
				chunkBufferIndex == -1,    /* isSealed */
				true                      /* isUpdate */
			);
		} else {
			event.resUpdate(
				event.socket, event.id, key,
				header.valueUpdateOffset,
				header.valueUpdateSize,
				true, false, false
			);
			this->dispatch( event );
			ret = true;
		}
		if ( chunkBufferIndex != -1 )
			chunkBuffer->updateAndUnlockChunk( chunkBufferIndex );
	} else if ( map->findRemappingRecordByKey( header.key, header.keySize, &remappingRecord, &key ) ) {
		// Redirect request to remapped slave
		event.resRedirect( event.socket, event.id, PROTO_OPCODE_UPDATE, key, remappingRecord );
		ret = false;
		this->dispatch( event );
	} else {
		event.resUpdate(
			event.socket, event.id, key,
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
	bool ret;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleDeleteRequest", "Invalid DELETE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleDeleteRequest",
		"[DELETE] Key: %.*s (key size = %u).",
		( int ) header.keySize, header.key, header.keySize
	);

	Key key;
	KeyValue keyValue;
	KeyMetadata keyMetadata;
	Metadata metadata;
	Chunk *chunk;
	RemappingRecord remappingRecord;
	if ( map->findValueByKey( header.key, header.keySize, &keyValue, 0, &keyMetadata, &metadata, &chunk ) ) {
		uint32_t deltaSize = 0;
		char *delta = 0;

		// Add the current request to the pending set
		if ( SlaveWorker::parityChunkCount ) {
			key.dup( header.keySize, header.key, ( void * ) event.socket );
			if ( ! SlaveWorker::pending->insertKey( PT_MASTER_DEL, event.id, ( void * ) event.socket, key ) ) {
				__ERROR__( "SlaveWorker", "handleDeleteRequest", "Cannot insert into master DELETE pending map." );
			}
			delta = this->buffer.data;
		} else {
			key.set( header.keySize, header.key, ( void * ) event.socket );
			delta = 0;
		}

		// Update data chunk and map
		key.ptr = 0;
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
				if ( chunk->lastDelPos < chunk->getSize() ) {
					// Only issue seal chunk request when new key-value pairs are received
					this->issueSealChunkRequest( chunk, chunk->lastDelPos );
				}
			}
		}
		SlaveWorker::map->deleteKey( key, PROTO_OPCODE_DELETE, keyMetadata, false, false );
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
				event.id, key.size, key.data,
				metadata, keyMetadata.offset, deltaSize, 0, delta,
				chunkBufferIndex == -1 /* isSealed */,
				false /* isUpdate */
			);
		} else {
			event.resDelete( event.socket, event.id, key, true, false, false );
			this->dispatch( event );
			ret = true;
		}
		if ( chunkBufferIndex != -1 )
			chunkBuffer->updateAndUnlockChunk( chunkBufferIndex );
	} else if ( map->findRemappingRecordByKey( header.key, header.keySize, &remappingRecord, &key ) ) {
		// Redirect request to remapped slave
		event.resRedirect( event.socket, event.id, PROTO_OPCODE_DELETE, key, remappingRecord );
		ret = false;
		this->dispatch( event );
	} else {
		key.set( header.keySize, header.key, ( void * ) event.socket );
		event.resDelete( event.socket, event.id, key, false, false, false );
		this->dispatch( event );
		ret = false;
	}

	return ret;
}

bool SlaveWorker::handleDegradedGetRequest( MasterEvent event, char *buf, size_t size ) {
	struct DegradedReqHeader header;
	if ( ! this->protocol.parseDegradedReqHeader( header, PROTO_OPCODE_DEGRADED_GET, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleDegradedRequest", "Invalid degraded GET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleDegradedGetRequest",
		"[GET (%u, %u, %u)] Key: %.*s (key size = %u).",
		header.listId, header.stripeId, header.chunkId,
		( int ) header.data.key.keySize,
		header.data.key.key,
		header.data.key.keySize
	);

	Key key;
	KeyValue keyValue;
	KeyMetadata keyMetadata;
	bool ret = true;
	DegradedMap *dmap = &SlaveWorker::degradedChunkBuffer->map;
	 // Check if the chunk is already fetched
	Chunk *chunk = dmap->findChunkById(
		header.listId, header.stripeId, header.chunkId
	);
	// Check if the key exists or is in a unsealed chunk
	bool isSealed;
	bool isKeyValueFound = dmap->findValueByKey(
		header.data.key.key,
		header.data.key.keySize,
		isSealed,
		&keyValue, &key, &keyMetadata
	);

	if ( isKeyValueFound ) {
		// Send the key-value pair to the master
		event.resGet( event.socket, event.id, keyValue, true /* isDegraded */ );
		this->dispatch( event );
	} else if ( chunk ) {
		// Key not found
		event.resGet( event.socket, event.id, key, true /* isDegraded */ );
		this->dispatch( event );
	} else {
		key.dup();
		ret = this->performDegradedRead(
			event.socket, header.listId, header.stripeId, header.chunkId,
			header.isSealed, PROTO_OPCODE_DEGRADED_GET, event.id, &key
		);

		if ( ! ret ) {
			__ERROR__( "SlaveWorker", "handleDegradedGetRequest", "Failed to perform degraded read on (%u, %u).", header.listId, header.stripeId );
		}
	}

	return ret;
}

bool SlaveWorker::handleDegradedUpdateRequest( MasterEvent event, char *buf, size_t size ) {
	struct DegradedReqHeader header;
	if ( ! this->protocol.parseDegradedReqHeader( header, PROTO_OPCODE_DEGRADED_UPDATE, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleDegradedUpdateRequest", "Invalid degraded UPDATE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleDegradedRequest",
		"[UPDATE (%u, %u, %u)] Key: %.*s (key size = %u); Value: (update size = %u, offset = %u).",
		header.listId, header.stripeId, header.chunkId,
		( int ) header.data.keyValueUpdate.keySize,
		header.data.keyValueUpdate.key,
		header.data.keyValueUpdate.keySize,
		header.data.keyValueUpdate.valueUpdateSize,
		header.data.keyValueUpdate.valueUpdateOffset
	);

	Key key;
	KeyValue keyValue;
	KeyValueUpdate keyValueUpdate;
	KeyMetadata keyMetadata;
	Metadata metadata;
	bool ret = true;
	DegradedMap *dmap = &SlaveWorker::degradedChunkBuffer->map;

	keyMetadata.offset = 0;

	// Check if the chunk is already fetched
	Chunk *chunk = dmap->findChunkById(
		header.listId, header.stripeId, header.chunkId
	);
	// Check if the key exists or is in a unsealed chunk
	bool isSealed;
	bool isKeyValueFound = dmap->findValueByKey(
		header.data.keyValueUpdate.key,
		header.data.keyValueUpdate.keySize,
		isSealed,
		&keyValue, &key, &keyMetadata
	);
	// Set up KeyValueUpdate
	keyValueUpdate.set( key.size, key.data, ( void * ) event.socket );
	keyValueUpdate.offset = header.data.keyValueUpdate.valueUpdateOffset;
	keyValueUpdate.length = header.data.keyValueUpdate.valueUpdateSize;
	// Set up metadata
	metadata.set( header.listId, header.stripeId, header.chunkId );

	if ( isKeyValueFound ) {
		keyValueUpdate.dup( 0, 0, ( void * ) event.socket );
		// Insert into master UPDATE pending set
		if ( ! SlaveWorker::pending->insertKeyValueUpdate( PT_MASTER_UPDATE, event.id, ( void * ) event.socket, keyValueUpdate ) ) {
			__ERROR__( "SlaveWorker", "handleDegradedRequest", "Cannot insert into master UPDATE pending map." );
		}

		char *valueUpdate = header.data.keyValueUpdate.valueUpdate;

		if ( chunk ) {
			// Send UPDATE_CHUNK request to the parity slaves
			uint32_t chunkUpdateOffset = KeyValue::getChunkUpdateOffset(
				keyMetadata.offset, // chunkOffset
				keyValueUpdate.size, // keySize
				keyValueUpdate.offset // valueUpdateOffset
			);

			SlaveWorker::degradedChunkBuffer->updateKeyValue(
				keyValueUpdate.size,
				keyValueUpdate.data,
				keyValueUpdate.length,
				keyValueUpdate.offset,
				chunkUpdateOffset,
				valueUpdate,
				chunk,
				true /* isSealed */
			);

			this->sendModifyChunkRequest(
				event.id,
				keyValueUpdate.size,
				keyValueUpdate.data,
				metadata,
				chunkUpdateOffset,
				keyValueUpdate.length /* deltaSize */,
				keyValueUpdate.offset,
				valueUpdate,
				true /* isSealed */,
				true /* isUpdate */
			);
		} else {
			// Send UPDATE request to the parity slaves
			uint32_t dataUpdateOffset = KeyValue::getChunkUpdateOffset(
				0,                            // chunkOffset
				keyValueUpdate.size,  // keySize
				keyValueUpdate.offset // valueUpdateOffset
			);

			// Compute data delta
			Coding::bitwiseXOR(
				valueUpdate,
				keyValue.data + dataUpdateOffset, // original data
				valueUpdate,                      // new data
				keyValueUpdate.length
			);
			// Perform actual data update
			Coding::bitwiseXOR(
				keyValue.data + dataUpdateOffset,
				keyValue.data + dataUpdateOffset, // original data
				valueUpdate,                      // new data
				keyValueUpdate.length
			);

			// Send UPDATE request to the parity slaves
			this->sendModifyChunkRequest(
				event.id,
				keyValueUpdate.size,
				keyValueUpdate.data,
				metadata,
				0, /* chunkUpdateOffset */
				keyValueUpdate.length, /* deltaSize */
				keyValueUpdate.offset,
				valueUpdate,
				false /* isSealed */,
				true /* isUpdate */
			);
		}
	} else if ( chunk ) {
		// Key not found
		event.resUpdate(
			event.socket, event.id, key,
			header.data.keyValueUpdate.valueUpdateOffset,
			header.data.keyValueUpdate.valueUpdateSize,
			false, /* success */
			false, /* needsFree */
			true   /* isDegraded */
		);
		this->dispatch( event );
	} else {
		key.dup();
		keyValueUpdate.dup( 0, 0, ( void * ) event.socket );

		// Backup valueUpdate
		char *valueUpdate = new char[ keyValueUpdate.length ];
		memcpy( valueUpdate, header.data.keyValueUpdate.valueUpdate, keyValueUpdate.length );
		keyValueUpdate.ptr = valueUpdate;

		ret = this->performDegradedRead(
			event.socket,
			header.listId, header.stripeId, header.chunkId,
			header.isSealed, PROTO_OPCODE_DEGRADED_UPDATE,
			event.id, &key, &keyValueUpdate
		);

		if ( ! ret ) {
			__ERROR__( "SlaveWorker", "handleDegradedUpdateRequest", "Failed to perform degraded read on (%u, %u).", header.listId, header.stripeId );
		}
	}

	return ret;
}

bool SlaveWorker::handleDegradedDeleteRequest( MasterEvent event, char *buf, size_t size ) {
	struct DegradedReqHeader header;
	if ( ! this->protocol.parseDegradedReqHeader( header, PROTO_OPCODE_DEGRADED_DELETE, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleDegradedDeleteRequest", "Invalid degraded DELETE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleDegradedDeleteRequest",
		"[DELETE (%u, %u, %u)] Key: %.*s (key size = %u).",
		header.listId, header.stripeId, header.chunkId,
		( int ) header.data.key.keySize,
		header.data.key.key,
		header.data.key.keySize
	);

	Key key;
	KeyValue keyValue;
	KeyMetadata keyMetadata;
	Metadata metadata;
	bool ret = true;
	DegradedMap *dmap = &SlaveWorker::degradedChunkBuffer->map;

	keyMetadata.offset = 0;

	// Check if the chunk is already fetched
	Chunk *chunk = dmap->findChunkById(
		header.listId, header.stripeId, header.chunkId
	);
	// Check if the key exists or is in a unsealed chunk
	bool isSealed;
	bool isKeyValueFound = dmap->findValueByKey(
		header.data.key.key,
		header.data.key.keySize,
		isSealed,
		&keyValue, &key, &keyMetadata
	);
	// Set up metadata
	metadata.set( header.listId, header.stripeId, header.chunkId );

	if ( isKeyValueFound ) {
		key.dup( 0, 0, ( void * ) event.socket );
		if ( ! SlaveWorker::pending->insertKey( PT_MASTER_DEL, event.id, ( void * ) event.socket, key ) ) {
			__ERROR__( "SlaveWorker", "handleDegradedDeleteRequest", "Cannot insert into master DELETE pending map." );
		}

		uint32_t deltaSize = this->buffer.size;
		char *delta = this->buffer.data;

		if ( chunk ) {
			SlaveWorker::degradedChunkBuffer->deleteKey(
				PROTO_OPCODE_DELETE,
				key.size, key.data,
				metadata,
				true, /* isSealed */
				deltaSize, delta, chunk
			);

			// Send DELETE_CHUNK request to the parity slaves
			this->sendModifyChunkRequest(
				event.id,
				key.size,
				key.data,
				metadata,
				keyMetadata.offset,
				deltaSize,
				0,   /* valueUpdateOffset */
				delta,
				true /* isSealed */,
				false /* isUpdate */
			);
		} else {
			uint32_t tmp = 0;
			SlaveWorker::degradedChunkBuffer->deleteKey(
				PROTO_OPCODE_DELETE,
				key.size, key.data,
				metadata,
				false,
				tmp, 0, 0
			);

			// Send DELETE request to the parity slaves
			this->sendModifyChunkRequest(
				event.id,
				key.size,
				key.data,
				metadata,
				// not needed for deleting a key-value pair in an unsealed chunk:
				0, 0, 0, 0,
				false /* isSealed */,
				false /* isUpdate */
			);
		}
	} else if ( chunk ) {
		// Key not found
		event.resDelete(
			event.socket, event.id, key,
			false, /* success */
			false, /* needsFree */
			true   /* isDegraded */
		);
		this->dispatch( event );
	} else {
		key.dup();
		ret = this->performDegradedRead(
			event.socket, header.listId, header.stripeId, header.chunkId,
			header.isSealed, PROTO_OPCODE_DEGRADED_DELETE, event.id, &key
		);

		if ( ! ret ) {
			__ERROR__( "SlaveWorker", "handleDegradedDeleteRequest", "Failed to perform degraded read on (%u, %u).", header.listId, header.stripeId );
		}
	}

	return ret;
}

bool SlaveWorker::handleSlavePeerRegisterRequest( SlavePeerSocket *socket, char *buf, size_t size ) {
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
	if ( index == -1 ) {
		__ERROR__( "SlaveWorker", "handleSlavePeerRegisterRequest", "The slave is not in the list. Ignoring this slave..." );
		return false;
	}

	SlavePeerEvent event;
	event.resRegister( slavePeers->values[ index ], true );
	SlaveWorker::eventQueue->insert( event );
	return true;
}

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

	if ( ! ret )
		__ERROR__( "SlaveWorker", "handleSealChunkRequest", "Cannot update parity chunk." );

	return true;
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
		event.resGet( event.socket, event.id, keyValue );
	else
		event.resGet( event.socket, event.id, key );
	this->dispatch( event );
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

	SlaveWorker::chunkBuffer->at( header.listId )->update(
		header.stripeId, header.chunkId,
		header.offset, header.length, header.delta,
		this->chunks, this->dataChunk, this->parityChunk
	);
	ret = true;

	Metadata metadata;
	metadata.set( header.listId, header.stripeId, header.chunkId );

	event.resUpdateChunk(
		event.socket, event.id, metadata,
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

	SlaveWorker::chunkBuffer->at( header.listId )->update(
		header.stripeId, header.chunkId,
		header.offset, header.length, header.delta,
		this->chunks, this->dataChunk, this->parityChunk,
		true /* isDelete */
	);
	ret = true;

	Metadata metadata;
	metadata.set( header.listId, header.stripeId, header.chunkId );

	event.resDeleteChunk(
		event.socket, event.id, metadata,
		header.offset, header.length,
		header.updatingChunkId, ret
	);

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
	bool ret = SlaveWorker::chunkBuffer->at( header.listId )->updateKeyValue(
		header.key, header.keySize,
		header.valueUpdateOffset, header.valueUpdateSize, header.valueUpdate
	);
	if ( ! ret ) {
		// Use the chunkUpdateOffset
		SlaveWorker::chunkBuffer->at( header.listId )->update(
			header.stripeId, header.chunkId,
			header.chunkUpdateOffset, header.valueUpdateSize, header.valueUpdate,
			this->chunks, this->dataChunk, this->parityChunk
		);
		ret = true;
	}

	key.set( header.keySize, header.key );
	event.resUpdate(
		event.socket, event.id,
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
	bool ret = SlaveWorker::chunkBuffer->at( header.listId )->deleteKey( header.key, header.keySize );

	key.set( header.keySize, header.key );
	event.resDelete(
		event.socket, event.id,
		header.listId, header.stripeId, header.chunkId,
		key, ret
	);
	this->dispatch( event );

	return ret;
}

bool SlaveWorker::handleGetChunkRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct ChunkHeader header;
	bool ret;
	if ( ! this->protocol.parseChunkHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleGetChunkRequest", "Invalid GET_CHUNK request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleGetChunkRequest",
		"[GET_CHUNK] List ID: %u; stripe ID: %u; chunk ID: %u.",
		header.listId, header.stripeId, header.chunkId
	);

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

	event.resGetChunk( event.socket, event.id, metadata, ret, isSealed ? chunk : 0 );
	this->dispatch( event );

	chunkBuffer->unlock( chunkBufferIndex );

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
		__ERROR__( "SlaveWorker", "handleSetChunkRequest", "The chunk (%u, %u, %u) does not exist.", metadata.listId, metadata.stripeId, metadata.chunkId );
	} else {
		if ( metadata.chunkId < SlaveWorker::dataChunkCount ) {
			if ( isSealed ) {
				uint32_t originalChunkSize;
				// Delete all keys in the chunk from the map
				offset = 0;
				originalChunkSize = chunkSize = chunk->getSize();
				while ( offset < chunkSize ) {
					keyValue = chunk->getKeyValue( offset );
					keyValue.deserialize( key.data, key.size, valueStr, valueSize );

					key.set( key.size, key.data );
					SlaveWorker::map->deleteKey(
						key, 0, keyMetadata,
						false, // needsLock
						false, // needsUnlock
						false  // needsUpdateOpMetadata
					);

					objSize = KEY_VALUE_METADATA_SIZE + key.size + valueSize;
					offset += objSize;
				}

				// Replace chunk contents
				chunk->loadFromSetChunkRequest( header.chunkData.data, header.chunkData.size );

				// Add all keys in the new chunk to the map
				offset = 0;
				chunkSize = header.chunkData.size;
				while( offset < chunkSize ) {
					keyValue = chunk->getKeyValue( offset );
					keyValue.deserialize( key.data, key.size, valueStr, valueSize );
					objSize = KEY_VALUE_METADATA_SIZE + key.size + valueSize;

					key.set( key.size, key.data );

					keyMetadata.set( metadata.listId, metadata.stripeId, metadata.chunkId );
					keyMetadata.offset = offset;
					keyMetadata.length = objSize;

					SlaveWorker::map->insertKey(
						key, 0, keyMetadata,
						false, // needsLock
						false, // needsUnlock
						false  // needsUpdateOpMetadata
					);

					offset += objSize;
				}

				// Re-insert into data chunk buffer
				assert( chunkBufferIndex == -1 );
				if ( ! chunkBuffer->reInsert( this, chunk, originalChunkSize - chunkSize, false, false ) ) {
					// The chunk is compacted before. Need to seal the chunk first
					// Seal from chunk->lastDelPos
					if ( chunk->lastDelPos < chunk->getSize() ) {
						// Only issue seal chunk request when new key-value pairs are received
						this->issueSealChunkRequest( chunk, chunk->lastDelPos );
					}
				}
			} else {
				struct KeyHeader keyHeader;
				struct KeyValueHeader keyValueHeader;
				uint32_t offset = ptr - buf;

				// Handle removed keys in DEGRADED_DELETE
				for ( uint32_t i = 0; i < header.chunkKeyValue.deleted; i++ ) {
					if ( ! this->protocol.parseKeyHeader( keyHeader, buf, size, offset ) ) {
						__ERROR__( "SlaveWorker", "handleSetChunkRequest", "Invalid key in deleted key list." );
						break;
					}
					key.set( keyHeader.keySize, keyHeader.key );

					// Update key map and chunk
					if ( SlaveWorker::map->deleteKey( key, PROTO_OPCODE_DELETE, keyMetadata, false, false, false ) ) {
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
			// Replace chunk contents
			chunk->loadFromSetChunkRequest( header.chunkData.data, header.chunkData.size );
		}
	}

	UNLOCK( cacheLock );
	UNLOCK( keysLock );
	if ( chunkBufferIndex != -1 )
		chunkBuffer->updateAndUnlockChunk( chunkBufferIndex );

	if ( isSealed || header.chunkKeyValue.isCompleted ) {
		event.resSetChunk( event.socket, event.id, metadata, ret );
		this->dispatch( event );
	}

	return ret;
}

bool SlaveWorker::handleSealChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
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

	if ( ! SlaveWorker::pending->eraseChunkUpdate( PT_SLAVE_PEER_UPDATE_CHUNK, event.id, event.socket, &pid, &chunkUpdate, true, false ) ) {
		UNLOCK( &SlaveWorker::pending->slavePeers.updateChunkLock );
		__ERROR__( "SlaveWorker", "handleUpdateChunkResponse", "Cannot find a pending slave UPDATE_CHUNK request that matches the response. This message will be discarded. (ID: %u)", event.id );
		return false;
	}
	// Check pending slave UPDATE requests
	pending = SlaveWorker::pending->count( PT_SLAVE_PEER_UPDATE_CHUNK, pid.id, false, true );

	__DEBUG__( BLUE, "SlaveWorker", "handleUpdateChunkResponse", "Pending slave UPDATE_CHUNK requests = %d (%s).", pending, success ? "success" : "fail" );
	if ( pending == 0 ) {
		// Only send application UPDATE response when the number of pending slave UPDATE_CHUNK requests equal 0
		Key key;
		KeyValueUpdate keyValueUpdate;
		MasterEvent masterEvent;

		if ( ! SlaveWorker::pending->eraseKeyValueUpdate( PT_MASTER_UPDATE, pid.parentId, 0, &pid, &keyValueUpdate ) ) {
			__ERROR__( "SlaveWorker", "handleUpdateChunkResponse", "Cannot find a pending master UPDATE request that matches the response. This message will be discarded." );
			return false;
		}

		key.set( keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.ptr );
		masterEvent.resUpdate(
			( MasterSocket * ) keyValueUpdate.ptr, pid.id, key,
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

	chunkUpdate.set(
		header.listId, header.stripeId, header.updatingChunkId,
		header.offset, header.length
	);
	chunkUpdate.setKeyValueUpdate( 0, 0, 0 );
	chunkUpdate.ptr = ( void * ) event.socket;

	if ( ! SlaveWorker::pending->eraseChunkUpdate( PT_SLAVE_PEER_DEL_CHUNK, event.id, event.socket, &pid, &chunkUpdate, true, false ) ) {
		UNLOCK( &SlaveWorker::pending->slavePeers.delChunkLock );
		__ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Cannot find a pending slave DELETE_CHUNK request that matches the response. This message will be discarded. (ID: %u)", event.id );
		return false;
	}
	// Check pending slave UPDATE requests
	pending = SlaveWorker::pending->count( PT_SLAVE_PEER_DEL_CHUNK, pid.id, false, true );

	// __ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Pending slave DELETE_CHUNK requests = %d.", pending );
	if ( pending == 0 ) {
		// Only send master DELETE response when the number of pending slave DELETE_CHUNK requests equal 0
		MasterEvent masterEvent;
		Key key;

		if ( ! SlaveWorker::pending->eraseKey( PT_MASTER_DEL, pid.parentId, 0, &pid, &key ) ) {
			__ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Cannot find a pending master DELETE request that matches the response. This message will be discarded." );
			return false;
		}

		masterEvent.resDelete( ( MasterSocket * ) pid.ptr, pid.id, key, success, true, false );
		SlaveWorker::eventQueue->insert( masterEvent );
	}
	return true;
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
	std::vector<uint32_t> pids;
	bool isInserted = false;

	if ( ! SlaveWorker::pending->eraseKey( PT_SLAVE_PEER_GET, event.id, event.socket, &pid ) ) {
		__ERROR__( "SlaveWorker", "handleGetResponse", "Cannot find a pending slave UNSEALED_GET request that matches the response. This message will be discarded. (ID: %u)", event.id );
		if ( success ) keyValue.free();
		return false;
	}

	if ( ! dmap->deleteDegradedKey( key, pids ) ) {
		__ERROR__( "SlaveWorker", "handleGetResponse", "SlaveWorker::degradedChunkBuffer->deleteDegradedChunk() failed." );
	}

	for ( int pidsIndex = 0, len = pids.size(); pidsIndex < len; pidsIndex++ ) {
		if ( pidsIndex == 0 ) {
			assert( pids[ pidsIndex ] == pid.id );
		}

		if ( ! SlaveWorker::pending->eraseDegradedOp( PT_SLAVE_PEER_DEGRADED_OPS, pids[ pidsIndex ], 0, &pid, &op ) ) {
			__ERROR__( "SlaveWorker", "handleGetResponse", "Cannot find a pending slave DEGRADED_OPS request that matches the response. This message will be discarded." );
			if ( success ) keyValue.free();
			continue;
		}

		if ( success ) {
			if ( op.opcode == PROTO_OPCODE_DEGRADED_DELETE ) {
				metadata.set( op.listId, op.stripeId, op.chunkId );
				dmap->deleteValue( key, metadata, PROTO_OPCODE_DELETE );
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
					masterEvent.resGet( op.socket, pid.parentId, keyValue, true );
				} else {
					masterEvent.resGet( op.socket, pid.parentId, key, true );
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
					if ( ! SlaveWorker::pending->insertKeyValueUpdate( PT_MASTER_UPDATE, pid.parentId, op.socket, op.data.keyValueUpdate ) ) {
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
						pid.parentId,
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
						op.socket, pid.parentId, key,
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
					if ( ! SlaveWorker::pending->insertKey( PT_MASTER_DEL, pid.parentId, op.socket, op.data.key ) ) {
						__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot insert into master DELETE pending map." );
					}

					this->sendModifyChunkRequest(
						pid.parentId, key.size, key.data,
						metadata,
						// not needed for deleting a key-value pair in an unsealed chunk:
						0, 0, 0, 0,
						false /* isSealed */,
						false /* isUpdate */
					);
				} else {
					masterEvent.resDelete(
						op.socket, pid.parentId, key,
						false, false, true
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

	if ( ! SlaveWorker::pending->eraseKeyValueUpdate( PT_SLAVE_PEER_UPDATE, event.id, event.socket, &pid, &keyValueUpdate, true, false ) ) {
		UNLOCK( &SlaveWorker::pending->slavePeers.updateLock );
		__ERROR__( "SlaveWorker", "handleUpdateResponse", "Cannot find a pending slave UPDATE request that matches the response. This message will be discarded. (ID: %u)", event.id );
		return false;
	}
	// Check pending slave UPDATE requests
	pending = SlaveWorker::pending->count( PT_SLAVE_PEER_UPDATE, pid.id, false, true );

	__DEBUG__( BLUE, "SlaveWorker", "handleUpdateResponse", "Pending slave UPDATE requests = %d (%s).", pending, success ? "success" : "fail" );
	if ( pending == 0 ) {
		// Only send master DELETE response when the number of pending slave DELETE requests equal 0
		MasterEvent masterEvent;

		if ( ! SlaveWorker::pending->eraseKeyValueUpdate( PT_MASTER_UPDATE, pid.parentId, 0, &pid, &keyValueUpdate ) ) {
			__ERROR__( "SlaveWorker", "handleUpdateResponse", "Cannot find a pending master UPDATE request that matches the response. This message will be discarded." );
			return false;
		}

		masterEvent.resUpdate(
			( MasterSocket * ) keyValueUpdate.ptr, pid.id,
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

	if ( ! SlaveWorker::pending->eraseKey( PT_SLAVE_PEER_DEL, event.id, event.socket, &pid, &key, true, false ) ) {
		UNLOCK( &SlaveWorker::pending->slavePeers.delLock );
		__ERROR__( "SlaveWorker", "handleDeleteResponse", "Cannot find a pending slave DELETE request that matches the response. This message will be discarded. (ID: %u)", event.id );
		return false;
	}
	// Check pending slave UPDATE requests
	pending = SlaveWorker::pending->count( PT_SLAVE_PEER_DEL, pid.id, false, true );

	__DEBUG__( BLUE, "SlaveWorker", "handleDeleteResponse", "Pending slave DELETE requests = %d (%s).", pending, success ? "success" : "fail" );
	if ( pending == 0 ) {
		// Only send master DELETE response when the number of pending slave DELETE requests equal 0
		MasterEvent masterEvent;
		Key key;

		if ( ! SlaveWorker::pending->eraseKey( PT_MASTER_DEL, pid.parentId, 0, &pid, &key ) ) {
			__ERROR__( "SlaveWorker", "handleDeleteResponse", "Cannot find a pending master DELETE request that matches the response. This message will be discarded." );
			return false;
		}

		masterEvent.resDelete( ( MasterSocket * ) key.ptr, pid.id, key, success, true, false );
		SlaveWorker::eventQueue->insert( masterEvent );
	}
	return true;
}

bool SlaveWorker::handleGetChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	int pending;
	uint32_t listId, stripeId, chunkId;
	std::unordered_map<PendingIdentifier, ChunkRequest>::iterator it, tmp, end;
	ChunkRequest chunkRequest;
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
	if ( ! SlaveWorker::pending->findChunkRequest( PT_SLAVE_PEER_GET_CHUNK, event.id, event.socket, it, true, false ) ) {
		UNLOCK( &SlaveWorker::pending->slavePeers.getChunkLock );
		__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot find a pending slave GET_CHUNK request that matches the response. This message will be discarded." );
		return false;
	}

	chunkRequest = it->second;

	// Prepare stripe buffer
	for ( uint32_t i = 0, total = SlaveWorker::chunkCount; i < total; i++ ) {
		this->chunks[ i ] = 0;
	}

	// Check remaining slave GET_CHUNK requests in the pending set
	pending = 0;
	tmp = it;
	end = SlaveWorker::pending->slavePeers.getChunk.end();
	while( tmp != end && tmp->first.id == event.id ) {
		if ( tmp->second.chunkId == chunkId ) {
			// Store the chunk into the buffer
			if ( success ) {
				if ( header.chunkData.size ) {
					// The requested chunk is sealed
					tmp->second.chunk = SlaveWorker::chunkPool->malloc();
					tmp->second.chunk->loadFromGetChunkRequest(
						header.chunkData.listId, header.chunkData.stripeId, header.chunkData.chunkId,
						header.chunkData.chunkId >= SlaveWorker::dataChunkCount, // isParity
						header.chunkData.data, header.chunkData.size
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

			if ( ! SlaveWorker::pending->eraseDegradedOp( PT_SLAVE_PEER_DEGRADED_OPS, event.id, event.socket, &pid, &op ) ) {
				__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot find a pending slave DEGRADED_OPS request that matches the response. This message will be discarded." );
			} else {
				DegradedMap *dmap = &SlaveWorker::degradedChunkBuffer->map;
				bool isKeyValueFound, isSealed;
				Key key;
				KeyValue keyValue;
				KeyMetadata keyMetadata;
				Metadata metadata;
				std::vector<uint32_t> pids;

				keyMetadata.offset = 0;

				if ( ! dmap->deleteDegradedChunk( op.listId, op.stripeId, op.chunkId, pids ) ) {
					__ERROR__( "SlaveWorker", "handleGetChunkResponse", "dmap->deleteDegradedChunk() failed (%u, %u, %u).", op.listId, op.stripeId, op.chunkId );
				}
				metadata.set( op.listId, op.stripeId, op.chunkId );

				for ( int pidsIndex = 0, len = pids.size(); pidsIndex < len; pidsIndex++ ) {
					if ( pidsIndex == 0 ) {
						assert( pids[ pidsIndex ] == pid.id );
					} else {
						if ( ! SlaveWorker::pending->eraseDegradedOp( PT_SLAVE_PEER_DEGRADED_OPS, pids[ pidsIndex ], 0, &pid, &op ) ) {
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
							event.resGet( op.socket, pid.parentId, keyValue, true );
						else
							event.resGet( op.socket, pid.parentId, key, true );
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
							if ( ! SlaveWorker::pending->insertKeyValueUpdate( PT_MASTER_UPDATE, pid.parentId, op.socket, op.data.keyValueUpdate ) ) {
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
								pid.parentId,
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
								op.socket, pid.parentId, key,
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
							// Insert into master DELETE pending set
							op.data.key.ptr = op.socket;
							if ( ! SlaveWorker::pending->insertKey( PT_MASTER_DEL, pid.parentId, op.socket, op.data.key ) ) {
								__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot insert into master DELETE pending map." );
							}

							SlaveWorker::degradedChunkBuffer->deleteKey(
								PROTO_OPCODE_DELETE,
								key.size, key.data,
								metadata,
								true /* isSealed */,
								deltaSize, delta, chunk
							);

							this->sendModifyChunkRequest(
								pid.parentId, key.size, key.data,
								metadata, keyMetadata.offset, deltaSize, 0, delta,
								true /* isSealed */,
								false /* isUpdate */
							);
						} else {
							MasterEvent event;

							event.resDelete( op.socket, pid.parentId, key, false, false, true );
							this->dispatch( event );
							op.data.key.free();
						}
					}
				}
			}
		} else {
			SlavePeerSocket *target;
			PendingIdentifier pid = it->first;
			Metadata metadata;

			std::unordered_set<uint32_t> *stripeIds = SlaveWorker::pending->findRecovery(
				pid.parentId, target,
				listId, chunkId
			);

			__ERROR__( "SlaveWorker", "handleGetChunkResponse", "TODO: Handle the case for recovery: (%u, %u, %u); target = %p.", listId, stripeId, chunkId, target );

			// Send SET_CHUNK request
			uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
			chunkRequest.set(
				listId, stripeId, chunkId, target,
				0 /* chunk */, false /* isDegraded */
			);
			if ( ! SlaveWorker::pending->insertChunkRequest( PT_SLAVE_PEER_SET_CHUNK, requestId, pid.parentId, target, chunkRequest ) ) {
				__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot insert into slave CHUNK_REQUEST pending map." );
			}

			metadata.set( listId, stripeId, chunkId );

			event.reqSetChunk( target, requestId, metadata, this->chunks[ chunkId ], false );
			this->dispatch( event );
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

	chunkRequest.set(
		header.listId, header.stripeId, header.chunkId,
		event.socket, 0 // ptr
	);

	if ( ! SlaveWorker::pending->eraseChunkRequest( PT_SLAVE_PEER_SET_CHUNK, event.id, event.socket, &pid, &chunkRequest ) ) {
		__ERROR__( "SlaveWorker", "handleSetChunkResponse", "Cannot find a pending slave SET_CHUNK request that matches the response. This message will be discarded." );
	}

	if ( chunkRequest.isDegraded ) {
		// Release degraded lock
		uint32_t remaining, total;
		if ( ! SlaveWorker::pending->eraseReleaseDegradedLock( pid.parentId, 1, remaining, total, &pid ) ) {
			__ERROR__( "SlaveWorker", "handleSetChunkResponse", "Cannot find a pending coordinator release degraded lock request that matches the response. The message will be discarded." );
			return false;
		}

		if ( remaining == 0 ) {
			// Tell the coordinator that all degraded lock is released
			CoordinatorEvent coordinatorEvent;
			coordinatorEvent.resReleaseDegradedLock( ( CoordinatorSocket * ) pid.ptr, pid.id, total );
			SlaveWorker::eventQueue->insert( coordinatorEvent );
		}
	} else {
		// Recovery
		// TODO
	}

	return true;
}

bool SlaveWorker::performDegradedRead( MasterSocket *masterSocket, uint32_t listId, uint32_t stripeId, uint32_t lostChunkId, bool isSealed, uint8_t opcode, uint32_t parentId, Key *key, KeyValueUpdate *keyValueUpdate ) {
	Key mykey;
	SlavePeerEvent event;
	SlavePeerSocket *socket = 0;
	uint32_t selected = 0;

	SlaveWorker::stripeList->get( listId, this->paritySlaveSockets, this->dataSlaveSockets );

	if ( ! isSealed ) {
		// Check whether there are surviving parity slaves
		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			socket = this->paritySlaveSockets[ ( parentId + i ) % SlaveWorker::parityChunkCount ];
			if ( socket->ready() ) break;
		}
		if ( ! socket ) {
			__ERROR__( "SlaveWorker", "performDegradedRead", "There are no surviving parity slaves. The data cannot be recovered." );
			return false;
		}
	} else {
		// Check whether the number of surviving nodes >= k
		for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
			// Never get from the overloaded slave (even if it is still "ready")
			if ( i == lostChunkId )
				continue;
			socket = ( i < SlaveWorker::dataChunkCount ) ?
			         ( this->dataSlaveSockets[ i ] ) :
			         ( this->paritySlaveSockets[ i - SlaveWorker::dataChunkCount ] );
			if ( socket->ready() ) selected++;
		}
		if ( selected < SlaveWorker::dataChunkCount ) {
			__ERROR__( "SlaveWorker", "performDegradedRead", "The number of surviving nodes is less than k. The data cannot be recovered." );
			return false;
		}
	}

	// Add to degraded operation pending set
	uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
	DegradedOp op;
	op.set( listId, stripeId, lostChunkId, isSealed, opcode, masterSocket );
	if ( opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		op.data.keyValueUpdate = *keyValueUpdate;
		mykey.set( keyValueUpdate->size, keyValueUpdate->data );
	} else {
		op.data.key = *key;
		mykey.set( key->size, key->data );
	}

	if ( isSealed || ! socket->self ) {
		if ( ! SlaveWorker::pending->insertDegradedOp( PT_SLAVE_PEER_DEGRADED_OPS, requestId, parentId, 0, op ) ) {
			__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave DEGRADED_OPS pending map." );
		}
	}

	// Insert the degraded operation into degraded chunk buffer pending set
	bool needsContinue;
	if ( isSealed ) {
		needsContinue = SlaveWorker::degradedChunkBuffer->map.insertDegradedChunk( listId, stripeId, lostChunkId, requestId );
		// printf( "insertDegradedChunk(): (%u, %u, %u) - needsContinue: %d\n", listId, stripeId, lostChunkId, needsContinue );
	} else {
		Key k;
		if ( opcode == PROTO_OPCODE_DEGRADED_UPDATE )
			k.set( keyValueUpdate->size, keyValueUpdate->data );
		else
			k = *key;
		needsContinue = SlaveWorker::degradedChunkBuffer->map.insertDegradedKey( k, requestId );
		// printf( "insertDegradedKey(): (%.*s) - needsContinue: %d\n", k.size, k.data, needsContinue );
	}

	if ( isSealed ) {
		if ( ! needsContinue )
			return true;

		// Send GET_CHUNK requests to surviving nodes
		Metadata metadata;
		metadata.set( listId, stripeId, 0 );
		selected = 0;
		for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
			if ( selected >= SlaveWorker::dataChunkCount )
				break;
			if ( i == lostChunkId )
				continue;

			socket = ( i < SlaveWorker::dataChunkCount ) ?
			         ( this->dataSlaveSockets[ i ] ) :
			         ( this->paritySlaveSockets[ i - SlaveWorker::dataChunkCount ] );

			// Add to pending GET_CHUNK request set
			ChunkRequest chunkRequest;
			chunkRequest.set( listId, stripeId, i, socket, 0, true );
			if ( socket->self ) {
				chunkRequest.chunk = SlaveWorker::map->findChunkById( listId, stripeId, i );
				// Check whether the chunk is sealed or not
				if ( ! chunkRequest.chunk ) {
					chunkRequest.chunk = Coding::zeros;
				} else {
					MixedChunkBuffer *chunkBuffer = SlaveWorker::chunkBuffer->at( listId );
					int chunkBufferIndex = chunkBuffer->lockChunk( chunkRequest.chunk, true );
					bool isSealed = ( chunkBufferIndex == -1 );
					if ( ! isSealed )
						chunkRequest.chunk = Coding::zeros;
					chunkBuffer->unlock( chunkBufferIndex );
				}

				if ( ! SlaveWorker::pending->insertChunkRequest( PT_SLAVE_PEER_GET_CHUNK, requestId, parentId, socket, chunkRequest ) ) {
					__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave CHUNK_REQUEST pending map." );
				}
			} else if ( socket->ready() ) {
				chunkRequest.chunk = 0;

				if ( ! SlaveWorker::pending->insertChunkRequest( PT_SLAVE_PEER_GET_CHUNK, requestId, parentId, socket, chunkRequest ) ) {
					__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave CHUNK_REQUEST pending map." );
				}
			} else {
				continue;
			}
			selected++;
		}

		selected = 0;
		for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
			if ( selected >= SlaveWorker::dataChunkCount )
				break;
			if ( i == lostChunkId )
				continue;

			socket = ( i < SlaveWorker::dataChunkCount ) ?
			         ( this->dataSlaveSockets[ i ] ) :
			         ( this->paritySlaveSockets[ i - SlaveWorker::dataChunkCount ] );

			if ( socket->self ) {
				selected++;
			} else if ( socket->ready() ) {
				metadata.chunkId = i;
				event.reqGetChunk( socket, requestId, metadata );
				SlaveWorker::eventQueue->insert( event );
				selected++;
			}
		}

		return ( selected >= SlaveWorker::dataChunkCount );
	} else {
		// Send GET request to surviving parity slave
		if ( socket->self ) {
			KeyValue keyValue;
			MasterEvent masterEvent;

			bool success = SlaveWorker::chunkBuffer->at( listId )->findValueByKey( mykey.data, mykey.size, &keyValue, &mykey );
			if ( success && opcode != PROTO_OPCODE_DEGRADED_DELETE ) {
				// Insert into degradedChunkBuffer
				char *key, *value;
				uint8_t keySize;
				uint32_t valueSize;
				Metadata metadata;

				metadata.set( listId, stripeId, lostChunkId );

				keyValue.deserialize( key, keySize, value, valueSize );
				keyValue.dup( key, keySize, value, valueSize );

				if ( ! SlaveWorker::degradedChunkBuffer->map.insertValue( keyValue, metadata ) ) {
					__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into degraded chunk buffer values map. (Key: %.*s)", keySize, key );
					// keyValue.free();
					// success = false;
				}
			}

			switch( opcode ) {
				case PROTO_OPCODE_DEGRADED_GET:
					if ( success ) {
						masterEvent.resGet( masterSocket, parentId, keyValue, true );
					} else {
						// Return failure to master
						masterEvent.resGet( masterSocket, parentId, mykey, true );
					}
					this->dispatch( masterEvent );
					op.data.key.free();
					break;
				case PROTO_OPCODE_DEGRADED_UPDATE:
					if ( success ) {
						Metadata metadata;
						metadata.set( listId, stripeId, lostChunkId );

						uint32_t dataUpdateOffset = KeyValue::getChunkUpdateOffset(
							0,                     // chunkOffset
							keyValueUpdate->size,  // keySize
							keyValueUpdate->offset // valueUpdateOffset
						);

						char *valueUpdate = ( char * ) keyValueUpdate->ptr;

						// Compute data delta
						Coding::bitwiseXOR(
							valueUpdate,
							keyValue.data + dataUpdateOffset, // original data
							valueUpdate,                      // new data
							keyValueUpdate->length
						);
						// Perform actual data update
						Coding::bitwiseXOR(
							keyValue.data + dataUpdateOffset,
							keyValue.data + dataUpdateOffset, // original data
							valueUpdate,                      // new data
							keyValueUpdate->length
						);

						// Send UPDATE request to the parity slaves
						this->sendModifyChunkRequest(
							event.id,
							keyValueUpdate->size,
							keyValueUpdate->data,
							metadata,
							0, /* chunkUpdateOffset */
							keyValueUpdate->length, /* deltaSize */
							keyValueUpdate->offset,
							valueUpdate,
							false /* isSealed */,
							true /* isUpdate */
						);
					} else {
						masterEvent.resUpdate(
							masterSocket, parentId, mykey,
							keyValueUpdate->offset,
							keyValueUpdate->length,
							false, false, true
						);
						this->dispatch( masterEvent );
					}
					op.data.keyValueUpdate.free();
					delete[] ( ( char * ) op.data.keyValueUpdate.ptr );
					break;
				case PROTO_OPCODE_DEGRADED_DELETE:
					if ( success ) {
						Metadata metadata;
						KeyMetadata keyMetadata;
						metadata.set( listId, stripeId, lostChunkId );
						keyMetadata.set( listId, stripeId, lostChunkId );

						SlaveWorker::map->insertOpMetadata(
							PROTO_OPCODE_DELETE,
							mykey, keyMetadata
						);

						uint32_t tmp = 0;
						SlaveWorker::degradedChunkBuffer->deleteKey(
							PROTO_OPCODE_DELETE,
							mykey.size, mykey.data,
							metadata,
							true /* isSealed */,
							tmp, 0, 0
						);

						this->sendModifyChunkRequest(
							parentId, mykey.size, mykey.data,
							metadata,
							// not needed for deleting a key-value pair in an unsealed chunk:
							0, 0, 0, 0,
							false /* isSealed */,
							false /* isUpdate */
						);
					} else {
						masterEvent.resDelete( masterSocket, parentId, mykey, false, false, true );
						this->dispatch( masterEvent );
					}
					op.data.key.free();
					break;
			}

			return success;
		} else if ( needsContinue ) {
			if ( ! SlaveWorker::pending->insertKey( PT_SLAVE_PEER_GET, requestId, parentId, socket, op.data.key ) ) {
				__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave GET pending map." );
			}
			event.reqGet( socket, requestId, listId, lostChunkId, op.data.key );
			this->dispatch( event );
		}
		return true;
	}
}

bool SlaveWorker::sendModifyChunkRequest( uint32_t parentId, uint8_t keySize, char *keyStr, Metadata &metadata, uint32_t offset, uint32_t deltaSize, uint32_t valueUpdateOffset, char *delta, bool isSealed, bool isUpdate ) {
	Key key;
	KeyValueUpdate keyValueUpdate;
	uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );

	key.set( keySize, keyStr );
	this->getSlaves( metadata.listId );

	if ( isSealed ) {
		// Send UPDATE_CHUNK / DELETE_CHUNK requests to parity slaves if the chunk is sealed
		ChunkUpdate chunkUpdate;
		chunkUpdate.set(
			metadata.listId, metadata.stripeId, metadata.chunkId,
			offset, deltaSize
		);
		chunkUpdate.setKeyValueUpdate( key.size, key.data, offset );

		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			if ( this->paritySlaveSockets[ i ]->self )
				continue;

			chunkUpdate.chunkId = SlaveWorker::dataChunkCount + i; // updatingChunkId
			chunkUpdate.ptr = ( void * ) this->paritySlaveSockets[ i ];
			if ( ! SlaveWorker::pending->insertChunkUpdate(
				isUpdate ? PT_SLAVE_PEER_UPDATE_CHUNK : PT_SLAVE_PEER_DEL_CHUNK,
				requestId, parentId,
				( void * ) this->paritySlaveSockets[ i ],
				chunkUpdate
			) ) {
				__ERROR__( "SlaveWorker", "sendModifyChunkRequest", "Cannot insert into slave %s pending map.", isUpdate ? "UPDATE_CHUNK" : "DELETE_CHUNK" );
			}
		}

		// Start sending packets only after all the insertion to the slave peer DELETE_CHUNK pending set is completed
		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			if ( this->paritySlaveSockets[ i ]->self ) {
				SlaveWorker::chunkBuffer->at( metadata.listId )->update(
					metadata.stripeId, metadata.chunkId,
					offset, deltaSize, delta,
					this->chunks, this->dataChunk, this->parityChunk,
					true /* isDelete */
				);
			} else {
				// Prepare DELETE_CHUNK request
				size_t size;
				Packet *packet = SlaveWorker::packetPool->malloc();
				packet->setReferenceCount( 1 );
				if ( isUpdate ) {
					this->protocol.reqUpdateChunk(
						size,
						requestId,
						metadata.listId,
						metadata.stripeId,
						metadata.chunkId,
						offset,
						deltaSize,                       // length
						SlaveWorker::dataChunkCount + i, // updatingChunkId
						delta,
						packet->data
					);
				} else {
					this->protocol.reqDeleteChunk(
						size,
						requestId,
						metadata.listId,
						metadata.stripeId,
						metadata.chunkId,
						offset,
						deltaSize,                       // length
						SlaveWorker::dataChunkCount + i, // updatingChunkId
						delta,
						packet->data
					);
				}
				packet->size = ( uint32_t ) size;

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
		}
	} else {
		// Send UPDATE / DELETE request if the chunk is not yet sealed

		// Check whether any of the parity slaves are self-socket
		uint32_t self = 0;
		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			if ( this->paritySlaveSockets[ i ]->self ) {
				self = i + 1;
				break;
			}
		}

		// Prepare UPDATE / DELETE request
		size_t size;
		Packet *packet = SlaveWorker::packetPool->malloc();
		packet->setReferenceCount( self == 0 ? SlaveWorker::parityChunkCount : SlaveWorker::parityChunkCount - 1 );
		if ( isUpdate ) {
			this->protocol.reqUpdate(
				size,
				requestId,
				metadata.listId,
				metadata.stripeId,
				metadata.chunkId,
				keyStr,
				keySize,
				delta /* valueUpdate */,
				valueUpdateOffset,
				deltaSize /* valueUpdateSize */,
				offset, // Chunk update offset
				packet->data
			);
		} else {
			this->protocol.reqDelete(
				size,
				requestId,
				metadata.listId,
				metadata.stripeId,
				metadata.chunkId,
				keyStr,
				keySize,
				packet->data
			);
		}
		packet->size = ( uint32_t ) size;

		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			if ( this->paritySlaveSockets[ i ]->self )
				continue;

			if ( isUpdate ) {
				keyValueUpdate.ptr = ( void * ) this->paritySlaveSockets[ i ];
				if ( ! SlaveWorker::pending->insertKeyValueUpdate(
					PT_SLAVE_PEER_UPDATE, requestId, parentId,
					( void * ) this->paritySlaveSockets[ i ],
					keyValueUpdate
				) ) {
					__ERROR__( "SlaveWorker", "handleUpdateRequest", "Cannot insert into slave UPDATE pending map." );
				}
			} else {
				key.ptr = ( void * ) this->paritySlaveSockets[ i ];
				if ( ! SlaveWorker::pending->insertKey(
					PT_SLAVE_PEER_DEL, requestId, parentId,
					( void * ) this->paritySlaveSockets[ i ],
					key
				) ) {
					__ERROR__( "SlaveWorker", "handleDeleteRequest", "Cannot insert into slave DELETE pending map." );
				}
			}
		}

		// Start sending packets only after all the insertion to the slave peer DELETE pending set is completed
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

		if ( ! self ) {
			self--;
			if ( isUpdate ) {
				bool ret = SlaveWorker::chunkBuffer->at( metadata.listId )->updateKeyValue(
					keyStr, keySize,
					valueUpdateOffset, deltaSize, delta
				);
				if ( ! ret ) {
					// Use the chunkUpdateOffset
					SlaveWorker::chunkBuffer->at( metadata.listId )->update(
						metadata.stripeId, metadata.chunkId,
						offset, deltaSize, delta,
						this->chunks, this->dataChunk, this->parityChunk
					);
					ret = true;
				}
			} else {
				SlaveWorker::chunkBuffer->at( metadata.listId )->deleteKey( keyStr, keySize );
			}
		}
	}
	return true;
}

void SlaveWorker::free() {
	if ( this->storage ) {
		this->storage->stop();
		Storage::destroy( this->storage );
	}
	this->protocol.free();
	delete this->buffer.data;
	this->buffer.data = 0;
	this->buffer.size = 0;
	delete this->chunkStatus;
	this->dataChunk->free();
	this->parityChunk->free();
	delete this->dataChunk;
	delete this->parityChunk;
	delete[] this->chunks;
	delete[] this->freeChunks;
	delete[] this->dataSlaveSockets;
	delete[] this->paritySlaveSockets;
}

void *SlaveWorker::run( void *argv ) {
	SlaveWorker *worker = ( SlaveWorker * ) argv;
	WorkerRole role = worker->getRole();
	SlaveEventQueue *eventQueue = SlaveWorker::eventQueue;

#define SLAVE_WORKER_EVENT_LOOP(_EVENT_TYPE_, _EVENT_QUEUE_) \
	do { \
		_EVENT_TYPE_ event; \
		bool ret; \
		while( worker->getIsRunning() | ( ret = _EVENT_QUEUE_->extract( event ) ) ) { \
			if ( ret ) \
				worker->dispatch( event ); \
		} \
	} while( 0 )

	switch ( role ) {
		case WORKER_ROLE_MIXED:
			// SLAVE_WORKER_EVENT_LOOP(
			// 	MixedEvent,
			// 	eventQueue->mixed
			// );
		{
			MixedEvent event;
			bool ret;
			while( worker->getIsRunning() | ( ret = eventQueue->extractMixed( event ) ) ) {
				if ( ret )
					worker->dispatch( event );
			}
		}
			break;
		case WORKER_ROLE_CODING:
			SLAVE_WORKER_EVENT_LOOP(
				CodingEvent,
				eventQueue->separated.coding
			);
			break;
		case WORKER_ROLE_COORDINATOR:
			SLAVE_WORKER_EVENT_LOOP(
				CoordinatorEvent,
				eventQueue->separated.coordinator
			);
			break;
		case WORKER_ROLE_IO:
			SLAVE_WORKER_EVENT_LOOP(
				IOEvent,
				eventQueue->separated.io
			);
			break;
		case WORKER_ROLE_MASTER:
			SLAVE_WORKER_EVENT_LOOP(
				MasterEvent,
				eventQueue->separated.master
			);
			break;
		case WORKER_ROLE_SLAVE:
			SLAVE_WORKER_EVENT_LOOP(
				SlaveEvent,
				eventQueue->separated.slave
			);
			break;
		case WORKER_ROLE_SLAVE_PEER:
			SLAVE_WORKER_EVENT_LOOP(
				SlavePeerEvent,
				eventQueue->separated.slavePeer
			);
			break;
		default:
			break;
	}

	worker->free();
	pthread_exit( 0 );
	return 0;
}

bool SlaveWorker::init() {
	Slave *slave = Slave::getInstance();

	SlaveWorker::idGenerator = &slave->idGenerator;
	SlaveWorker::dataChunkCount = slave->config.global.coding.params.getDataChunkCount();
	SlaveWorker::parityChunkCount = slave->config.global.coding.params.getParityChunkCount();
	SlaveWorker::chunkCount = SlaveWorker::dataChunkCount + SlaveWorker::parityChunkCount;
	SlaveWorker::slavePeers = &slave->sockets.slavePeers;
	SlaveWorker::pending = &slave->pending;
	SlaveWorker::slaveServerAddr = &slave->config.slave.slave.addr;
	SlaveWorker::coding = slave->coding;
	SlaveWorker::eventQueue = &slave->eventQueue;
	SlaveWorker::stripeList = slave->stripeList;
	SlaveWorker::stripeListIndex = &slave->stripeListIndex;
	SlaveWorker::map = &slave->map;
	SlaveWorker::chunkPool = slave->chunkPool;
	SlaveWorker::chunkBuffer = &slave->chunkBuffer;
	SlaveWorker::degradedChunkBuffer = &slave->degradedChunkBuffer;
	SlaveWorker::packetPool = &slave->packetPool;
	return true;
}

bool SlaveWorker::init( GlobalConfig &globalConfig, SlaveConfig &slaveConfig, WorkerRole role, uint32_t workerId ) {
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			globalConfig.size.key,
			globalConfig.size.chunk
		),
		SlaveWorker::dataChunkCount
	);
	this->role = role;
	this->workerId = workerId;
	this->buffer.data = new char[ globalConfig.size.chunk ];
	this->buffer.size = globalConfig.size.chunk;
	this->chunkStatus = new BitmaskArray( SlaveWorker::chunkCount, 1 );
	this->dataChunk = new Chunk();
	this->parityChunk = new Chunk();
	this->chunks = new Chunk*[ SlaveWorker::chunkCount ];
	this->freeChunks = new Chunk[ SlaveWorker::dataChunkCount ];
	for( uint32_t i = 0; i < SlaveWorker::dataChunkCount; i++ ) {
		this->freeChunks[ i ].init( globalConfig.size.chunk );
		this->freeChunks[ i ].init();
	}
	this->dataChunk->init( globalConfig.size.chunk );
	this->parityChunk->init( globalConfig.size.chunk );
	this->dataChunk->init();
	this->parityChunk->init();
	this->dataSlaveSockets = new SlavePeerSocket*[ SlaveWorker::dataChunkCount ];
	this->paritySlaveSockets = new SlavePeerSocket*[ SlaveWorker::parityChunkCount ];
	switch( this->role ) {
		case WORKER_ROLE_MIXED:
		case WORKER_ROLE_IO:
			this->storage = Storage::instantiate( slaveConfig );
			this->storage->start();
			break;
		default:
			this->storage = 0;
			break;
	}
	return role != WORKER_ROLE_UNDEFINED;
}

bool SlaveWorker::start() {
	this->isRunning = true;
	if ( pthread_create( &this->tid, NULL, SlaveWorker::run, ( void * ) this ) != 0 ) {
		__ERROR__( "SlaveWorker", "start", "Cannot start worker thread." );
		return false;
	}
	return true;
}

void SlaveWorker::stop() {
	this->isRunning = false;
}

void SlaveWorker::print( FILE *f ) {
	char role[ 16 ];
	switch( this->role ) {
		case WORKER_ROLE_MIXED:
			strcpy( role, "Mixed" );
			break;
		case WORKER_ROLE_CODING:
			strcpy( role, "Coding" );
			break;
		case WORKER_ROLE_COORDINATOR:
			strcpy( role, "Coordinator" );
			break;
		case WORKER_ROLE_IO:
			strcpy( role, "I/O" );
			break;
		case WORKER_ROLE_MASTER:
			strcpy( role, "Master" );
			break;
		case WORKER_ROLE_SLAVE:
			strcpy( role, "Slave" );
			break;
		case WORKER_ROLE_SLAVE_PEER:
			strcpy( role, "Slave peer" );
			break;
		default:
			return;
	}
	fprintf( f, "%11s worker (Thread ID = %lu): %srunning\n", role, this->tid, this->isRunning ? "" : "not " );
}
