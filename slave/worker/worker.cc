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

	if ( event.type != COORDINATOR_EVENT_TYPE_PENDING )
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
					case PROTO_OPCODE_SEAL_CHUNKS:
						Slave::getInstance()->seal();
						break;
					case PROTO_OPCODE_FLUSH_CHUNKS:
						Slave::getInstance()->flush();
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
		case MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
			success = true;
			break;
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_GET_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_FAILURE:
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
		// REMAPPING_SET_LOCK
		case MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRemappingSetLock(
				buffer.size,
				event.id,
				success,
				event.message.remap.listId,
				event.message.remap.chunkId,
				event.message.remap.key.size,
				event.message.remap.key.data
			);
			break;
		// REMAPPING_SET_LOCK
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
					case PROTO_OPCODE_REMAPPING_LOCK:
						this->handleRemappingSetLockRequest( event, buffer.data, buffer.size );
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
						this->handleDegradedRequest( event, header.opcode, buffer.data, buffer.size );
						this->load.get();
						break;
					case PROTO_OPCODE_DEGRADED_UPDATE:
						this->handleDegradedRequest( event, header.opcode, buffer.data, buffer.size );
						this->load.update();
						break;
					case PROTO_OPCODE_DEGRADED_DELETE:
						this->handleDegradedRequest( event, header.opcode, buffer.data, buffer.size );
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
	bool success, connected, isSend;
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
			buffer.data = this->protocol.reqSetChunk(
				buffer.size,
				event.id,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId,
				event.message.chunk.chunk->getSize(),
				event.message.chunk.chunk->getData()
			);
			break;
		case SLAVE_PEER_EVENT_TYPE_SEAL_CHUNK_REQUEST:
			this->issueSealChunkRequest( event.message.chunk.chunk );
			return;
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
			buffer.data = this->protocol.resGetChunk(
				buffer.size,
				event.id,
				true,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId,
				event.message.chunk.chunk->getSize(),
				event.message.chunk.chunk->getData()
			);
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
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							this->handleSetChunkRequest( event, buffer.data, buffer.size );
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
		this->dispatch( event );
	} else if ( map->findRemappingRecordByKey( header.key, header.keySize, &remappingRecord, &key ) ) {
		// Redirect request to remapped slave
		event.resRedirect( event.socket, event.id, PROTO_OPCODE_GET, key, remappingRecord );
		ret = false;
		this->dispatch( event );
	} else {
		event.resGet( event.socket, event.id, key, false );
		ret = false;
		this->dispatch( event );
	}
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

bool SlaveWorker::handleRemappingSetLockRequest( MasterEvent event, char *buf, size_t size ) {
	struct RemappingLockHeader header;
	if ( ! this->protocol.parseRemappingLockHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleRemappingSetLockRequest", "Invalid REMAPPING_SET_LOCK request (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleRemappingSetLockRequest",
		"[REMAPPING_SET_LOCK] Key: %.*s (key size = %u); remapped list ID: %u, remapped chunk ID: %u",
		( int ) header.keySize, header.key, header.keySize, header.listId, header.chunkId
	);

	Key key;
	key.set( header.keySize, header.key );

	RemappingRecord remappingRecord( header.listId, header.chunkId );
	if ( SlaveWorker::map->insertRemappingRecord( key, remappingRecord ) ) {
		event.resRemappingSetLock( event.socket, event.id, key, remappingRecord, true );
	} else {
		event.resRemappingSetLock( event.socket, event.id, key, remappingRecord, false );
	}
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
	KeyMetadata keyMetadata;
	Metadata metadata;
	Chunk *chunk;
	RemappingRecord remappingRecord;
	if ( map->findValueByKey( header.key, header.keySize, &keyValue, &key, &keyMetadata, &metadata, &chunk ) ) {
		uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
		uint32_t offset = keyMetadata.offset + PROTO_KEY_VALUE_SIZE + header.keySize + header.valueUpdateOffset;

		LOCK_T *keysLock;
		std::unordered_map<Key, KeyMetadata> *keys;
		SlaveWorker::map->getKeysMap( keys, keysLock );
		// Lock the data chunk buffer
		MixedChunkBuffer *chunkBuffer = SlaveWorker::chunkBuffer->at( metadata.listId );
		int chunkBufferIndex;

		if ( SlaveWorker::parityChunkCount ) {
			// Find parity slaves
			this->getSlaves(
				header.key, header.keySize,
				metadata.listId, metadata.chunkId
			);
		}

		chunkBufferIndex = chunkBuffer->lockChunk( chunk );
		if ( chunkBufferIndex == -1 ) {
			LOCK( keysLock );
			if ( SlaveWorker::parityChunkCount ) {
				KeyValueUpdate keyValueUpdate;
				ChunkUpdate chunkUpdate;

				// Compute delta and perform update
				chunk->computeDelta(
					header.valueUpdate, // delta
					header.valueUpdate, // new data
					offset, header.valueUpdateSize,
					true // perform update
				);
				UNLOCK( keysLock );

				// Add the current request to the pending set
				keyValueUpdate.dup( header.keySize, header.key, ( void * ) event.socket );
				keyValueUpdate.offset = header.valueUpdateOffset;
				keyValueUpdate.length = header.valueUpdateSize;

				if ( ! SlaveWorker::pending->insertKeyValueUpdate( PT_MASTER_UPDATE, event.id, ( void * ) event.socket, keyValueUpdate ) ) {
					__ERROR__( "SlaveWorker", "handleUpdateRequest", "Cannot insert into master UPDATE pending map (ID = %u).", event.id );
				}

				chunkUpdate.set(
					metadata.listId, metadata.stripeId, metadata.chunkId,
					offset, header.valueUpdateSize
				);
				chunkUpdate.setKeyValueUpdate( keyValueUpdate.size, keyValueUpdate.data, header.valueUpdateOffset );

				// Send UPDATE_CHUNK requests to parity slaves
				for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
					chunkUpdate.chunkId = SlaveWorker::dataChunkCount + i; // updatingChunkId
					chunkUpdate.ptr = ( void * ) this->paritySlaveSockets[ i ];

					// Insert into pending set
					if ( ! SlaveWorker::pending->insertChunkUpdate(
						PT_SLAVE_PEER_UPDATE_CHUNK, requestId, event.id,
						( void * ) this->paritySlaveSockets[ i ],
						chunkUpdate
					) ) {
						__ERROR__( "SlaveWorker", "handleUpdateRequest", "Cannot insert into slave UPDATE_CHUNK pending map." );
					}
				}

				// Start sending packets only after all the insertion to the slave peer UPDATE_CHUNK pending set is completed
				for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
					// Prepare UPDATE_CHUNK request
					size_t size;
					Packet *packet = SlaveWorker::packetPool->malloc();
					packet->setReferenceCount( 1 );
					this->protocol.reqUpdateChunk(
						size,
						requestId,
						metadata.listId,
						metadata.stripeId,
						metadata.chunkId,
						offset,
						header.valueUpdateSize,          // length
						SlaveWorker::dataChunkCount + i, // updatingChunkId
						header.valueUpdate,
						packet->data
					);
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
			} else {
				chunk->update( header.valueUpdate, offset, header.valueUpdateSize );
				UNLOCK( keysLock );

				event.resUpdate(
					event.socket, event.id, key,
					header.valueUpdateOffset,
					header.valueUpdateSize,
					true, false, false
				);
				this->dispatch( event );
			}
		} else {
			LOCK( keysLock );
			// The chunk is not yet sealed
			// chunk->update( header.valueUpdate, offset, header.valueUpdateSize );
			// Compute delta and perform update
			chunk->computeDelta(
				header.valueUpdate, // delta
				header.valueUpdate, // new data
				offset, header.valueUpdateSize,
				true // perform update
			);
			UNLOCK( keysLock );

			if ( SlaveWorker::parityChunkCount ) {
				// Add the current request to the pending set
				KeyValueUpdate keyValueUpdate;
				keyValueUpdate.dup( header.keySize, header.key, ( void * ) event.socket );
				keyValueUpdate.offset = header.valueUpdateOffset;
				keyValueUpdate.length = header.valueUpdateSize;

				if ( ! SlaveWorker::pending->insertKeyValueUpdate( PT_MASTER_UPDATE, event.id, ( void * ) event.socket, keyValueUpdate ) ) {
					__ERROR__( "SlaveWorker", "handleUpdateRequest", "Cannot insert into master UPDATE pending map (ID = %u).", event.id );
				}

				// Prepare UPDATE request
				size_t size;
				Packet *packet = SlaveWorker::packetPool->malloc();
				packet->setReferenceCount( SlaveWorker::parityChunkCount );
				this->protocol.reqUpdate(
					size,
					requestId,
					metadata.listId,
					metadata.stripeId,
					metadata.chunkId,
					header.key,
					header.keySize,
					header.valueUpdate,
					header.valueUpdateOffset,
					header.valueUpdateSize,
					offset, // Chunk update offset
					packet->data
				);
				packet->size = ( uint32_t ) size;

				for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
					key.ptr = ( void * ) this->paritySlaveSockets[ i ];
					if ( ! SlaveWorker::pending->insertKeyValueUpdate(
						PT_SLAVE_PEER_UPDATE, requestId, event.id,
						( void * ) this->paritySlaveSockets[ i ],
						keyValueUpdate
					) ) {
						__ERROR__( "SlaveWorker", "handleUpdateRequest", "Cannot insert into slave UPDATE pending map." );
					}
				}

				// Start sending packets only after all the insertion to the slave peer UPDATE pending set is completed
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
				event.resUpdate(
					event.socket, event.id, key,
					header.valueUpdateOffset,
					header.valueUpdateSize,
					true, false, false
				);
				this->dispatch( event );
			}
			// Only unlock chunk when all requests are sent
			chunkBuffer->updateAndUnlockChunk( chunkBufferIndex );
		}

		ret = true;
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
		uint32_t deltaSize;
		char *delta;

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
			uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
			// Find parity slaves
			this->getSlaves(
				header.key, header.keySize,
				metadata.listId, metadata.chunkId
			);
			if ( chunkBufferIndex == -1 ) {
				// Send DELETE_CHUNK requests to parity slaves if the chunk is sealed
				ChunkUpdate chunkUpdate;
				chunkUpdate.set(
					metadata.listId, metadata.stripeId, metadata.chunkId,
					keyMetadata.offset, deltaSize
				);
				chunkUpdate.setKeyValueUpdate( key.size, key.data, keyMetadata.offset );

				for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
					chunkUpdate.chunkId = SlaveWorker::dataChunkCount + i; // updatingChunkId
					chunkUpdate.ptr = ( void * ) this->paritySlaveSockets[ i ];
					if ( ! SlaveWorker::pending->insertChunkUpdate(
						PT_SLAVE_PEER_DEL_CHUNK, requestId, event.id,
						( void * ) this->paritySlaveSockets[ i ],
						chunkUpdate
					) ) {
						__ERROR__( "SlaveWorker", "handleDeleteRequest", "Cannot insert into slave DELETE_CHUNK pending map." );
					}
				}

				// Start sending packets only after all the insertion to the slave peer DELETE_CHUNK pending set is completed
				for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
					// Prepare DELETE_CHUNK request
					size_t size;
					Packet *packet = SlaveWorker::packetPool->malloc();
					packet->setReferenceCount( 1 );
					this->protocol.reqDeleteChunk(
						size,
						requestId,
						metadata.listId,
						metadata.stripeId,
						metadata.chunkId,
						keyMetadata.offset,
						deltaSize,                       // length
						SlaveWorker::dataChunkCount + i, // updatingChunkId
						delta,
						packet->data
					);
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
			} else {
				// Send DELETE request if the chunk is not yet sealed
				// TODO: Handle remapped keys
				Key key;
				key.dup( header.keySize, header.key );

				// Prepare DELETE request
				size_t size;
				Packet *packet = SlaveWorker::packetPool->malloc();
				packet->setReferenceCount( SlaveWorker::parityChunkCount );
				this->protocol.reqDelete(
					size,
					requestId,
					metadata.listId,
					metadata.stripeId,
					metadata.chunkId,
					header.key,
					header.keySize,
					packet->data
				);
				packet->size = ( uint32_t ) size;

				for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
					key.ptr = ( void * ) this->paritySlaveSockets[ i ];
					if ( ! SlaveWorker::pending->insertKey(
						PT_SLAVE_PEER_DEL, requestId, event.id,
						( void * ) this->paritySlaveSockets[ i ],
						key
					) ) {
						__ERROR__( "SlaveWorker", "handleDeleteRequest", "Cannot insert into slave DELETE pending map." );
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
			}
		} else {
			event.resDelete( event.socket, event.id, key, true, false, false );
			this->dispatch( event );
		}
		if ( chunkBufferIndex != -1 )
			chunkBuffer->updateAndUnlockChunk( chunkBufferIndex );
		ret = true;
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

bool SlaveWorker::handleDegradedRequest( MasterEvent event, uint8_t opcode, char *buf, size_t size ) {
	struct DegradedReqHeader header;
	Key key;
	KeyValue keyValue;
	KeyValueUpdate keyValueUpdate;
	DegradedMap *dmap = &SlaveWorker::degradedChunkBuffer->map;
	char *valueUpdate;

	if ( ! this->protocol.parseDegradedReqHeader( header, opcode, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleDegradedRequest", "Invalid degraded request." );
		return false;
	}

	// Check if the chunk is already fetched
	Chunk *chunk = dmap->findChunkById( header.listId, header.stripeId, header.chunkId );
	bool isKeyValueFound = false;
	if ( chunk ) {
		// Retrieve the key-value pair from the reconstructed chunk
		char *keyStr;
		uint8_t keySize;
		if ( opcode == PROTO_OPCODE_UPDATE ) {
			keyStr = header.data.keyValueUpdate.key;
			keySize = header.data.keyValueUpdate.keySize;
		} else {
			keyStr = header.data.key.key;
			keySize = header.data.key.keySize;
		}
		isKeyValueFound = dmap->findValueByKey( keyStr, keySize, &keyValue, &key );
	}

	switch ( opcode ) {
		case PROTO_OPCODE_DEGRADED_GET:
			__DEBUG__(
				BLUE, "SlaveWorker", "handleDegradedRequest",
				"[GET (%u, %u, %u)] Key: %.*s (key size = %u).",
				header.listId, header.stripeId, header.chunkId,
				( int ) header.data.key.keySize,
				header.data.key.key,
				header.data.key.keySize
			);
			key.dup( header.data.key.keySize, header.data.key.key );

			if ( chunk ) {
				if ( isKeyValueFound )
					event.resGet( event.socket, event.id, keyValue, false );
				else
					event.resGet( event.socket, event.id, key, false );
			}
			break;
		case PROTO_OPCODE_DEGRADED_UPDATE:
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

			valueUpdate = new char [ header.data.keyValueUpdate.valueUpdateSize ];
			memcpy( valueUpdate, header.data.keyValueUpdate.valueUpdate, header.data.keyValueUpdate.valueUpdateSize );
			keyValueUpdate.dup( header.data.keyValueUpdate.keySize, header.data.keyValueUpdate.key, valueUpdate );
			keyValueUpdate.offset = header.data.keyValueUpdate.valueUpdateOffset;
			keyValueUpdate.length = header.data.keyValueUpdate.valueUpdateSize;

			printf( "Degraded UPDATE: TODO...\n" );
			if ( chunk ) {
				// if ( isKeyValueFound )
				// 	event.resGet( event.socket, event.id, keyValue, false );
				// else
				// 	event.resGet( event.socket, event.id, key, false );
			}
			break;
		case PROTO_OPCODE_DEGRADED_DELETE:
			__DEBUG__(
				BLUE, "SlaveWorker", "handleDegradedRequest",
				"[DELETE (%u, %u, %u)] Key: %.*s (key size = %u).",
				header.listId, header.stripeId, header.chunkId,
				( int ) header.data.key.keySize,
				header.data.key.key,
				header.data.key.keySize
			);
			key.set( header.data.key.keySize, header.data.key.key );

			printf( "Degraded DELETE: TODO...\n" );
			if ( chunk ) {
				// if ( isKeyValueFound )
				// 	event.resGet( event.socket, event.id, keyValue, false );
				// else
				// 	event.resGet( event.socket, event.id, key, false );
			}
			break;
		default:
			__ERROR__( "SlaveWorker", "handleDegradedRequest", "Invalid opcode for degraded request." );
			return false;
	}

	if ( chunk ) {
		this->dispatch( event );
	} else if ( this->performDegradedRead( event.socket, header.listId, header.stripeId, header.chunkId, header.isSealed, opcode, event.id, &key, &keyValueUpdate ) ) {
		// __ERROR__( "SlaveWorker", "handleDegradedRequest", "Performing degraded read on (%u, %u)...", header.listId, header.stripeId );
	} else {
		__ERROR__( "SlaveWorker", "handleDegradedRequest", "Failed to perform degraded read on (%u, %u).", header.listId, header.stripeId );
		return false;
	}
	return true;
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

	if ( ! chunk ) {
		fprintf( stderr, "chunk == null\n" );
	}

	event.resGetChunk( event.socket, event.id, metadata, ret, chunk );
	this->dispatch( event );

	return ret;
}

bool SlaveWorker::handleSetChunkRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct ChunkDataHeader header;
	bool ret;
	if ( ! this->protocol.parseChunkDataHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleSetChunkRequest", "Invalid SET_CHUNK request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleSetChunkRequest",
		"[SET_CHUNK] List ID: %u; stripe ID: %u; chunk ID: %u; chunk size = %u.",
		header.listId, header.stripeId, header.chunkId, header.size
	);

	Metadata metadata;
	metadata.listId = header.listId;
	metadata.stripeId = header.stripeId;
	metadata.chunkId = header.chunkId;
	__ERROR__( "SlaveWorker", "handleSetChunkRequest", "TODO: SET_CHUNK not yet implemented!" );
	ret = false;

	event.resSetChunk( event.socket, event.id, metadata, ret );
	this->dispatch( event );

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

		masterEvent.resDelete( ( MasterSocket * ) key.ptr, pid.id, key, success, true, false );
		SlaveWorker::eventQueue->insert( masterEvent );
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
	std::unordered_map<PendingIdentifier, ChunkRequest>::iterator it, tmp, end;
	ChunkRequest chunkRequest;

	if ( success ) {
		struct ChunkDataHeader header;
		// Parse header
		if ( ! this->protocol.parseChunkDataHeader( header, buf, size ) ) {
			__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Invalid GET_CHUNK (success) response." );
			return false;
		}
		__DEBUG__(
			BLUE, "SlaveWorker", "handleGetChunkResponse",
			"[GET_CHUNK (success)] List ID: %u, stripe ID: %u, chunk ID: %u; chunk size = %u.",
			header.listId, header.stripeId, header.chunkId, header.size
		);

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
			if ( tmp->second.chunkId == header.chunkId ) {
				// Store the chunk into the buffer
				tmp->second.chunk = SlaveWorker::chunkPool->malloc();
				tmp->second.chunk->loadFromGetChunkRequest(
					header.listId, header.stripeId, header.chunkId,
					header.chunkId >= SlaveWorker::dataChunkCount, // isParity
					header.data, header.size
				);
				this->chunks[ header.chunkId ] = tmp->second.chunk;
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
			}

			for ( uint32_t i = SlaveWorker::dataChunkCount; i < SlaveWorker::chunkCount; i++ ) {
				if ( this->chunks[ i ]->status == CHUNK_STATUS_RECONSTRUCTED ) {
					this->chunks[ i ]->isParity = true;
					this->chunks[ i ]->setSize( maxChunkSize );
				} else if ( this->chunks[ i ]->getSize() != maxChunkSize ) {
					__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Parity chunk size mismatch (chunk ID = %u): %u vs. %u.", i, this->chunks[ i ]->getSize(), maxChunkSize );
				}
			}

			PendingIdentifier pid;
			DegradedOp op;

			// Respond the original GET/UPDATE/DELETE operation using the reconstructed data
			if ( ! SlaveWorker::pending->eraseDegradedOp( PT_SLAVE_PEER_DEGRADED_OPS, event.id, event.socket, &pid, &op ) ) {
				__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot find a pending slave DEGRADED_OPS request that matches the response. This message will be discarded." );
			} else {
				//// TODO ----------------------------------------
				// Find the chunk from the map
				Chunk *chunk = SlaveWorker::map->findChunkById( op.listId, op.stripeId, op.chunkId );
				if ( ! chunk ) {
					this->chunks[ op.chunkId ]->status = CHUNK_STATUS_RECONSTRUCTED;

					chunk = SlaveWorker::chunkPool->malloc();
					chunk->swap( this->chunks[ op.chunkId ] );

					SlaveWorker::map->setChunk(
						op.listId, op.stripeId, op.chunkId, chunk,
						op.chunkId >= SlaveWorker::dataChunkCount
					);
				}
				// Send response
				if ( op.opcode == PROTO_OPCODE_DEGRADED_GET ) {
					MasterEvent event;
					KeyValue keyValue;
					Key &key = op.data.key;

					if ( SlaveWorker::map->findValueByKey( key.data, key.size, &keyValue ) )
						event.resGet( op.socket, pid.parentId, keyValue, true );
					else
						event.resGet( op.socket, pid.parentId, key, true );
					this->dispatch( event );
					key.free();
				} else if ( op.opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
				} else if ( op.opcode == PROTO_OPCODE_DEGRADED_DELETE ) {
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
	} else {
		struct ChunkHeader header;
		if ( ! this->protocol.parseChunkHeader( header, buf, size ) ) {
			__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Invalid GET_CHUNK (failure) response." );
			return false;
		}
		__DEBUG__(
			BLUE, "SlaveWorker", "handleGetChunkResponse",
			"[GET_CHUNK (failure)] List ID: %u, stripe ID: %u, chunk ID: %u.",
			header.listId, header.stripeId, header.chunkId
		);
		chunkRequest.set(
			header.listId, header.stripeId, header.chunkId,
			event.socket, 0 // chunk
		);

		PendingIdentifier pid;

		// Find the corresponding GET_CHUNK request from the pending set
		if ( ! SlaveWorker::pending->eraseChunkRequest( PT_SLAVE_PEER_GET_CHUNK, event.id, event.socket ) ) {
			__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot find a pending slave GET_CHUNK request that matches the response. This message will be discarded." );
		}

		return false;
	}
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

	ChunkRequest chunkRequest;

	chunkRequest.set(
		header.listId, header.stripeId, header.chunkId,
		event.socket, 0 // ptr
	);

	if ( ! SlaveWorker::pending->eraseChunkRequest( PT_SLAVE_PEER_SET_CHUNK, event.id, event.socket ) ) {
		__ERROR__( "SlaveWorker", "handleSetChunkResponse", "Cannot find a pending slave SET_CHUNK request that matches the response. This message will be discarded." );
	}

	// TODO: What should we do next?

	return true;
}

bool SlaveWorker::performDegradedRead( MasterSocket *masterSocket, uint32_t listId, uint32_t stripeId, uint32_t lostChunkId, bool isSealed, uint8_t opcode, uint32_t parentId, Key *key, KeyValueUpdate *keyValueUpdate ) {
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
	if ( opcode == PROTO_OPCODE_DEGRADED_UPDATE )
		op.data.keyValueUpdate = *keyValueUpdate;
	else
		op.data.key = *key;
	if ( ! SlaveWorker::pending->insertDegradedOp( PT_SLAVE_PEER_DEGRADED_OPS, requestId, parentId, 0, op ) ) {
		__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave DEGRADED_OPS pending map." );
	}

	if ( isSealed ) {
		// Send GET_CHUNK requests to surviving nodes
		Metadata metadata;
		metadata.set( listId, stripeId, 0 );
		selected = 0;
		for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
		// for ( uint32_t i = SlaveWorker::chunkCount - 1; i >= 0; i-- ) {
			if ( selected >= SlaveWorker::dataChunkCount )
				break;
			if ( i == lostChunkId )
				continue;

			socket = ( i < SlaveWorker::dataChunkCount ) ?
			         ( this->dataSlaveSockets[ i ] ) :
			         ( this->paritySlaveSockets[ i - SlaveWorker::dataChunkCount ] );

			// Add to pending GET_CHUNK request set
			ChunkRequest chunkRequest;
			chunkRequest.set( listId, stripeId, i, socket );
			if ( socket->self ) {
				chunkRequest.chunk = SlaveWorker::map->findChunkById( listId,
				 stripeId, i );

				if ( ! SlaveWorker::pending->insertChunkRequest( PT_SLAVE_PEER_GET_CHUNK, requestId, parentId, socket, chunkRequest ) ) {
					__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave CHUNK_REQUEST pending map." );
				}
			} else if ( socket->ready() ) {
				chunkRequest.chunk = 0;

				if ( ! SlaveWorker::pending->insertChunkRequest( PT_SLAVE_PEER_GET_CHUNK, requestId, parentId, socket, chunkRequest ) ) {
					__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave CHUNK_REQUEST pending map." );
				}

				event.reqGetChunk( socket, requestId, metadata );
				SlaveWorker::eventQueue->insert( event );
			} else {
				continue;
			}
			selected++;
		}

		return ( selected >= SlaveWorker::dataChunkCount );
	} else {
		if ( ! SlaveWorker::pending->insertKey( PT_SLAVE_PEER_GET, requestId, parentId, socket, op.data.key ) ) {
			__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave GET pending map." );
		}
		event.reqGet( socket, requestId, listId, lostChunkId, op.data.key );

		printf( "TODO: Retrieve key-value pairs from unsealed chunks.\n" );
		return false;
	}
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
