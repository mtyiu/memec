#include "worker.hh"
#include "../main/slave.hh"
#include "../../common/util/debug.hh"
#include "../../common/util/time.hh"

#define WORKER_COLOR	YELLOW

uint32_t SlaveWorker::dataChunkCount;
uint32_t SlaveWorker::parityChunkCount;
Pending *SlaveWorker::pending;
ServerAddr *SlaveWorker::slaveServerAddr;
Coding *SlaveWorker::coding;
SlaveEventQueue *SlaveWorker::eventQueue;
StripeList<SlavePeerSocket> *SlaveWorker::stripeList;
Map *SlaveWorker::map;
MemoryPool<Chunk> *SlaveWorker::chunkPool;
MemoryPool<Stripe> *SlaveWorker::stripePool;
std::vector<MixedChunkBuffer *> *SlaveWorker::chunkBuffer;

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
	Chunk **dataChunks, *parityChunk;
	uint32_t parityChunkId;

	switch( event.type ) {
		case CODING_EVENT_TYPE_ENCODE:
			__ERROR__( "SlaveWorker", "dispatch", "Received an CODING_EVENT_TYPE_ENCODE event." );
			parityChunkId = event.message.stripe->get( dataChunks, parityChunk );
			SlaveWorker::coding->encode( dataChunks, parityChunk, parityChunkId );

			// Release memory for data chunks
			for ( uint32_t i = 0; i < Stripe::dataChunkCount; i++ ) {
				SlaveWorker::chunkPool->free( dataChunks[ i ] );
			}

			// Release memory for stripe
			SlaveWorker::stripePool->free( event.message.stripe );

			// Append a flush event to the event queue
			IOEvent ioEvent;
			ioEvent.flush( parityChunk );
			SlaveWorker::eventQueue->insert( ioEvent );
			break;
		default:
			return;
	}
}

void SlaveWorker::dispatch( CoordinatorEvent event ) {
	bool connected, isSend;
	ssize_t ret;
	size_t count;
	struct {
		size_t size;
		char *data;
	} buffer;
	struct timespec t = start_timer();

	switch( event.type ) {
		case COORDINATOR_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterCoordinator( buffer.size );
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_SYNC:
			buffer.data = this->protocol.sendHeartbeat(
				buffer.size,
				Slave::getInstance()->aggregateLoad().ops,
				SlaveWorker::map->ops,
				count
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

		if ( ret > 0 ) {
			this->load.sentBytes += ret;
			this->load.elapsedTime += get_elapsed_time( t );
		}

		if ( event.type == COORDINATOR_EVENT_TYPE_SYNC && SlaveWorker::map->ops.size() ) {
			// Some metadata is not sent yet, continue to send
			SlaveWorker::eventQueue->insert( event );
		}
	} else {
		ProtocolHeader header;
		ret = event.socket->recv(
			this->protocol.buffer.recv,
			this->protocol.buffer.size,
			connected,
			false
		);
		buffer.data = this->protocol.buffer.recv;
		buffer.size = ret > 0 ? ( size_t ) ret : 0;
		if ( ret > 0 ) {
			this->load.recvBytes += ret;
			this->load.elapsedTime += get_elapsed_time( t );
		}
		while ( buffer.size > 0 ) {
			if ( ! this->protocol.parseHeader( header, buffer.data, buffer.size ) ) {
				__ERROR__( "SlaveWorker", "dispatch", "Undefined message (remaining bytes = %lu).", buffer.size );
				break;
			}

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_COORDINATOR ) {
				__ERROR__( "SlaveWorker", "dispatch", "Invalid message source from coordinator." );
				continue;
			}
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
				default:
					__ERROR__( "SlaveWorker", "dispatch", "Invalid opcode from coordinator." );
					return;
			}
			buffer.data += PROTO_HEADER_SIZE + header.length;
			buffer.size -= PROTO_HEADER_SIZE + header.length;
		}
	}
	if ( ! connected )
		__ERROR__( "SlaveWorker", "dispatch", "The coordinator is disconnected." );
}

void SlaveWorker::dispatch( IOEvent event ) {
	__ERROR__( "SlaveWorker", "dispatch", "Received an I/O event." );
	switch( event.type ) {
		case IO_EVENT_TYPE_FLUSH_CHUNK:
			this->storage->write(
				event.message.chunk,
				true
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
	struct timespec t = start_timer();

	switch( event.type ) {
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_GET_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
			success = true;
			break;
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_GET_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
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
			buffer.data = this->protocol.resRegisterMaster( buffer.size, success );
			break;
		// GET
		case MASTER_EVENT_TYPE_GET_RESPONSE_SUCCESS:
		{
			char *key, *value;
			uint8_t keySize;
			uint32_t valueSize;

			event.message.keyValue.deserialize( key, keySize, value, valueSize );
			buffer.data = this->protocol.resGet( buffer.size, success, keySize, key, valueSize, value );
		}
			break;
		case MASTER_EVENT_TYPE_GET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resGet(
				buffer.size, success,
				event.message.key.size,
				event.message.key.data
			);
			break;
		// SET
		case MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resSet(
				buffer.size, success,
				event.message.key.size,
				event.message.key.data
			);
			break;
		// UPDATE
		case MASTER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
			buffer.data = this->protocol.resUpdate(
				buffer.size, success,
				event.message.keyValueUpdate.key.size,
				event.message.keyValueUpdate.key.data,
				event.message.keyValueUpdate.valueUpdateOffset,
				event.message.keyValueUpdate.valueUpdateSize
			);
			break;
		// DELETE
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
			buffer.data = this->protocol.resDelete(
				buffer.size, success,
				event.message.key.size,
				event.message.key.data
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

		if ( ret > 0 ) {
			this->load.sentBytes += ret;
			this->load.elapsedTime += get_elapsed_time( t );
		}
	} else {
		// Parse requests from masters
		ProtocolHeader header;
		ret = event.socket->recv(
			this->protocol.buffer.recv,
			this->protocol.buffer.size,
			connected,
			false
		);
		buffer.data = this->protocol.buffer.recv;
		buffer.size = ret > 0 ? ( size_t ) ret : 0;
		if ( ret > 0 ) {
			this->load.recvBytes += ret;
			this->load.elapsedTime += get_elapsed_time( t );
		}
		while ( buffer.size > 0 ) {
			if ( ! this->protocol.parseHeader( header, buffer.data, buffer.size ) ) {
				__ERROR__( "SlaveWorker", "dispatch", "Undefined message (remaining bytes = %lu).", buffer.size );
				break;
			}

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if (
				header.magic != PROTO_MAGIC_REQUEST ||
				header.from != PROTO_MAGIC_FROM_MASTER
			) {
				__ERROR__( "SlaveWorker", "dispatch", "Invalid protocol header." );
			}

			switch( header.opcode ) {
				case PROTO_OPCODE_GET:
					this->handleGetRequest( event, buffer.data, buffer.size );
					break;
				case PROTO_OPCODE_SET:
					this->handleSetRequest( event, buffer.data, buffer.size );
					break;
				case PROTO_OPCODE_UPDATE:
					this->handleUpdateRequest( event, buffer.data, buffer.size );
					break;
				case PROTO_OPCODE_DELETE:
					this->handleDeleteRequest( event, buffer.data, buffer.size );
					break;
				default:
					__ERROR__( "SlaveWorker", "dispatch", "Invalid opcode from master." );
					return;
			}
			buffer.data += header.length;
			buffer.size -= header.length;
		}
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
	struct timespec t = start_timer();

	isSend = ( event.type != SLAVE_PEER_EVENT_TYPE_PENDING );
	success = false;
	switch( event.type ) {
		//////////////
		// Requests //
		//////////////
		case SLAVE_PEER_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterSlavePeer( buffer.size, SlaveWorker::slaveServerAddr );
			break;
		case SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_REQUEST:
			buffer.data = this->protocol.reqUpdateChunk(
				buffer.size,
				event.message.chunkUpdate.metadata.listId,
				event.message.chunkUpdate.metadata.stripeId,
				event.message.chunkUpdate.metadata.chunkId,
				event.message.chunkUpdate.offset,
				event.message.chunkUpdate.length,
				event.message.chunkUpdate.updatingChunkId,
				event.message.chunkUpdate.delta
			);
			break;
		case SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_REQUEST:
			buffer.data = this->protocol.reqDeleteChunk(
				buffer.size,
				event.message.chunkUpdate.metadata.listId,
				event.message.chunkUpdate.metadata.stripeId,
				event.message.chunkUpdate.metadata.chunkId,
				event.message.chunkUpdate.offset,
				event.message.chunkUpdate.length,
				event.message.chunkUpdate.updatingChunkId,
				event.message.chunkUpdate.delta
			);
			break;
		case SLAVE_PEER_EVENT_TYPE_GET_CHUNK_REQUEST:
			buffer.data = this->protocol.reqGetChunk(
				buffer.size,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId
			);
			break;
		case SLAVE_PEER_EVENT_TYPE_SET_CHUNK_REQUEST:
			buffer.data = this->protocol.reqSetChunk(
				buffer.size,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId,
				event.message.chunk.chunk->size,
				event.message.chunk.chunk->data
			);
			break;
		///////////////
		// Responses //
		///////////////
		// Register
		case SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
			success = true; // default is false
		case SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRegisterSlavePeer( buffer.size, success );
			break;
		// UPDATE_CHUNK
		case SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS:
			success = true; // default is false
		case SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE:
			buffer.data = this->protocol.resUpdateChunk(
				buffer.size, success,
				event.message.chunkUpdate.metadata.listId,
				event.message.chunkUpdate.metadata.stripeId,
				event.message.chunkUpdate.metadata.chunkId,
				event.message.chunkUpdate.offset,
				event.message.chunkUpdate.length,
				event.message.chunkUpdate.updatingChunkId
			);
			break;
		// DELETE_CHUNK
		case SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS:
			success = true; // default is false
		case SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE:
			buffer.data = this->protocol.resDeleteChunk(
				buffer.size, success,
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
			success = true; // default is false
		case SLAVE_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_FAILURE:
			buffer.data = this->protocol.resGetChunk(
				buffer.size, success,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId,
				event.message.chunk.chunk->size,
				event.message.chunk.chunk->data
			);
			break;
		// SET_CHUNK
		case SLAVE_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_SUCCESS:
			success = true; // default is false
		case SLAVE_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_FAILURE:
			buffer.data = this->protocol.resSetChunk(
				buffer.size, success,
				event.message.chunk.metadata.listId,
				event.message.chunk.metadata.stripeId,
				event.message.chunk.metadata.chunkId
			);
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

		if ( ret > 0 ) {
			this->load.sentBytes += ret;
			this->load.elapsedTime += get_elapsed_time( t );
		}

		if ( event.type == SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_REQUEST ||
		     event.type == SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_REQUEST )
			*event.message.chunkUpdate.status = false;
	} else {
		ProtocolHeader header;
		ret = event.socket->recv(
			this->protocol.buffer.recv,
			this->protocol.buffer.size,
			connected,
			false
		);
		if ( ret > 0 ) {
			this->load.recvBytes += ret;
			this->load.elapsedTime += get_elapsed_time( t );
		}
		buffer.data = this->protocol.buffer.recv;
		buffer.size = ret > 0 ? ( size_t ) ret : 0;
		while ( buffer.size > 0 ) {
			if ( ! this->protocol.parseHeader( header, buffer.data, buffer.size ) ) {
				__ERROR__( "SlaveWorker", "dispatch", "Undefined message (remaining bytes = %lu).", buffer.size );
				break;
			}

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_SLAVE ) {
				__ERROR__( "SlaveWorker", "dispatch", "Invalid protocol header." );
			}

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
				case PROTO_OPCODE_UPDATE_CHUNK:
					switch( header.magic ) {
						case PROTO_MAGIC_REQUEST:
							fprintf( stderr, "handleUpdateChunkRequest\n" );
							this->handleUpdateChunkRequest( event, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							fprintf( stderr, "handleUpdateChunkResponse\n" );
							this->handleUpdateChunkResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							fprintf( stderr, "handleUpdateChunkResponse\n" );
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
							fprintf( stderr, "handleDeleteChunkRequest\n" );
							this->handleDeleteChunkRequest( event, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							fprintf( stderr, "handleDeleteChunkResponse\n" );
							this->handleDeleteChunkResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							fprintf( stderr, "handleDeleteChunkResponse\n" );
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
							fprintf( stderr, "handleGetChunkRequest\n" );
							this->handleGetChunkRequest( event, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							fprintf( stderr, "handleGetChunkResponse\n" );
							this->handleGetChunkResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							fprintf( stderr, "handleGetChunkResponse\n" );
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
							fprintf( stderr, "handleSetChunkRequest\n" );
							this->handleSetChunkRequest( event, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							fprintf( stderr, "handleSetChunkResponse\n" );
							this->handleSetChunkResponse( event, true, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							fprintf( stderr, "handleSetChunkResponse\n" );
							this->handleSetChunkResponse( event, false, buffer.data, buffer.size );
							break;
						default:
							__ERROR__( "SlaveWorker", "dispatch", "Invalid magic code from slave: 0x%x.", header.magic );
							break;
					}
					break;
				default:
					__ERROR__( "SlaveWorker", "dispatch", "Invalid opcode from slave: 0x%x.", header.opcode );
					return;
			}
			buffer.data += header.length;
			buffer.size -= header.length;
		}
	}
	if ( ! connected )
		__ERROR__( "SlaveWorker", "dispatch", "The slave is disconnected." );
}

bool SlaveWorker::isRedirectedRequest( char *key, uint8_t size, bool *isParity, uint32_t *listIdPtr, uint32_t *chunkIdPtr ) {
	SlavePeerSocket *target;
	uint32_t listId, chunkId;
	listId = SlaveWorker::stripeList->get( key, size, &target, isParity ? this->paritySlaveSockets : 0, &chunkId );
	if ( listIdPtr ) *listIdPtr = listId;
	if ( chunkIdPtr ) *chunkIdPtr = chunkId;
	if ( ! target->self ) {
		if ( isParity ) {
			for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
				if ( this->paritySlaveSockets[ i ]->self ) {
					*isParity = true;
					return false;
				}
			}
			*isParity = false;
			return false;
		}
		return false;
	}
	if ( isParity ) *isParity = false;
	return true;
}

bool SlaveWorker::isRedirectedRequest( uint32_t listId, uint32_t updatingChunkId ) {
	SlaveWorker::stripeList->get( listId, this->paritySlaveSockets );
	for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ )
		if ( this->paritySlaveSockets[ i ]->self )
			return updatingChunkId == i + SlaveWorker::dataChunkCount;
	return true;
}

SlavePeerSocket *SlaveWorker::getSlave( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId, bool allowDegraded, bool *isDegraded ) {
	SlavePeerSocket *ret;
	listId = SlaveWorker::stripeList->get(
		data, ( size_t ) size,
		this->dataSlaveSockets,
		this->paritySlaveSockets,
		&chunkId, false
	);

	ret = *this->dataSlaveSockets;

	if ( isDegraded )
		*isDegraded = ( ! ret->ready() && allowDegraded );

	if ( ret->ready() )
		return ret;

	if ( allowDegraded ) {
		for ( uint32_t i = 0; i < SlaveWorker::dataChunkCount + SlaveWorker::parityChunkCount; i++ ) {
			ret = SlaveWorker::stripeList->get( listId, chunkId, i );
			if ( ret->ready() )
				return ret;
		}
		__ERROR__( "SlaveWorker", "getSlave", "Cannot find a slave for performing degraded operation." );
		return 0;
	}

	return 0;
}

SlavePeerSocket *SlaveWorker::getSlaves( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId, bool allowDegraded, bool *isDegraded ) {
	SlavePeerSocket *ret = this->getSlave( data, size, listId, chunkId, allowDegraded, isDegraded );

	if ( isDegraded ) *isDegraded = false;
	for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
		if ( ! this->paritySlaveSockets[ i ]->ready() ) {
			if ( ! allowDegraded )
				return 0;
			if ( isDegraded ) *isDegraded = true;

			for ( uint32_t i = 0; i < SlaveWorker::dataChunkCount + SlaveWorker::parityChunkCount; i++ ) {
				SlavePeerSocket *s = SlaveWorker::stripeList->get( listId, chunkId, i );
				if ( s->ready() ) {
					this->paritySlaveSockets[ i ] = s;
					break;
				} else if ( i == SlaveWorker::dataChunkCount + SlaveWorker::parityChunkCount - 1 ) {
					__ERROR__( "SlaveWorker", "getSlave", "Cannot find a slave for performing degraded operation." );
					return 0;
				}
			}
		}
	}
	return ret;
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

	// Detect degraded GET
	if ( ! this->isRedirectedRequest( header.key, header.keySize ) ) {
		__DEBUG__( YELLOW, "SlaveWorker", "handleGetRequest", "!!! Degraded GET request [not yet implemented] !!!" );
		// TODO
	}

	Key key;
	KeyValue keyValue;
	if ( map->findValueByKey( header.key, header.keySize, &keyValue, &key ) ) {
		event.resGet( event.socket, keyValue );
		ret = true;
	} else {
		event.resGet( event.socket, key );
		ret = false;
	}

	this->load.get();
	this->dispatch( event );

	return ret;
}

bool SlaveWorker::handleSetRequest( MasterEvent event, char *buf, size_t size ) {
	struct KeyValueHeader header;
	if ( ! this->protocol.parseKeyValueHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleSetRequest", "Invalid SET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleSetRequest",
		"[SET] Key: %.*s (key size = %u); Value: (value size = %u)",
		( int ) header.keySize, header.key, header.keySize, header.valueSize
	);

	// Detect degraded SET
	bool isParity;
	uint32_t listId, chunkId;
	if ( ! this->isRedirectedRequest( header.key, header.keySize, &isParity, &listId, &chunkId ) && ! isParity ) {
		__DEBUG__( YELLOW, "SlaveWorker", "handleSetRequest", "!!! Degraded SET request [not yet implemented] !!!" );
		// TODO
	}

	Key key;
	KeyMetadata keyMetadata = SlaveWorker::chunkBuffer
		->at( listId )
		->set( header.key, header.keySize, header.value, header.valueSize,
		       isParity, chunkId );

	key.set( header.keySize, header.key );
	if ( ! isParity )
		map->insertKey( key, PROTO_OPCODE_SET, keyMetadata );

	event.resSet( event.socket, key, true );
	this->load.set();
	this->dispatch( event );

	return true;
}

bool SlaveWorker::handleUpdateRequest( MasterEvent event, char *buf, size_t size ) {
	struct KeyValueUpdateHeader header;
	bool ret;
	if ( ! this->protocol.parseKeyValueUpdateHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleUpdateRequest", "Invalid UPDATE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleUpdateRequest",
		"[UPDATE] Key: %.*s (key size = %u); Value: (update size = %u, offset = %u).",
		( int ) header.keySize, header.key, header.keySize,
		header.valueUpdateSize, header.valueUpdateOffset
	);

	// Detect degraded UPDATE
	if ( ! this->isRedirectedRequest( header.key, header.keySize ) ) {
		__DEBUG__( YELLOW, "SlaveWorker", "handleUpdateRequest", "!!! Degraded UPDATE request [not yet implemented] !!!" );
		// TODO
	}

	Key key;
	KeyValue keyValue;
	KeyMetadata keyMetadata;
	Metadata metadata;
	Chunk *chunk;
	if ( map->findValueByKey( header.key, header.keySize, &keyValue, &key, &keyMetadata, &metadata, &chunk ) ) {
		uint32_t offset = keyMetadata.offset + PROTO_KEY_VALUE_SIZE + header.keySize + header.valueUpdateOffset;
#ifndef SLAVE_WORKER_SEND_REPLICAS_PARALLEL
		ssize_t sentBytes;
		bool connected;
#endif
		bool isDegraded;
		KeyValueUpdate keyValueUpdate;
		ChunkUpdate chunkUpdate;

		// Add the current request to the pending set
		keyValueUpdate.dup( header.keySize, header.key, ( void * ) event.socket );
		keyValueUpdate.offset = header.valueUpdateOffset;
		keyValueUpdate.length = header.valueUpdateSize;
		SlaveWorker::pending->masters.update.insert( keyValueUpdate );

		// Compute delta and perform update
		chunk->computeDelta(
			header.valueUpdate, // delta
			header.valueUpdate, // new data
			offset, header.valueUpdateSize,
			true // perform update
		);
		chunkUpdate.set(
			metadata.listId, metadata.stripeId, metadata.chunkId,
			offset, header.valueUpdateSize
		);
		chunkUpdate.setKeyValueUpdate( keyValueUpdate.size, keyValueUpdate.data, header.valueUpdateOffset );

		// Find parity slaves
		this->getSlaves(
			header.key, header.keySize,
			metadata.listId, metadata.chunkId,
			true, &isDegraded
		);

		// Send UPDATE_CHUNK requests to parity slaves
		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			key.ptr = ( void * ) this->paritySlaveSockets[ i ];
			SlaveWorker::pending->slavePeers.updateChunk.insert( chunkUpdate );

			SlavePeerEvent slavePeerEvent;
			slavePeerEvent.reqUpdateChunk(
				this->paritySlaveSockets[ i ],
				metadata, offset, header.valueUpdateSize,
				SlaveWorker::dataChunkCount + i, this->protocol.status + i,
				header.valueUpdate
			);

#ifdef SLAVE_WORKER_SEND_REPLICAS_PARALLEL
			this->protocol.status[ i ] = true;
			SlaveWorker::eventQueue->insert( slavePeerEvent );
#else
			this->dispatch( slavePeerEvent );
#endif
		}

#ifdef SLAVE_WORKER_SEND_REPLICAS_PARALLEL
		// Wait until all replicas are sent
		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			while( this->protocol.status[ i ] ); // Busy waiting
		}
#endif
		ret = true;
	} else {
		event.resUpdate( event.socket, key, header.valueUpdateOffset, header.valueUpdateSize, false );
		ret = false;
	}

	this->load.update();
	this->dispatch( event );

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

	// Detect degraded DELETE
	if ( ! this->isRedirectedRequest( header.key, header.keySize ) ) {
		__DEBUG__( YELLOW, "SlaveWorker", "handleDeleteRequest", "!!! Degraded DELETE request [not yet implemented] !!!" );
		// TODO
	}

	Key key;
	KeyValue keyValue;
	KeyMetadata keyMetadata;
	Metadata metadata;
	Chunk *chunk;

	key.set( header.keySize, header.key );
	if ( map->findValueByKey( header.key, header.keySize, &keyValue, 0, &keyMetadata, &metadata, &chunk ) ) {
		uint32_t deltaSize;
		char *delta;
#ifndef SLAVE_WORKER_SEND_REPLICAS_PARALLEL
		ssize_t sentBytes;
		bool connected;
#endif
		bool isDegraded;
		ChunkUpdate chunkUpdate;

		// Add the current request to the pending set
		key.dup( header.keySize, header.key, ( void * ) event.socket );
		SlaveWorker::pending->masters.del.insert( key );

		// Update data chunk and map
		delta = this->protocol.buffer.recv + PROTO_KEY_SIZE + key.size;
		key.ptr = 0;
		deltaSize = chunk->deleteKeyValue(
			key, &map->keys, delta,
			this->protocol.buffer.size - PROTO_KEY_SIZE - key.size
		);
		chunkUpdate.set(
			metadata.listId, metadata.stripeId, metadata.chunkId,
			keyMetadata.offset, deltaSize
		);
		chunkUpdate.setKeyValueUpdate( key.size, key.data, keyMetadata.offset );

		// Find parity slaves
		this->getSlaves(
			header.key, header.keySize,
			metadata.listId, metadata.chunkId,
			true, &isDegraded
		);

		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			key.ptr = ( void * ) this->paritySlaveSockets[ i ];
			SlaveWorker::pending->slavePeers.deleteChunk.insert( chunkUpdate );

			SlavePeerEvent slavePeerEvent;
			slavePeerEvent.reqDeleteChunk(
				this->paritySlaveSockets[ i ],
				metadata, keyMetadata.offset, deltaSize,
				SlaveWorker::dataChunkCount + i, this->protocol.status + i,
				delta
			);
#ifdef SLAVE_WORKER_SEND_REPLICAS_PARALLEL
			this->protocol.status[ i ] = true;
			SlaveWorker::eventQueue->insert( slavePeerEvent );
#else
			this->dispatch( slavePeerEvent );
#endif
		}

#ifdef SLAVE_WORKER_SEND_REPLICAS_PARALLEL
		// Wait until all replicas are sent
		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			while( this->protocol.status[ i ] ); // Busy waiting
		}
#endif
		ret = true;
	} else {
		ret = false;
		event.resDelete( event.socket, key, false );
		this->dispatch( event );
	}

	this->load.del();

	return ret;
}

bool SlaveWorker::handleSlavePeerRegisterRequest( SlavePeerSocket *socket, char *buf, size_t size ) {
	static Slave *slave = Slave::getInstance();
	SlavePeerSocket *s = 0;
	ServerAddr serverAddr;

	// Find the corresponding SlavePeerSocket
	serverAddr.deserialize( buf );
	for ( int i = 0, len = slave->sockets.slavePeers.size(); i < len; i++ ) {
		if ( slave->sockets.slavePeers[ i ].isMatched( serverAddr ) ) {
			s = &slave->sockets.slavePeers[ i ];
			break;
		}
	}

	if ( s ) {
		SlavePeerEvent event;
		event.resRegister( s, true );
		SlaveWorker::eventQueue->insert( event );
	}
	return true;
}

bool SlaveWorker::handleUpdateChunkRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct ChunkUpdateHeader header;
	bool ret;
	if ( ! this->protocol.parseChunkUpdateHeader( header, buf, size ) ) {
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

	// Detect degraded UPDATE_CHUNK
	if ( ! this->isRedirectedRequest( header.listId, header.updatingChunkId ) ) {
		__DEBUG__( YELLOW, "SlaveWorker", "handleUpdateChunkRequest", "!!! Degraded UPDATE_CHUNK request [not yet implemented] !!!" );
		// TODO
	}

	Metadata metadata;
	Chunk *chunk = map->findChunkById( header.listId, header.stripeId, header.chunkId, &metadata );
	if ( chunk ) {
		__ERROR__( "SlaveWorker", "handleUpdateChunkRequest", "TODO: UPDATE_CHUNK not yet implemented!" );
		ret = true;
	} else {
		ret = false;
	}

	event.resUpdateChunk(
		event.socket, metadata,
		header.offset, header.length,
		header.updatingChunkId, ret
	);

	this->load.updateChunk();
	this->dispatch( event );

	return ret;
}

bool SlaveWorker::handleDeleteChunkRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct ChunkUpdateHeader header;
	bool ret;
	if ( ! this->protocol.parseChunkUpdateHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleDeleteChunkRequest", "Invalid DELETE_CHUNK request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleDeleteChunkRequest",
		"[DELETE_CHUNK] List ID: %u; stripe ID: %u; chunk ID: %u; offset: %u; length: %u; updating chunk ID: %u.",
		header.listId, header.stripeId, header.chunkId,
		header.offset, header.length, header.updatingChunkId
	);

	// Detect degraded UPDATE_CHUNK
	if ( ! this->isRedirectedRequest( header.listId, header.updatingChunkId ) ) {
		__DEBUG__( YELLOW, "SlaveWorker", "handleDeleteChunkRequest", "!!! Degraded DELETE_CHUNK request [not yet implemented] !!!" );
		// TODO
	}

	Metadata metadata;
	Chunk *chunk = map->findChunkById( header.listId, header.stripeId, header.chunkId, &metadata );
	if ( chunk ) {
		__ERROR__( "SlaveWorker", "handleDeleteChunkRequest", "TODO: DELETE_CHUNK not yet implemented!" );
		ret = true;
	} else {
		ret = false;
	}

	metadata.chunkId = header.chunkId;
	event.resDeleteChunk(
		event.socket, metadata,
		header.offset, header.length,
		header.updatingChunkId, ret
	);

	this->load.delChunk();
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
	if ( chunk ) {
		__ERROR__( "SlaveWorker", "handleGetChunkRequest", "TODO: GET_CHUNK not yet implemented!" );
		ret = true;
	} else {
		ret = false;
	}

	event.resGetChunk( event.socket, metadata, ret, chunk );
	this->load.getChunk();
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

	event.resSetChunk( event.socket, metadata, ret );
	this->load.setChunk();
	this->dispatch( event );

	return ret;
}

bool SlaveWorker::handleUpdateChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	struct ChunkUpdateHeader header;
	if ( ! this->protocol.parseChunkUpdateHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleUpdateChunkResponse", "Invalid UPDATE_CHUNK response." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleUpdateChunkResponse",
		"[UPDATE_CHUNK] List ID: %u, stripe ID: %u, chunk ID: %u; offset = %u, length = %u.",
		header.listId, header.stripeId, header.chunkId,
		header.offset, header.length
	);

	std::set<ChunkUpdate>::iterator it;
	ChunkUpdate chunkUpdate;
	int pending;

	chunkUpdate.set(
		header.listId, header.stripeId, header.chunkId,
		header.offset, header.length
	);
	chunkUpdate.setKeyValueUpdate( 0, 0, 0 );
	chunkUpdate.ptr = ( void * ) event.socket;

	it = SlaveWorker::pending->slavePeers.updateChunk.lower_bound( chunkUpdate );
	if ( it == SlaveWorker::pending->slavePeers.updateChunk.end() || ! chunkUpdate.equal( *it ) ) {
		__ERROR__( "SlaveWorker", "handleUpdateChunkResponse", "Cannot find a pending slave UPDATE_CHUNK request that matches the response. This message will be discarded." );
		return false;
	}
	chunkUpdate = *it;
	SlaveWorker::pending->slavePeers.updateChunk.erase( it );

	// Check pending slave UPDATE_CHUNK requests
	chunkUpdate.ptr = 0;
	it = SlaveWorker::pending->slavePeers.updateChunk.lower_bound( chunkUpdate );
	for ( pending = 0; it != SlaveWorker::pending->slavePeers.updateChunk.end() && chunkUpdate.equal( *it ); pending++, it++ );
	__ERROR__( "SlaveWorker", "handleUpdateChunkResponse", "Pending slave UPDATE_CHUNK requests = %d (%s).", pending, success ? "success" : "fail" );
	if ( pending == 0 ) {
		// Only send application UPDATE response when the number of pending slave UPDATE_CHUNK requests equal 0
		MasterEvent masterEvent;
		std::set<KeyValueUpdate>::iterator it;
		KeyValueUpdate keyValueUpdate;
		Key key;

		keyValueUpdate.set( chunkUpdate.keySize, chunkUpdate.key, 0 );
		keyValueUpdate.offset = chunkUpdate.valueUpdateOffset;
		keyValueUpdate.length = chunkUpdate.length;
		it = SlaveWorker::pending->masters.update.lower_bound( keyValueUpdate );
		if ( it == SlaveWorker::pending->masters.update.end() || ! keyValueUpdate.equal( *it ) ) {
			__ERROR__( "SlaveWorker", "handleUpdateChunkResponse", "Cannot find a pending master UPDATE request that matches the response. This message will be discarded." );
			return false;
		}
		keyValueUpdate = *it;

		key.set( keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.ptr );
		masterEvent.resUpdate(
			( MasterSocket * ) keyValueUpdate.ptr, key,
			keyValueUpdate.offset, keyValueUpdate.length, success
		);
		SlaveWorker::eventQueue->insert( masterEvent );
	}
	return true;
}

bool SlaveWorker::handleDeleteChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	struct ChunkUpdateHeader header;
	if ( ! this->protocol.parseChunkUpdateHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Invalid DELETE_CHUNK response." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleDeleteChunkResponse",
		"[DELETE_CHUNK] List ID: %u, stripe ID: %u, chunk ID: %u; offset = %u, length = %u.",
		header.listId, header.stripeId, header.chunkId,
		header.offset, header.length
	);

	std::set<ChunkUpdate>::iterator it;
	ChunkUpdate chunkUpdate;
	int pending;

	chunkUpdate.set(
		header.listId, header.stripeId, header.chunkId,
		header.offset, header.length
	);
	chunkUpdate.setKeyValueUpdate( 0, 0, 0 );
	chunkUpdate.ptr = ( void * ) event.socket;

	it = SlaveWorker::pending->slavePeers.deleteChunk.lower_bound( chunkUpdate );
	if ( it == SlaveWorker::pending->slavePeers.deleteChunk.end() || ! chunkUpdate.equal( *it ) ) {
		__ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Cannot find a pending slave DELETE_CHUNK request that matches the response. This message will be discarded." );
		return false;
	}
	chunkUpdate = *it;
	SlaveWorker::pending->slavePeers.deleteChunk.erase( it );

	// Check pending slave DELETE_CHUNK requests
	chunkUpdate.ptr = 0;
	it = SlaveWorker::pending->slavePeers.deleteChunk.lower_bound( chunkUpdate );
	for ( pending = 0; it != SlaveWorker::pending->slavePeers.deleteChunk.end() && chunkUpdate.equal( *it ); pending++, it++ );
	__ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Pending slave DELETE_CHUNK requests = %d.", pending );
	if ( pending == 0 ) {
		// Only send application DELETE response when the number of pending slave DELETE_CHUNK requests equal 0
		MasterEvent masterEvent;
		std::set<Key>::iterator it;
		Key key;

		key.set( chunkUpdate.keySize, chunkUpdate.key, 0 );
		it = SlaveWorker::pending->masters.del.lower_bound( key );
		if ( it == SlaveWorker::pending->masters.del.end() || ! key.equal( *it ) ) {
			__ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Cannot find a pending master DELETE request that matches the response. This message will be discarded." );
			return false;
		}
		key = *it;
		masterEvent.resDelete( ( MasterSocket * ) key.ptr, key, success );
		SlaveWorker::eventQueue->insert( masterEvent );
	}
	return true;
}

bool SlaveWorker::handleGetChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	int pending;
	std::set<ChunkRequest>::iterator it;
	ChunkRequest chunkRequest;

	if ( success ) {
		struct ChunkDataHeader header;
		if ( ! this->protocol.parseChunkDataHeader( header, buf, size ) ) {
			__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Invalid GET_CHUNK (success) response." );
			return false;
		}
		__DEBUG__(
			BLUE, "SlaveWorker", "handleGetChunkResponse",
			"[GET_CHUNK (success)] List ID: %u, stripe ID: %u, chunk ID: %u; chunk size = %u.",
			header.listId, header.stripeId, header.chunkId, header.size
		);
		chunkRequest.set(
			header.listId, header.stripeId, header.chunkId,
			event.socket, 0 // ptr
		);
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
			event.socket, 0 // ptr
		);
	}

	it = SlaveWorker::pending->slavePeers.getChunk.lower_bound( chunkRequest );
	if ( it == SlaveWorker::pending->slavePeers.getChunk.end() || ! chunkRequest.equal( *it ) ) {
		__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot find a pending slave GET_CHUNK request that matches the response. This message will be discarded." );
		return false;
	}
	chunkRequest = *it;
	SlaveWorker::pending->slavePeers.getChunk.erase( it );

	// Check pending slave GET_CHUNK requests
	chunkRequest.chunkId = 0;
	chunkRequest.socket = 0;
	it = SlaveWorker::pending->slavePeers.getChunk.lower_bound( chunkRequest );
	for ( pending = 0; it != SlaveWorker::pending->slavePeers.getChunk.end() && chunkRequest.matchStripe( *it ); pending++, it++ );
	__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Pending slave GET_CHUNK requests = %d (%s).", pending, success ? "success" : "fail" );
	if ( pending == 0 ) {
		__ERROR__( "SlaveWorker", "handleGetChunkResponse", "What should be done after all GET_CHUNK requests return?" );
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

	std::set<ChunkRequest>::iterator it;
	ChunkRequest chunkRequest;

	chunkRequest.set(
		header.listId, header.stripeId, header.chunkId,
		event.socket, 0 // ptr
	);

	it = SlaveWorker::pending->slavePeers.setChunk.lower_bound( chunkRequest );
	if ( it == SlaveWorker::pending->slavePeers.setChunk.end() || ! chunkRequest.equal( *it ) ) {
		__ERROR__( "SlaveWorker", "handleSetChunkResponse", "Cannot find a pending slave SET_CHUNK request that matches the response. This message will be discarded." );
		return false;
	}
	chunkRequest = *it;
	SlaveWorker::pending->slavePeers.setChunk.erase( it );

	// TODO: What should we do next?

	return true;
}

void SlaveWorker::free() {
	if ( this->storage ) {
		this->storage->stop();
		Storage::destroy( this->storage );
	}
	this->protocol.free();
	this->dataChunk->free();
	this->parityChunk->free();
	delete this->dataChunk;
	delete this->parityChunk;
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
			SLAVE_WORKER_EVENT_LOOP(
				MixedEvent,
				eventQueue->mixed
			);
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

	SlaveWorker::dataChunkCount = slave->config.global.coding.params.getDataChunkCount();
	SlaveWorker::parityChunkCount = slave->config.global.coding.params.getParityChunkCount();
	SlaveWorker::pending = &slave->pending;
	SlaveWorker::slaveServerAddr = &slave->config.slave.slave.addr;
	SlaveWorker::coding = slave->coding;
	SlaveWorker::eventQueue = &slave->eventQueue;
	SlaveWorker::stripeList = slave->stripeList;
	SlaveWorker::map = &slave->map;
	SlaveWorker::chunkPool = slave->chunkPool;
	SlaveWorker::stripePool = slave->stripePool;
	SlaveWorker::chunkBuffer = &slave->chunkBuffer;
	return true;
}

bool SlaveWorker::init( GlobalConfig &globalConfig, SlaveConfig &slaveConfig, WorkerRole role ) {
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			globalConfig.size.key,
			globalConfig.size.chunk
		),
		SlaveWorker::parityChunkCount
	);
	this->role = role;
	this->dataChunk = new Chunk();
	this->parityChunk = new Chunk();
	this->dataChunk->init( globalConfig.size.chunk );
	this->parityChunk->init( globalConfig.size.chunk );
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
