#include "worker.hh"
#include "../main/slave.hh"
#include "../../common/util/debug.hh"
#include "../../common/util/time.hh"

#define WORKER_COLOR	YELLOW

uint32_t SlaveWorker::dataChunkCount;
uint32_t SlaveWorker::parityChunkCount;
uint32_t SlaveWorker::chunkCount;
ArrayMap<int, SlavePeerSocket> *SlaveWorker::slavePeers;
Pending *SlaveWorker::pending;
ServerAddr *SlaveWorker::slaveServerAddr;
Coding *SlaveWorker::coding;
SlaveEventQueue *SlaveWorker::eventQueue;
StripeList<SlavePeerSocket> *SlaveWorker::stripeList;
std::vector<StripeListIndex> *SlaveWorker::stripeListIndex;
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
		case CODING_EVENT_TYPE_DECODE:
			SlaveWorker::coding->decode( event.message.decode.chunks, event.message.decode.status );
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
			buffer.data = this->protocol.reqRegisterCoordinator( buffer.size, event.message.address.addr, event.message.address.port );
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
				case PROTO_OPCODE_SLAVE_CONNECTED:
					this->handleSlaveConnectedMsg( event, buffer.data, buffer.size );
					break;
				default:
					__ERROR__( "SlaveWorker", "dispatch", "Invalid opcode from coordinator." );
					return;
			}
			buffer.data += header.length;
			buffer.size -= header.length;
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
			buffer.data = this->protocol.resGet(
				buffer.size, success,
				keySize, key,
				valueSize, value
			);
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
			printf( "[REQ] Register slave peer: sending\n" );
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
			printf( "[RESP] Register slave peer: sending\n" );
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
							printf( "[REQ] Register slave peer: received\n" );
							this->handleSlavePeerRegisterRequest( event.socket, buffer.data, buffer.size );
							break;
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							printf( "[RESP] Register slave peer: received\n" );
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
							this->handleUpdateChunkRequest( event, buffer.data, buffer.size );
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
					__ERROR__( "SlaveWorker", "dispatch", "Invalid opcode from slave." );
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
		}
		return true;
	}
	if ( isParity ) *isParity = false;
	return false;
}

bool SlaveWorker::isRedirectedRequest( uint32_t listId, uint32_t updatingChunkId ) {
	SlaveWorker::stripeList->get( listId, this->paritySlaveSockets );
	for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ )
		if ( this->paritySlaveSockets[ i ]->self )
			return updatingChunkId != i + SlaveWorker::dataChunkCount;
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
		for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
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

			for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
				SlavePeerSocket *s = SlaveWorker::stripeList->get( listId, chunkId, i );
				if ( s->ready() ) {
					this->paritySlaveSockets[ i ] = s;
					break;
				} else if ( i == SlaveWorker::chunkCount - 1 ) {
					__ERROR__( "SlaveWorker", "getSlave", "Cannot find a slave for performing degraded operation." );
					return 0;
				}
			}
		}
	}
	return ret;
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
	for ( int i = 0, len = slavePeers->values.size(); i < len; i++ ) {
		if ( slavePeers->values[ i ].equal( header.addr, header.port ) ) {
			index = i;
			break;
		}
	}
	if ( index == -1 ) {
		__ERROR__( "SlaveWorker", "handleSlaveConnectedMsg", "The slave is not in the list. Ignoring this slave..." );
		return false;
	}

	// Update sockfd in the array Map
	int sockfd = slavePeers->values[ index ].init();
	slavePeers->keys[ index ] = sockfd;

	// Connect to the slave peer
	slavePeers->values[ index ].start();

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
	if ( map->findValueByKey( header.key, header.keySize, &keyValue, &key ) ) {
		event.resGet( event.socket, keyValue );
		ret = true;
		this->dispatch( event );
	} else {
		// Detect degraded GET
		uint32_t listId, chunkId;
		if ( this->isRedirectedRequest( header.key, header.keySize, 0, &listId, &chunkId ) ) {
			Key key;
			key.dup( header.keySize, header.key, ( void * ) event.socket );

			pthread_mutex_lock( &SlaveWorker::pending->masters.getLock );
			SlaveWorker::pending->masters.get.insert( key );
			pthread_mutex_unlock( &SlaveWorker::pending->masters.getLock );

			// Need to know the stripe ID
			this->performDegradedRead( listId, 0, chunkId, PROTO_OPCODE_GET, &key );
			ret = false;
		} else {
			event.resGet( event.socket, key );
			ret = false;
			this->dispatch( event );
		}
	}
	this->load.get();
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
	if ( this->isRedirectedRequest( header.key, header.keySize, &isParity, &listId, &chunkId ) && ! isParity ) {
		__DEBUG__( YELLOW, "SlaveWorker", "handleSetRequest", "!!! Degraded SET request [not yet implemented] !!!" );
		// TODO
	}

	SlaveWorker::chunkBuffer->at( listId )->set(
		header.key, header.keySize, header.value, header.valueSize, PROTO_OPCODE_SET, chunkId,
		this->chunks, this->dataChunk, this->parityChunk
	);

	Key key;
	key.set( header.keySize, header.key );
	event.resSet( event.socket, key, true );
	this->load.set();
	this->dispatch( event );

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

	// Detect degraded UPDATE
	if ( this->isRedirectedRequest( header.key, header.keySize ) ) {
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

		pthread_mutex_lock( &SlaveWorker::pending->masters.updateLock );
		SlaveWorker::pending->masters.update.insert( keyValueUpdate );
		pthread_mutex_unlock( &SlaveWorker::pending->masters.updateLock );

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

			pthread_mutex_lock( &SlaveWorker::pending->slavePeers.updateLock );
			SlaveWorker::pending->slavePeers.updateChunk.insert( chunkUpdate );
			pthread_mutex_unlock( &SlaveWorker::pending->slavePeers.updateLock );

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
	if ( this->isRedirectedRequest( header.key, header.keySize ) ) {
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

		pthread_mutex_lock( &SlaveWorker::pending->masters.delLock );
		SlaveWorker::pending->masters.del.insert( key );
		pthread_mutex_unlock( &SlaveWorker::pending->masters.delLock );

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

			pthread_mutex_lock( &SlaveWorker::pending->slavePeers.delLock );
			SlaveWorker::pending->slavePeers.deleteChunk.insert( chunkUpdate );
			pthread_mutex_unlock( &SlaveWorker::pending->slavePeers.delLock );

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
	struct AddressHeader header;
	if ( ! this->protocol.parseAddressHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleSlavePeerRegisterRequest", "Invalid address header." );
		return false;
	}

	// Find the slave peer socket in the array map
	int index = -1;
	for ( int i = 0, len = slavePeers->values.size(); i < len; i++ ) {
		if ( slavePeers->values[ i ].equal( header.addr, header.port ) ) {
			index = i;
			break;
		}
	}
	if ( index == -1 ) {
		__ERROR__( "SlaveWorker", "handleSlavePeerRegisterRequest", "The slave is not in the list. Ignoring this slave..." );
		return false;
	}

	SlavePeerEvent event;
	event.resRegister( &slavePeers->values[ index ], true );
	SlaveWorker::eventQueue->insert( event );
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

	// Detect degraded UPDATE_CHUNK
	if ( this->isRedirectedRequest( header.listId, header.updatingChunkId ) ) {
		__DEBUG__( YELLOW, "SlaveWorker", "handleUpdateChunkRequest", "!!! Degraded UPDATE_CHUNK request [not yet implemented] !!!" );
		// TODO
		ret = false;
	} else {
		SlaveWorker::chunkBuffer->at( header.listId )->update(
			header.stripeId, header.chunkId,
			header.offset, header.length, header.delta,
			this->chunks, this->dataChunk, this->parityChunk
		);
		ret = true;
	}

	Metadata metadata;
	metadata.set( header.listId, header.stripeId, header.chunkId );

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
	if ( ! this->protocol.parseChunkUpdateHeader( header, true, buf, size ) ) {
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
	if ( this->isRedirectedRequest( header.listId, header.updatingChunkId ) ) {
		__DEBUG__( YELLOW, "SlaveWorker", "handleDeleteChunkRequest", "!!! Degraded DELETE_CHUNK request [not yet implemented] !!!" );
		// TODO
		ret = false;
	} else {
		SlaveWorker::chunkBuffer->at( header.listId )->update(
			header.stripeId, header.chunkId,
			header.offset, header.length, header.delta,
			this->chunks, this->dataChunk, this->parityChunk
		);
		ret = true;
	}

	Metadata metadata;
	metadata.set( header.listId, header.stripeId, header.chunkId );

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
	ret = chunk;

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

	std::set<ChunkUpdate>::iterator it;
	ChunkUpdate chunkUpdate;
	int pending;

	chunkUpdate.set(
		header.listId, header.stripeId, header.chunkId,
		header.offset, header.length
	);
	chunkUpdate.setKeyValueUpdate( 0, 0, 0 );
	chunkUpdate.ptr = ( void * ) event.socket;

	pthread_mutex_lock( &SlaveWorker::pending->slavePeers.updateLock );
	it = SlaveWorker::pending->slavePeers.updateChunk.lower_bound( chunkUpdate );
	if ( it == SlaveWorker::pending->slavePeers.updateChunk.end() || ! chunkUpdate.equal( *it ) ) {
		pthread_mutex_unlock( &SlaveWorker::pending->slavePeers.updateLock );
		__ERROR__( "SlaveWorker", "handleUpdateChunkResponse", "Cannot find a pending slave UPDATE_CHUNK request that matches the response. This message will be discarded." );
		return false;
	}
	chunkUpdate = *it;
	SlaveWorker::pending->slavePeers.updateChunk.erase( it );

	// Check pending slave UPDATE_CHUNK requests
	chunkUpdate.ptr = 0;
	it = SlaveWorker::pending->slavePeers.updateChunk.lower_bound( chunkUpdate );
	for ( pending = 0; it != SlaveWorker::pending->slavePeers.updateChunk.end() && chunkUpdate.equal( *it ); pending++, it++ );
	pthread_mutex_unlock( &SlaveWorker::pending->slavePeers.updateLock );

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

		pthread_mutex_lock( &SlaveWorker::pending->masters.updateLock );
		it = SlaveWorker::pending->masters.update.lower_bound( keyValueUpdate );
		if ( it == SlaveWorker::pending->masters.update.end() || ! keyValueUpdate.equal( *it ) ) {
			pthread_mutex_unlock( &SlaveWorker::pending->masters.updateLock );
			__ERROR__( "SlaveWorker", "handleUpdateChunkResponse", "Cannot find a pending master UPDATE request that matches the response. This message will be discarded." );
			return false;
		}
		keyValueUpdate = *it;
		pthread_mutex_unlock( &SlaveWorker::pending->masters.updateLock );

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

	std::set<ChunkUpdate>::iterator it;
	ChunkUpdate chunkUpdate;
	int pending;

	chunkUpdate.set(
		header.listId, header.stripeId, header.chunkId,
		header.offset, header.length
	);
	chunkUpdate.setKeyValueUpdate( 0, 0, 0 );
	chunkUpdate.ptr = ( void * ) event.socket;

	pthread_mutex_lock( &SlaveWorker::pending->slavePeers.delLock );
	it = SlaveWorker::pending->slavePeers.deleteChunk.lower_bound( chunkUpdate );
	if ( it == SlaveWorker::pending->slavePeers.deleteChunk.end() || ! chunkUpdate.equal( *it ) ) {
		pthread_mutex_unlock( &SlaveWorker::pending->slavePeers.delLock );
		__ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Cannot find a pending slave DELETE_CHUNK request that matches the response. This message will be discarded." );
		return false;
	}
	chunkUpdate = *it;
	SlaveWorker::pending->slavePeers.deleteChunk.erase( it );

	// Check pending slave DELETE_CHUNK requests
	chunkUpdate.ptr = 0;
	it = SlaveWorker::pending->slavePeers.deleteChunk.lower_bound( chunkUpdate );
	for ( pending = 0; it != SlaveWorker::pending->slavePeers.deleteChunk.end() && chunkUpdate.equal( *it ); pending++, it++ );
	pthread_mutex_unlock( &SlaveWorker::pending->slavePeers.delLock );

	__ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Pending slave DELETE_CHUNK requests = %d.", pending );
	if ( pending == 0 ) {
		// Only send application DELETE response when the number of pending slave DELETE_CHUNK requests equal 0
		MasterEvent masterEvent;
		std::set<Key>::iterator it;
		Key key;

		key.set( chunkUpdate.keySize, chunkUpdate.key, 0 );

		pthread_mutex_lock( &SlaveWorker::pending->masters.delLock );
		it = SlaveWorker::pending->masters.del.lower_bound( key );
		if ( it == SlaveWorker::pending->masters.del.end() || ! key.equal( *it ) ) {
			pthread_mutex_unlock( &SlaveWorker::pending->masters.delLock );
			__ERROR__( "SlaveWorker", "handleDeleteChunkResponse", "Cannot find a pending master DELETE request that matches the response. This message will be discarded." );
			return false;
		}
		key = *it;
		pthread_mutex_unlock( &SlaveWorker::pending->masters.delLock );
		masterEvent.resDelete( ( MasterSocket * ) key.ptr, key, success );
		SlaveWorker::eventQueue->insert( masterEvent );
	}
	return true;
}

bool SlaveWorker::handleGetChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	int pending;
	std::set<ChunkRequest>::iterator it, tmp;
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
		chunkRequest.set(
			header.listId, header.stripeId, header.chunkId,
			event.socket, 0 // chunk
		);

		// Find the corresponding GET_CHUNK request from the pending set
		pthread_mutex_lock( &SlaveWorker::pending->slavePeers.getLock );
		it = SlaveWorker::pending->slavePeers.getChunk.find( chunkRequest );
		if ( it == SlaveWorker::pending->slavePeers.getChunk.end() || ! chunkRequest.equal( *it ) ) {
			pthread_mutex_unlock( &SlaveWorker::pending->slavePeers.getLock );
			__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot find a pending slave GET_CHUNK request that matches the response. This message will be discarded." );
			return false;
		}

		// Store the chunk into the buffer
		it->chunk = SlaveWorker::chunkPool->malloc();
		it->chunk->loadFromGetChunkRequest(
			header.listId, header.stripeId, header.chunkId,
			header.chunkId >= SlaveWorker::dataChunkCount, // isParity
			header.data, header.size
		);

		// Prepare stripe buffer
		for ( uint32_t i = 0, total = SlaveWorker::chunkCount; i < total; i++ ) {
			this->chunks[ i ] = 0;
		}

		// Check remaining slave GET_CHUNK requests in the pending set
		chunkRequest.chunkId = 0;
		chunkRequest.socket = 0;
		chunkRequest.chunk = 0;
		it = SlaveWorker::pending->slavePeers.getChunk.lower_bound( chunkRequest );
		tmp = it;
		for ( pending = 0; it != SlaveWorker::pending->slavePeers.getChunk.end() && chunkRequest.matchStripe( *it ); it++ ) {
			if ( ! it->chunk ) { // The chunk is not received yet
				pending++;
			} else {
				this->chunks[ it->chunkId ] = it->chunk;
			}
		}
		__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Pending slave GET_CHUNK requests = %d (%s).", pending, success ? "success" : "fail" );
		if ( pending == 0 )
			SlaveWorker::pending->slavePeers.getChunk.erase( tmp, it );
		pthread_mutex_unlock( &SlaveWorker::pending->slavePeers.getLock );

		if ( pending == 0 ) {
			std::set<DegradedOp>::iterator degradedOpsIt, degradedOpsBeginIt;
			DegradedOp op;
			op.set( chunkRequest.listId, chunkRequest.stripeId, 0, 0 );

			// Set up chunk buffer for storing reconstructed chunks
			for ( uint32_t i = 0, j = 0; i < SlaveWorker::chunkCount; i++ ) {
				if ( ! this->chunks[ i ] ) {
					this->freeChunks[ j ].setReconstructed(
						chunkRequest.listId, chunkRequest.stripeId, i,
						i >= SlaveWorker::dataChunkCount
					);
					this->freeChunks[ j ].status = CHUNK_STATUS_TEMPORARY;
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
				            this->chunks[ i ]->size;
				if ( chunkSize > maxChunkSize )
					maxChunkSize = chunkSize;
			}

			for ( uint32_t i = SlaveWorker::dataChunkCount; i < SlaveWorker::chunkCount; i++ ) {
				if ( this->chunks[ i ]->status == CHUNK_STATUS_RECONSTRUCTED ) {
					this->chunks[ i ]->isParity = true;
					this->chunks[ i ]->size = maxChunkSize;
				} else if ( this->chunks[ i ]->size != maxChunkSize ) {
					__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Parity chunk size mismatch (chunk ID = %u): %u vs. %u.", i, this->chunks[ i ]->size, maxChunkSize );
				}
			}

			// Respond the original GET/UPDATE/DELETE operation using the reconstructed data
			pthread_mutex_lock( &SlaveWorker::pending->slavePeers.degradedOpsLock );
			degradedOpsBeginIt = degradedOpsIt = SlaveWorker::pending->slavePeers.degradedOps.lower_bound( op );
			for ( pending = 0; degradedOpsIt != SlaveWorker::pending->slavePeers.degradedOps.end() && op.matchStripe( *degradedOpsIt ); degradedOpsIt++ ) {
				const DegradedOp &op = *degradedOpsIt;
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
				if ( degradedOpsIt->opcode == PROTO_OPCODE_GET ) {
					Key key = op.data.key;
					std::set<Key>::iterator it;

					pthread_mutex_lock( &SlaveWorker::pending->masters.getLock );
					it = SlaveWorker::pending->masters.get.lower_bound( key );
					if ( it == SlaveWorker::pending->masters.get.end() || ! key.equal( *it ) ) {
						pthread_mutex_unlock( &SlaveWorker::pending->masters.getLock );
						__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot find a pending master GET request that matches the response. This message will be discarded." );
						continue;
					}
					key = *it;
					SlaveWorker::pending->masters.get.erase( it );
					pthread_mutex_unlock( &SlaveWorker::pending->masters.getLock );

					MasterEvent event;
					KeyValue keyValue;

					if ( SlaveWorker::map->findValueByKey( key.data, key.size, &keyValue ) )
						event.resGet( ( MasterSocket * ) key.ptr, keyValue );
					else
						event.resGet( ( MasterSocket * ) key.ptr, key );
					this->dispatch( event );
					key.free();
				} else if ( degradedOpsIt->opcode == PROTO_OPCODE_UPDATE ) {
				} else if ( degradedOpsIt->opcode == PROTO_OPCODE_DELETE ) {
				}
			}
			SlaveWorker::pending->slavePeers.degradedOps.erase( degradedOpsBeginIt, degradedOpsIt );
			pthread_mutex_unlock( &SlaveWorker::pending->slavePeers.degradedOpsLock );

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

		pthread_mutex_lock( &SlaveWorker::pending->slavePeers.getLock );
		it = SlaveWorker::pending->slavePeers.getChunk.lower_bound( chunkRequest );
		if ( it == SlaveWorker::pending->slavePeers.getChunk.end() || ! chunkRequest.equal( *it ) ) {
			pthread_mutex_unlock( &SlaveWorker::pending->slavePeers.getLock );
			__ERROR__( "SlaveWorker", "handleGetChunkResponse", "Cannot find a pending slave GET_CHUNK request that matches the response. This message will be discarded." );
			return false;
		}
		SlaveWorker::pending->slavePeers.getChunk.erase( it );
		pthread_mutex_unlock( &SlaveWorker::pending->slavePeers.getLock );

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

	std::set<ChunkRequest>::iterator it;
	ChunkRequest chunkRequest;

	chunkRequest.set(
		header.listId, header.stripeId, header.chunkId,
		event.socket, 0 // ptr
	);

	pthread_mutex_lock( &SlaveWorker::pending->slavePeers.setLock );
	it = SlaveWorker::pending->slavePeers.setChunk.lower_bound( chunkRequest );
	if ( it == SlaveWorker::pending->slavePeers.setChunk.end() || ! chunkRequest.equal( *it ) ) {
		pthread_mutex_unlock( &SlaveWorker::pending->slavePeers.setLock );
		__ERROR__( "SlaveWorker", "handleSetChunkResponse", "Cannot find a pending slave SET_CHUNK request that matches the response. This message will be discarded." );
		return false;
	}
	chunkRequest = *it;
	SlaveWorker::pending->slavePeers.setChunk.erase( it );
	pthread_mutex_unlock( &SlaveWorker::pending->slavePeers.setLock );

	// TODO: What should we do next?

	return true;
}

bool SlaveWorker::performDegradedRead( uint32_t listId, uint32_t stripeId, uint32_t lostChunkId, uint8_t opcode, Key *key, KeyValueUpdate *keyValueUpdate ) {
	SlaveWorker::stripeList->get( listId, this->paritySlaveSockets, this->dataSlaveSockets );

	// Check whether the number of surviving nodes >= k
	uint32_t selected = 0;
	SlavePeerSocket *socket = 0;
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

	// Add to degraded operation pending set
	DegradedOp op;
	op.set( listId, stripeId, lostChunkId, opcode );
	if ( opcode == PROTO_OPCODE_UPDATE )
		op.data.keyValueUpdate = *keyValueUpdate;
	else
		op.data.key = *key;
	pthread_mutex_lock( &SlaveWorker::pending->slavePeers.degradedOpsLock );
	SlaveWorker::pending->slavePeers.degradedOps.insert( op );
	pthread_mutex_unlock( &SlaveWorker::pending->slavePeers.degradedOpsLock );

	// Send GET_CHUNK requests to surviving nodes
	Metadata metadata;
	SlavePeerEvent event;
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

		if ( socket->self ) {
			ChunkRequest chunkRequest;
			chunkRequest.set( listId, stripeId, i, socket );
			chunkRequest.chunk = SlaveWorker::map->findChunkById( listId,
			 stripeId, i );
 			// Add to pending GET_CHUNK request set
 			pthread_mutex_lock( &SlaveWorker::pending->slavePeers.getLock );
 			SlaveWorker::pending->slavePeers.getChunk.insert( chunkRequest );
 			pthread_mutex_unlock( &SlaveWorker::pending->slavePeers.getLock );
		} else if ( socket->ready() ) {
			ChunkRequest chunkRequest;
			chunkRequest.set( listId, stripeId, i, socket );
			chunkRequest.chunk = 0;
			// Add to pending GET_CHUNK request set
			pthread_mutex_lock( &SlaveWorker::pending->slavePeers.getLock );
			SlaveWorker::pending->slavePeers.getChunk.insert( chunkRequest );
			pthread_mutex_unlock( &SlaveWorker::pending->slavePeers.getLock );

			metadata.chunkId = i;
			event.reqGetChunk( socket, metadata );
			SlaveWorker::eventQueue->insert( event );
		}
		selected++;
	}

	return ( selected >= SlaveWorker::dataChunkCount );
}

void SlaveWorker::free() {
	if ( this->storage ) {
		this->storage->stop();
		Storage::destroy( this->storage );
	}
	this->protocol.free();
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
		SlaveWorker::dataChunkCount
	);
	this->role = role;
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
