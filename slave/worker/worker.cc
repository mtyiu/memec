#include "worker.hh"
#include "../main/slave.hh"
#include "../../common/util/debug.hh"
#include "../../common/util/time.hh"

#define WORKER_COLOR	YELLOW

uint32_t SlaveWorker::dataChunkCount;
uint32_t SlaveWorker::parityChunkCount;
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

		ret = event.socket->recv( this->protocol.buffer.recv, PROTO_HEADER_SIZE, connected, true );

		if ( ret == PROTO_HEADER_SIZE && connected ) {
			this->load.recvBytes += ret;
			this->load.elapsedTime += get_elapsed_time( t );

			this->protocol.parseHeader( header, this->protocol.buffer.recv, ret );
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_COORDINATOR ) {
				__ERROR__( "SlaveWorker", "dispatch", "Invalid message source from coordinator." );
				return;
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
	bool success = true, connected, isSend;
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
		case MASTER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS:
			success = true;
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_GET_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE:
			success = false;
			isSend = true;
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
				buffer.size,
				success,
				event.message.key.size,
				event.message.key.data
			);
			break;
		// SET
		case MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resSet(
				buffer.size,
				success,
				event.message.key.size,
				event.message.key.data
			);
			break;
		// UPDATE
		case MASTER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resUpdate(
				buffer.size,
				event.message.keyValueChunkUpdate.key.size,
				event.message.keyValueChunkUpdate.key.data,
				event.message.keyValueChunkUpdate.valueUpdateOffset,
				event.message.keyValueChunkUpdate.valueUpdateSize,
				event.message.keyValueChunkUpdate.metadata.listId,
				event.message.keyValueChunkUpdate.metadata.stripeId,
				event.message.keyValueChunkUpdate.metadata.chunkId,
				event.message.keyValueChunkUpdate.offset,
				event.message.keyValueChunkUpdate.length,
				event.message.keyValueChunkUpdate.delta
			);
			break;
		case MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
			buffer.data = this->protocol.resUpdate(
				buffer.size,
				event.message.keyValueChunkUpdate.key.size,
				event.message.keyValueChunkUpdate.key.data,
				event.message.keyValueChunkUpdate.valueUpdateOffset,
				event.message.keyValueChunkUpdate.valueUpdateSize
			);
			break;
		// UPDATE_CHUNK
		case MASTER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE:
			buffer.data = this->protocol.resUpdateChunk(
				buffer.size,
				success,
				event.message.chunkUpdate.metadata.listId,
				event.message.chunkUpdate.metadata.stripeId,
				event.message.chunkUpdate.metadata.chunkId,
				event.message.chunkUpdate.offset,
				event.message.chunkUpdate.length,
				event.message.chunkUpdate.valueUpdateOffset
			);
			break;
		// DELETE
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resDelete(
				buffer.size,
				event.message.keyChunkUpdate.key.size,
				event.message.keyChunkUpdate.key.data,
				event.message.keyChunkUpdate.metadata.listId,
				event.message.keyChunkUpdate.metadata.stripeId,
				event.message.keyChunkUpdate.metadata.chunkId,
				event.message.keyChunkUpdate.offset,
				event.message.keyChunkUpdate.length,
				event.message.keyChunkUpdate.delta
			);
			break;
		case MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
			buffer.data = this->protocol.resDelete(
				buffer.size,
				event.message.key.size,
				event.message.key.data
			);
			break;
		// DELETE_CHUNK
		case MASTER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE:
			buffer.data = this->protocol.resDeleteChunk(
				buffer.size,
				success,
				event.message.chunkUpdate.metadata.listId,
				event.message.chunkUpdate.metadata.stripeId,
				event.message.chunkUpdate.metadata.chunkId,
				event.message.chunkUpdate.offset,
				event.message.chunkUpdate.length
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
		if ( ret > 0 ) {
			this->load.recvBytes += ret;
			this->load.elapsedTime += get_elapsed_time( t );
		}
		if ( ! this->protocol.parseHeader( header ) ) {
			__ERROR__( "SlaveWorker", "dispatch", "Undefined message." );
		} else {
			if (
				header.magic != PROTO_MAGIC_REQUEST ||
				header.from != PROTO_MAGIC_FROM_MASTER ||
				header.to != PROTO_MAGIC_TO_SLAVE
			) {
				__ERROR__( "SlaveWorker", "dispatch", "Invalid protocol header." );
			}

			switch( header.opcode ) {
				case PROTO_OPCODE_GET:
					this->handleGetRequest( event );
					break;
				case PROTO_OPCODE_SET:
					this->handleSetRequest( event );
					break;
				case PROTO_OPCODE_UPDATE:
					this->handleUpdateRequest( event );
					break;
				case PROTO_OPCODE_UPDATE_CHUNK:
					this->handleUpdateChunkRequest( event );
					break;
				case PROTO_OPCODE_DELETE:
					this->handleDeleteRequest( event );
					break;
				case PROTO_OPCODE_DELETE_CHUNK:
					this->handleDeleteChunkRequest( event );
					break;
				default:
					__ERROR__( "SlaveWorker", "dispatch", "Invalid opcode from master." );
					return;
			}
		}
	}

	if ( ! connected )
		__ERROR__( "SlaveWorker", "dispatch", "The master is disconnected." );
}

void SlaveWorker::dispatch( SlaveEvent event ) {
}

void SlaveWorker::dispatch( SlavePeerEvent event ) {
	bool connected, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;
	struct timespec t = start_timer();

	switch( event.type ) {
		case SLAVE_PEER_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterSlavePeer( buffer.size, SlaveWorker::slaveServerAddr );
			isSend = true;
			break;
		case SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resRegisterSlavePeer( buffer.size, true );
			isSend = true;
			break;
		case SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRegisterSlavePeer( buffer.size, false );
			isSend = true;
			break;
		case SLAVE_PEER_EVENT_TYPE_PENDING:
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
slave_peer_parse_again:
		if ( ! this->protocol.parseHeader( header ) ) {
			__ERROR__( "SlaveWorker", "dispatch", "Undefined message." );
		} else {
			if (
				header.from != PROTO_MAGIC_FROM_SLAVE ||
				header.to != PROTO_MAGIC_TO_SLAVE
			) {
				__ERROR__( "SlaveWorker", "dispatch", "Invalid protocol header." );
			}

			switch ( header.opcode ) {
				case PROTO_OPCODE_REGISTER:
					switch( header.magic ) {
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							event.socket->registered = true;
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							__ERROR__( "SlaveWorker", "dispatch", "Failed to register with slave." );
							break;
						case PROTO_MAGIC_REQUEST:
							this->handleSlavePeerRegisterRequest( event.socket, header, ret );
							if ( ret > header.length + PROTO_HEADER_SIZE ) {
								memmove(
									this->protocol.buffer.recv,
									this->protocol.buffer.recv + header.length + PROTO_HEADER_SIZE,
									ret - header.length - PROTO_HEADER_SIZE
								);
								goto slave_peer_parse_again;
							}
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
		}
	}
	if ( ! connected )
		__ERROR__( "SlaveWorker", "dispatch", "The slave is disconnected." );
}

bool SlaveWorker::handleSlavePeerRegisterRequest( SlavePeerSocket *socket, ProtocolHeader &header, ssize_t recvBytes ) {
	static Slave *slave = Slave::getInstance();
	SlavePeerSocket *s = 0;
	ServerAddr serverAddr;

	if ( header.length > SERVER_ADDR_MESSSAGE_MAX_LEN ) {
		__ERROR__( "SlaveWorker", "handleSlavePeerRegisterRequest", "Unexpected header size in slave registration." );
		return false;
	}

	// Find the corresponding SlavePeerSocket
	serverAddr.deserialize( this->protocol.buffer.recv + PROTO_HEADER_SIZE );
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

bool SlaveWorker::isRedirectedRequest( uint32_t listId ) {
	SlaveWorker::stripeList->get( listId, this->paritySlaveSockets );
	for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ )
		if ( this->paritySlaveSockets[ i ]->self )
			return false;
	return true;
}

bool SlaveWorker::handleGetRequest( MasterEvent event ) {
	struct KeyHeader header;
	bool ret;
	if ( ! this->protocol.parseKeyHeader( header ) ) {
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

bool SlaveWorker::handleSetRequest( MasterEvent event ) {
	struct KeyValueHeader header;
	if ( ! this->protocol.parseKeyValueHeader( header ) ) {
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

	event.resSet( event.socket, key );
	this->load.set();
	this->dispatch( event );

	return true;
}

bool SlaveWorker::handleUpdateRequest( MasterEvent event ) {
	struct KeyValueUpdateHeader header;
	bool ret;
	if ( ! this->protocol.parseKeyValueUpdateHeader( header ) ) {
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

		// Compute delta and perform update
		chunk->computeDelta(
			header.valueUpdate, // delta
			header.valueUpdate, // new data
			offset, header.valueUpdateSize,
			true // perform update
		);

		event.resUpdate(
			event.socket, key,
			header.valueUpdateOffset,
			header.valueUpdateSize,
			metadata, offset,
			header.valueUpdateSize,
			header.valueUpdate
		);
		ret = true;
	} else {
		event.resUpdate( event.socket, key, header.valueUpdateOffset, header.valueUpdateSize );
		ret = false;
	}

	this->load.update();
	this->dispatch( event );

	return ret;
}

bool SlaveWorker::handleUpdateChunkRequest( MasterEvent event ) {
	struct ChunkUpdateHeader header;
	bool ret;
	if ( ! this->protocol.parseChunkUpdateHeader( header, true ) ) {
		__ERROR__( "SlaveWorker", "handleUpdateChunkRequest", "Invalid UPDATE_CHUNK request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleUpdateChunkRequest",
		"[UPDATE_CHUNK] List ID: %u; stripe ID: %u; chunk ID: %u; offset: %u; length: %u; value update offset: %u",
		header.listId, header.stripeId, header.chunkId,
		header.offset, header.length, header.valueUpdateOffset
	);

	// Detect degraded UPDATE_CHUNK
	if ( ! this->isRedirectedRequest( header.listId ) ) {
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
		header.valueUpdateOffset, ret
	);

	this->load.updateChunk();
	this->dispatch( event );

	return ret;
}

bool SlaveWorker::handleDeleteRequest( MasterEvent event ) {
	struct KeyHeader header;
	bool ret;
	if ( ! this->protocol.parseKeyHeader( header ) ) {
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

		// Update data chunk and map
		delta = this->protocol.buffer.recv + PROTO_KEY_CHUNK_UPDATE_SIZE + key.size;
		deltaSize = chunk->deleteKeyValue(
			key, &map->keys, delta,
			this->protocol.buffer.size - PROTO_KEY_CHUNK_UPDATE_SIZE - key.size
		);

		event.resDelete(
			event.socket, key,
			metadata, keyMetadata.offset,
			deltaSize, delta
		);

		ret = true;
	} else {
		event.resDelete( event.socket, key );
		ret = false;
	}

	this->load.del();
	this->dispatch( event );

	return ret;
}

bool SlaveWorker::handleDeleteChunkRequest( MasterEvent event ) {
	struct ChunkUpdateHeader header;
	bool ret;
	if ( ! this->protocol.parseChunkUpdateHeader( header, false ) ) {
		__ERROR__( "SlaveWorker", "handleDeleteChunkRequest", "Invalid DELETE_CHUNK request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleDeleteChunkRequest",
		"[DELETE_CHUNK] List ID: %u; stripe ID: %u; chunk ID: %u; offset: %u; length: %u.",
		header.listId, header.stripeId, header.chunkId,
		header.offset, header.length
	);

	// Detect degraded UPDATE_CHUNK
	if ( ! this->isRedirectedRequest( header.listId ) ) {
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
		header.offset, header.length, ret
	);

	this->load.delChunk();
	this->dispatch( event );

	return ret;
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
		)
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
