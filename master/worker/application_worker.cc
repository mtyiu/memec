#include "worker.hh"
#include "../main/master.hh"

void MasterWorker::dispatch( ApplicationEvent event ) {
	bool success = true, connected, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	switch( event.type ) {
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_GET_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
			success = true;
			isSend = true;
			break;
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
		case APPLICATION_EVENT_TYPE_GET_RESPONSE_FAILURE:
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE:
		case APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
		case APPLICATION_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
			success = false;
			isSend = true;
			break;
		case APPLICATION_EVENT_TYPE_PENDING:
		default:
			isSend = false;
			break;
	}

	switch( event.type ) {
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRegisterApplication( buffer.size, event.id, success );
			break;
		case APPLICATION_EVENT_TYPE_GET_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resGet(
				buffer.size,
				event.id,
				success,
				event.message.keyValue.keySize,
				event.message.keyValue.keyStr,
				event.message.keyValue.valueSize,
				event.message.keyValue.valueStr
			);
			break;
		case APPLICATION_EVENT_TYPE_GET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resGet(
				buffer.size,
				event.id,
				success,
				event.message.key.size,
				event.message.key.data
			);
			if ( event.needsFree )
				event.message.key.free();
			break;
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resSet(
				buffer.size,
				event.id,
				success,
				event.message.key.size,
				event.message.key.data
			);
			if ( event.needsFree )
				event.message.key.free();
			break;
		case APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
			buffer.data = this->protocol.resUpdate(
				buffer.size,
				event.id,
				success,
				event.message.keyValueUpdate.size,
				event.message.keyValueUpdate.data,
				event.message.keyValueUpdate.offset,
				event.message.keyValueUpdate.length
			);
			if ( event.needsFree )
				event.message.keyValueUpdate.free();
			break;
		case APPLICATION_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
			buffer.data = this->protocol.resDelete(
				buffer.size,
				event.id,
				success,
				event.message.key.size,
				event.message.key.data
			);
			if ( event.needsFree )
				event.message.key.free();
			break;
		case APPLICATION_EVENT_TYPE_PENDING:
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "MasterWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else {
		// Parse requests from applications
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "MasterWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			if ( header.magic != PROTO_MAGIC_REQUEST || header.from != PROTO_MAGIC_FROM_APPLICATION ) {
				__ERROR__( "MasterWorker", "dispatch", "Invalid protocol header." );
			} else {
				event.id = header.id;
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
						__ERROR__( "MasterWorker", "dispatch", "Invalid opcode from application." );
						break;
				}
			}
			buffer.data += header.length;
			buffer.size -= header.length;
		}
		if ( connected ) event.socket->done();
	}

	if ( ! connected ) {
		__DEBUG__( RED, "MasterWorker", "dispatch", "The application is disconnected." );
		// delete event.socket;
	}
}

bool MasterWorker::handleSetRequest( ApplicationEvent event, char *buf, size_t size ) {
	struct KeyValueHeader header;
	if ( ! this->protocol.parseKeyValueHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleSetRequest", "Invalid SET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleSetRequest",
		"[SET] Key: %.*s (key size = %u); Value: (value size = %u)",
		( int ) header.keySize, header.key, header.keySize, header.valueSize
	);

	uint32_t listId, chunkId;
	bool connected;
	ssize_t sentBytes;
	SlaveSocket *socket;

	socket = this->getSlaves(
		header.key, header.keySize,
		listId, chunkId
	);

	if ( ! socket ) {
		Key key;
		key.set( header.keySize, header.key );
		event.resSet( event.socket, event.id, key, false, false );
		this->dispatch( event );
		return false;
	}

	// decide whether any of the data / parity slave needs to use remapping flow
	bool remapped = false;
	for ( uint32_t i = 0; i < 1 + MasterWorker::parityChunkCount; i++ ) {
		Master *master = Master::getInstance();
		if ( i == 0 )
			remapped |= master->remapMsgHandler.useRemappingFlow( socket->getAddr() );
		else {
			remapped |= master->remapMsgHandler.useRemappingFlow( this->paritySlaveSockets[ i - 1 ]->getAddr() );
		}
	}
	remapped &= ( ! MasterWorker::disableRemappingSet );
	if ( remapped ) {
		return this->handleRemappingSetRequest( event, buf, size );
	}

	int sockfd = socket->getSocket();
	MasterWorker::slaveSockets->get( sockfd )->counter.increaseNormal();

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	uint32_t requestId = MasterWorker::idGenerator->nextVal( this->workerId );

#ifdef MASTER_WORKER_SEND_REPLICAS_PARALLEL
	Packet *packet = 0;
	if ( MasterWorker::parityChunkCount ) {
		packet = MasterWorker::packetPool->malloc();
		packet->setReferenceCount( 1 + MasterWorker::parityChunkCount );
		buffer.data = packet->data;
		this->protocol.reqSet( buffer.size, requestId, header.key, header.keySize, header.value, header.valueSize, buffer.data );
		packet->size = buffer.size;
	} else {
		buffer.data = this->protocol.reqSet( buffer.size, requestId, header.key, header.keySize, header.value, header.valueSize );
	}
#else
	buffer.data = this->protocol.reqSet( buffer.size, requestId, header.key, header.keySize, header.value, header.valueSize );
#endif

	key.dup( header.keySize, header.key, ( void * ) event.socket );

	if ( ! MasterWorker::pending->insertKey( PT_APPLICATION_SET, event.id, ( void * ) event.socket, key ) ) {
		__ERROR__( "MasterWorker", "handleSetRequest", "Cannot insert into application SET pending map." );
	}

	for ( uint32_t i = 0; i < MasterWorker::parityChunkCount + 1; i++ ) {
		key.ptr = ( void * )( i == 0 ? socket : this->paritySlaveSockets[ i - 1 ] );
		if ( ! MasterWorker::pending->insertKey(
			PT_SLAVE_SET, requestId, event.id,
			( void * )( i == 0 ? socket : this->paritySlaveSockets[ i - 1 ] ),
			key
		) ) {
			__ERROR__( "MasterWorker", "handleSetRequest", "Cannot insert into slave SET pending map." );
		}
	}

	// Send SET requests
	if ( MasterWorker::parityChunkCount ) {
		for ( uint32_t i = 0; i < MasterWorker::parityChunkCount; i++ ) {
			if ( MasterWorker::updateInterval ) {
				// Mark the time when request is sent
				MasterWorker::pending->recordRequestStartTime(
					PT_SLAVE_SET, requestId, event.id,
					( void * ) this->paritySlaveSockets[ i ],
					this->paritySlaveSockets[ i ]->getAddr()
				);
			}

#ifdef MASTER_WORKER_SEND_REPLICAS_PARALLEL
			SlaveEvent slaveEvent;
			slaveEvent.send( this->paritySlaveSockets[ i ], packet );
			MasterWorker::eventQueue->prioritizedInsert( slaveEvent );
		}

		if ( MasterWorker::updateInterval )
			MasterWorker::pending->recordRequestStartTime( PT_SLAVE_SET, requestId, event.id, ( void * ) socket, socket->getAddr() );
		SlaveEvent slaveEvent;
		slaveEvent.send( socket, packet );
		this->dispatch( slaveEvent );
#else
			sentBytes = this->paritySlaveSockets[ i ]->send( buffer.data, buffer.size, connected );
			if ( sentBytes != ( ssize_t ) buffer.size ) {
				__ERROR__( "MasterWorker", "handleSetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
			}
		}

		if ( MasterWorker::updateInterval )
			MasterWorker::pending->recordRequestStartTime( PT_SLAVE_SET, requestId, event.id, ( void * ) socket, socket->getAddr() );
		sentBytes = socket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "MasterWorker", "handleSetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );

			MasterWorker::slaveSockets->get( sockfd )->counter.decreaseNormal();
			Master::getInstance()->remapMsgHandler.ackRemap( socket->getAddr() );

			return false;
		}
#endif
	} else {
		if ( MasterWorker::updateInterval )
			MasterWorker::pending->recordRequestStartTime( PT_SLAVE_SET, requestId, event.id, ( void * ) socket, socket->getAddr() );
		sentBytes = socket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "MasterWorker", "handleSetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );

			MasterWorker::slaveSockets->get( sockfd )->counter.decreaseNormal();
			Master::getInstance()->remapMsgHandler.ackRemap( socket->getAddr() );

			return false;
		}
	}

	Master::getInstance()->remapMsgHandler.ackRemap( socket->getAddr() );

	return true;
}

bool MasterWorker::handleGetRequest( ApplicationEvent event, char *buf, size_t size ) {
	struct KeyHeader header;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleGetRequest", "Invalid GET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleGetRequest",
		"[GET] Key: %.*s (key size = %u).",
		( int ) header.keySize, header.key, header.keySize
	);

	uint32_t listId, chunkId, newChunkId;
	bool connected, useDegradedMode;
	SlaveSocket *socket, *target;

	socket = this->getSlaves(
		header.key, header.keySize,
		listId, chunkId, newChunkId, useDegradedMode, target
	);
	if ( ! socket ) {
		Key key;
		key.set( header.keySize, header.key );
		event.resGet( event.socket, event.id, key, false );
		this->dispatch( event );
		return false;
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	ssize_t sentBytes;
	uint32_t requestId = MasterWorker::idGenerator->nextVal( this->workerId );
	int sockfd = target->getSocket();

	key.dup( header.keySize, header.key, ( void * ) event.socket );
	if ( ! MasterWorker::pending->insertKey( PT_APPLICATION_GET, event.id, ( void * ) event.socket, key ) ) {
		__ERROR__( "MasterWorker", "handleGetRequest", "Cannot insert into application GET pending map." );
	}

	if ( useDegradedMode ) {
		// Acquire degraded lock from the coordinator
		// __INFO__(
		// 	BLUE, "MasterWorker", "handleGetRequest",
		// 	"[GET] Key: %.*s (key size = %u): acquiring lock.",
		// 	( int ) header.keySize, header.key, header.keySize
		// );

		MasterWorker::slaveSockets->get( sockfd )->counter.increaseDegraded();
		return this->sendDegradedLockRequest(
			event.id, PROTO_OPCODE_GET,
			listId,
			chunkId, newChunkId,
			0, 0, // parity chunk (not needed)
			key.data, key.size
		);
	} else {
		MasterWorker::slaveSockets->get( sockfd )->counter.increaseNormal();

		buffer.data = this->protocol.reqGet(
			buffer.size, requestId,
			header.key, header.keySize
		);

		key.ptr = ( void * ) socket;
		if ( ! MasterWorker::pending->insertKey( PT_SLAVE_GET, requestId, event.id, ( void * ) socket, key ) ) {
			__ERROR__( "MasterWorker", "handleGetRequest", "Cannot insert into slave GET pending map." );
		}

		if ( MasterWorker::updateInterval ) {
			// Mark the time when request is sent
			MasterWorker::pending->recordRequestStartTime( PT_SLAVE_GET, requestId, event.id, ( void * ) socket, socket->getAddr() );
		}

		// Send GET request
		sentBytes = socket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "MasterWorker", "handleGetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
			return false;
		}

		return true;
	}
}

bool MasterWorker::handleUpdateRequest( ApplicationEvent event, char *buf, size_t size ) {
	struct KeyValueUpdateHeader header;
	if ( ! this->protocol.parseKeyValueUpdateHeader( header, true, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleUpdateRequest", "Invalid UPDATE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleUpdateRequest",
		"[UPDATE] Key: %.*s (key size = %u); Value: (offset = %u, value update size = %u)",
		( int ) header.keySize, header.key, header.keySize,
		header.valueUpdateOffset, header.valueUpdateSize
	);

	uint32_t listId, dataChunkId, newDataChunkId, parityChunkId, newParityChunkId;
	bool connected, useDegradedMode;
	SlaveSocket *socket, *target;

	socket = this->getSlaves(
		header.key, header.keySize,
		listId,
		dataChunkId, newDataChunkId,
		parityChunkId, newParityChunkId,
		useDegradedMode
	);

	if ( ! socket ) {
		KeyValueUpdate keyValueUpdate;
		keyValueUpdate.set( header.keySize, header.key, event.socket );
		keyValueUpdate.offset = header.valueUpdateOffset;
		keyValueUpdate.length = header.valueUpdateSize;
		event.resUpdate( event.socket, event.id, keyValueUpdate, false, false );
		this->dispatch( event );
		return false;
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	KeyValueUpdate keyValueUpdate;
	ssize_t sentBytes;
	uint32_t requestId = MasterWorker::idGenerator->nextVal( this->workerId );
	int sockfd;

	char* valueUpdate = new char [ header.valueUpdateSize ];
	memcpy( valueUpdate, header.valueUpdate, header.valueUpdateSize );
	keyValueUpdate.dup( header.keySize, header.key, valueUpdate );
	keyValueUpdate.offset = header.valueUpdateOffset;
	keyValueUpdate.length = header.valueUpdateSize;
	if ( ! MasterWorker::pending->insertKeyValueUpdate( PT_APPLICATION_UPDATE, event.id, ( void * ) event.socket, keyValueUpdate ) ) {
		__ERROR__( "MasterWorker", "handleUpdateRequest", "Cannot insert into application UPDATE pending map." );
	}

	if ( useDegradedMode ) {
		if ( parityChunkId == 0 ) {
			// Redirecting data server
			__INFO__( CYAN, "Master", "handleUpdateRequest", "Redirecting UPDATE request for data server at list %u: (%u --> %u).", listId, dataChunkId, newDataChunkId );
			target = newDataChunkId < MasterWorker::dataChunkCount ? this->dataSlaveSockets[ newDataChunkId ] : this->paritySlaveSockets[ newDataChunkId - MasterWorker::dataChunkCount ];
		} else {
			__INFO__( CYAN, "Master", "handleUpdateRequest", "Redirecting UPDATE request for parity server at list %u: (%u --> %u).", listId, parityChunkId, newParityChunkId );
			target = newParityChunkId < MasterWorker::dataChunkCount ? this->dataSlaveSockets[ newParityChunkId ] : this->paritySlaveSockets[ newParityChunkId - MasterWorker::dataChunkCount ];
		}

		// Acquire degraded lock from the coordinator
		sockfd = target->getSocket();
		MasterWorker::slaveSockets->get( sockfd )->counter.increaseDegraded();
		return this->sendDegradedLockRequest(
			event.id, PROTO_OPCODE_UPDATE,
			listId,
			dataChunkId, newDataChunkId,
			parityChunkId, newParityChunkId,
			keyValueUpdate.data, keyValueUpdate.size,
			keyValueUpdate.length,
			keyValueUpdate.offset,
			( char * ) keyValueUpdate.ptr
		);
	} else {
		sockfd = socket->getSocket();
		MasterWorker::slaveSockets->get( sockfd )->counter.increaseNormal();

		buffer.data = this->protocol.reqUpdate(
			buffer.size, requestId,
			header.key, header.keySize,
			header.valueUpdate, header.valueUpdateOffset, header.valueUpdateSize
		);

		keyValueUpdate.ptr = ( void * ) socket;
		if ( ! MasterWorker::pending->insertKeyValueUpdate( PT_SLAVE_UPDATE, requestId, event.id, ( void * ) socket, keyValueUpdate ) ) {
			__ERROR__( "MasterWorker", "handleUpdateRequest", "Cannot insert into slave UPDATE pending map." );
		}

		// Send UPDATE request
		sentBytes = socket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "MasterWorker", "handleUpdateRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
			return false;
		}

		return true;
	}
}

bool MasterWorker::handleDeleteRequest( ApplicationEvent event, char *buf, size_t size ) {
	struct KeyHeader header;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleDeleteRequest", "Invalid DELETE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleDeleteRequest",
		"[DELETE] Key: %.*s (key size = %u).",
		( int ) header.keySize, header.key, header.keySize
	);

	uint32_t listId, dataChunkId, newDataChunkId, parityChunkId, newParityChunkId;
	bool connected, useDegradedMode;
	SlaveSocket *socket, *target;

	socket = this->getSlaves(
		header.key, header.keySize,
		listId,
		dataChunkId, newDataChunkId,
		parityChunkId, newParityChunkId,
		useDegradedMode
	);

	if ( ! socket ) {
		Key key;
		key.set( header.keySize, header.key );
		event.resDelete( event.socket, event.id, key, false, false );
		this->dispatch( event );
		return false;
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	ssize_t sentBytes;
	uint32_t requestId = MasterWorker::idGenerator->nextVal( this->workerId );
	int sockfd = socket->getSocket();

	key.dup( header.keySize, header.key, ( void * ) event.socket );
	if ( ! MasterWorker::pending->insertKey( PT_APPLICATION_DEL, event.id, ( void * ) event.socket, key ) ) {
		__ERROR__( "MasterWorker", "handleDeleteRequest", "Cannot insert into application DELETE pending map." );
	}

	if ( useDegradedMode ) {
		if ( parityChunkId == 0 ) {
			// Redirecting data server
			__INFO__( CYAN, "Master", "handleDeleteRequest", "Redirecting DELETE request for data server at list %u: (%u --> %u).", listId, dataChunkId, newDataChunkId );
			target = newDataChunkId < MasterWorker::dataChunkCount ? this->dataSlaveSockets[ newDataChunkId ] : this->paritySlaveSockets[ newDataChunkId - MasterWorker::dataChunkCount ];
		} else {
			__INFO__( CYAN, "Master", "handleDeleteRequest", "Redirecting DELETE request for parity server at list %u: (%u --> %u).", listId, parityChunkId, newParityChunkId );
			target = newParityChunkId < MasterWorker::dataChunkCount ? this->dataSlaveSockets[ newParityChunkId ] : this->paritySlaveSockets[ newParityChunkId - MasterWorker::dataChunkCount ];
		}

		// Acquire degraded lock from the coordinator
		sockfd = target->getSocket();
		MasterWorker::slaveSockets->get( sockfd )->counter.increaseDegraded();
		return this->sendDegradedLockRequest(
			event.id, PROTO_OPCODE_DELETE,
			listId,
			dataChunkId, newDataChunkId,
			parityChunkId, newParityChunkId,
			key.data, key.size
		);
	} else {
		MasterWorker::slaveSockets->get( sockfd )->counter.increaseNormal();

		buffer.data = this->protocol.reqDelete(
			buffer.size, requestId,
			header.key, header.keySize
		);

		key.ptr = ( void * ) socket;
		if ( ! MasterWorker::pending->insertKey( PT_SLAVE_DEL, requestId, event.id, ( void * ) socket, key ) ) {
			__ERROR__( "MasterWorker", "handleDeleteRequest", "Cannot insert into slave DELETE pending map." );
		}

		// Send DELETE requests
		sentBytes = socket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "MasterWorker", "handleDeleteRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
			return false;
		}

		return true;
	}
}
