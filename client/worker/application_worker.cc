#include "worker.hh"
#include "../main/client.hh"
#include "../../common/ds/instance_id_generator.hh"

void ClientWorker::dispatch( ApplicationEvent event ) {
	bool success = true, connected, isSend, isReplay = false;
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
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS_WITH_NAMED_PIPE:
			success = true;
			isSend = false;
			break;
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
		case APPLICATION_EVENT_TYPE_GET_RESPONSE_FAILURE:
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE:
		case APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
		case APPLICATION_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
			success = false;
			isSend = true;
			break;
		case APPLICATION_EVENT_TYPE_REPLAY_SET:
		case APPLICATION_EVENT_TYPE_REPLAY_GET:
		case APPLICATION_EVENT_TYPE_REPLAY_UPDATE:
		case APPLICATION_EVENT_TYPE_REPLAY_DEL:
			isReplay = true;
		case APPLICATION_EVENT_TYPE_PENDING:
		default:
			isSend = false;
			break;
	}

	buffer.data = this->protocol.buffer.send;
	buffer.size = 0;

	switch( event.type ) {
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_APPLICATION,
				PROTO_OPCODE_REGISTER,
				0, // length
				InstanceIdGenerator::getInstance()->generate( event.socket ),
				event.requestId
			);
			break;
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS_WITH_NAMED_PIPE:
		{
			Client *client = Client::getInstance();
			ClientSocket &clientSocket = client->sockets.self;
			NamedPipe &namedPipe = client->sockets.namedPipe;
			struct pipe_t {
				int fd;
				char *tmp;
				char pathname[ NAMED_PIPE_PATHNAME_MAX_LENGTH ];
			} pRead, pWrite;

			pRead.tmp = namedPipe.mkfifo();
			pWrite.tmp = namedPipe.mkfifo();

			namedPipe.getFullPath( pRead.pathname, pRead.tmp );
			namedPipe.getFullPath( pWrite.pathname, pWrite.tmp );

			// Create response
			buffer.size = this->protocol.generateNamedPipeHeader(
				PROTO_MAGIC_RESPONSE_SUCCESS,
				PROTO_MAGIC_TO_APPLICATION,
				PROTO_OPCODE_REGISTER_NAMED_PIPE,
				InstanceIdGenerator::getInstance()->generate( event.socket ),
				event.requestId,
				strlen( pRead.pathname ), strlen( pWrite.pathname ),
				pRead.pathname, pWrite.pathname
			);
			ret = event.socket->send( buffer.data, buffer.size, connected );
			if ( ret != ( ssize_t ) buffer.size )
				__ERROR__( "ClientWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );

			// Open the pipe
			pRead.fd = namedPipe.open( pRead.tmp, true /* readMode */ );
			pWrite.fd = namedPipe.open( pWrite.tmp, false /* readMode */ );

			// Configure the named pipe
			ClientSocket::setNonBlocking( pRead.fd );
			clientSocket.epoll->add( pRead.fd, EPOLL_EVENT_SET );

			// Create named pipe ApplicationSocket wrapper
			ApplicationSocket *applicationSocket = new ApplicationSocket();
			applicationSocket->initAsNamedPipe(
				pRead.fd, pRead.tmp,
				pWrite.fd, pWrite.tmp
			);
			client->sockets.applications.set( pRead.fd, applicationSocket );
			// clientSocket.done( pRead.fd );
		}
			break;
		case APPLICATION_EVENT_TYPE_GET_RESPONSE_SUCCESS:
			buffer.size = this->protocol.generateKeyValueHeader(
				PROTO_MAGIC_RESPONSE_SUCCESS, PROTO_MAGIC_TO_APPLICATION,
				PROTO_OPCODE_GET,
				event.instanceId, event.requestId,
				event.message.get.keySize,
				event.message.get.keyStr,
				event.message.get.valueSize,
				event.message.get.valueStr
			);
			break;
		case APPLICATION_EVENT_TYPE_GET_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateKeyHeader(
				PROTO_MAGIC_RESPONSE_FAILURE, PROTO_MAGIC_TO_APPLICATION,
				PROTO_OPCODE_GET,
				event.instanceId, event.requestId,
				event.message.key.size,
				event.message.key.data
			);
			if ( event.needsFree )
				event.message.key.free();
			break;
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE:
			if ( event.message.set.isKeyValue ) {
				Key key = event.message.set.data.keyValue.key();
				buffer.size = this->protocol.generateKeyHeader(
					success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
					PROTO_MAGIC_TO_APPLICATION,
					PROTO_OPCODE_SET,
					event.instanceId, event.requestId,
					key.size, key.data
				);
				if ( event.needsFree )
					event.message.set.data.keyValue.free();
			} else {
				Key &key = event.message.set.data.key;
				buffer.size = this->protocol.generateKeyHeader(
					success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
					PROTO_MAGIC_TO_APPLICATION,
					PROTO_OPCODE_SET,
					event.instanceId, event.requestId,
					key.size, key.data
				);
				if ( event.needsFree )
					key.free();
			}
			break;
		case APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateKeyValueUpdateHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_APPLICATION,
				PROTO_OPCODE_UPDATE,
				event.instanceId, event.requestId,
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
			buffer.size = this->protocol.generateKeyHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_APPLICATION,
				PROTO_OPCODE_DELETE,
				event.instanceId, event.requestId,
				event.message.key.size,
				event.message.key.data
			);
			if ( event.needsFree )
				event.message.key.free();
			break;
		case APPLICATION_EVENT_TYPE_REPLAY_SET:
			{
				Key key;
				char *valueStr;
				uint32_t valueSize;
				event.message.replay.set.keyValue._deserialize( key.data, key.size, valueStr, valueSize );
				buffer.data = this->protocol.buffer.recv;
				buffer.size = this->protocol.generateKeyValueHeader(
					PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_CLIENT,
					PROTO_OPCODE_SET,
					event.instanceId, event.requestId,
					key.size, key.data,
					valueSize, valueStr,
					this->protocol.buffer.recv
				);

				buffer.data += PROTO_HEADER_SIZE;
				buffer.size -= PROTO_HEADER_SIZE;
				this->handleSetRequest( event, buffer.data, buffer.size );
				event.message.replay.set.keyValue.free();
				buffer.data -= PROTO_HEADER_SIZE;
				buffer.size += PROTO_HEADER_SIZE;
			}
			break;
		case APPLICATION_EVENT_TYPE_REPLAY_GET:
			{
				buffer.data = this->protocol.buffer.recv;
				buffer.size = this->protocol.generateKeyHeader(
					PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_CLIENT,
					PROTO_OPCODE_GET,
					event.instanceId, event.requestId,
					event.message.replay.get.key.size,
					event.message.replay.get.key.data,
					this->protocol.buffer.recv
				);

				buffer.data += PROTO_HEADER_SIZE;
				buffer.size -= PROTO_HEADER_SIZE;
				this->handleGetRequest( event, buffer.data, buffer.size );
				event.message.replay.get.key.free();
				buffer.data -= PROTO_HEADER_SIZE;
				buffer.size += PROTO_HEADER_SIZE;
			}
			break;
		case APPLICATION_EVENT_TYPE_REPLAY_UPDATE:
			{
				buffer.data = this->protocol.buffer.recv;
				buffer.size = this->protocol.generateKeyValueUpdateHeader(
					PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_CLIENT,
					PROTO_OPCODE_UPDATE,
					event.instanceId, event.requestId,
					event.message.replay.update.keyValueUpdate.size,
					event.message.replay.update.keyValueUpdate.data,
					event.message.replay.update.keyValueUpdate.offset,
					event.message.replay.update.keyValueUpdate.length,
					( char * ) event.message.replay.update.keyValueUpdate.ptr,
					this->protocol.buffer.recv
				);

				buffer.data += PROTO_HEADER_SIZE;
				buffer.size -= PROTO_HEADER_SIZE;
				this->handleUpdateRequest( event, buffer.data, buffer.size );
				event.message.replay.update.keyValueUpdate.free();
				buffer.data -= PROTO_HEADER_SIZE;
				buffer.size += PROTO_HEADER_SIZE;
			}
			break;
		case APPLICATION_EVENT_TYPE_REPLAY_DEL:
			{
				buffer.data = this->protocol.buffer.recv;
				buffer.size = this->protocol.generateKeyHeader(
					PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_CLIENT,
					PROTO_OPCODE_DELETE,
					event.instanceId, event.requestId,
					event.message.replay.del.key.size,
					event.message.replay.del.key.data,
					this->protocol.buffer.recv
				);

				buffer.data += PROTO_HEADER_SIZE;
				buffer.size -= PROTO_HEADER_SIZE;
				this->handleDeleteRequest( event, buffer.data, buffer.size );
				event.message.replay.del.key.free();
				buffer.data -= PROTO_HEADER_SIZE;
				buffer.size += PROTO_HEADER_SIZE;
			}
			break;
		case APPLICATION_EVENT_TYPE_PENDING:
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "ClientWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else if ( ! isReplay ) {
		// Parse requests from applications
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "ClientWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			if ( header.magic != PROTO_MAGIC_REQUEST || header.from != PROTO_MAGIC_FROM_APPLICATION ) {
				__ERROR__( "ClientWorker", "dispatch", "Invalid protocol header." );
			} else {
				event.instanceId = header.instanceId;
				event.requestId = header.requestId;
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
						__ERROR__( "ClientWorker", "dispatch", "Invalid opcode from application." );
						break;
				}
			}
			buffer.data += header.length;
			buffer.size -= header.length;
		}
		if ( connected ) event.socket->done();
	}

	if ( ! connected && ! isReplay ) {
		__DEBUG__( RED, "ClientWorker", "dispatch", "The application is disconnected." );
		// delete event.socket;
	}
}

bool ClientWorker::handleSetRequest( ApplicationEvent event, char *buf, size_t size ) {
	struct KeyValueHeader header;
	if ( ! this->protocol.parseKeyValueHeader( header, buf, size, 0, false ) ) {
		__ERROR__( "ClientWorker", "handleSetRequest", "Invalid SET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ClientWorker", "handleSetRequest",
		"[SET] Key: %.*s (key size = %u); Value: (value size = %u)",
		( int ) header.keySize, header.key, header.keySize, header.valueSize
	);

	uint32_t listId, chunkId;
	ServerSocket *socket;
	Client *client = Client::getInstance();

	socket = this->getServers(
		header.key, header.keySize,
		listId, chunkId
	);

	// if ( ! socket ) {
	// 	Key key;
	// 	key.set( header.keySize, header.key );
	// 	event.resSet( event.socket, event.instanceId, event.requestId, key, false, false );
	// 	this->dispatch( event );
	// 	return false;
	// }

	// Check whether the object size exceesd the chunk size
	uint32_t numOfSplit, splitSize, splitOffset = 0;
	bool isLarge = LargeObjectUtil::isLarge(
		header.keySize, header.valueSize,
		&numOfSplit, &splitSize
	);
	if ( isLarge ) {
		// Value size exceeds the chunk size
		__DEBUG__( YELLOW, "ClientWorker", "handleSetRequest", "Value size (%u) exceeds the chunk size. Number of split = %u; split size = %u.", header.valueSize, numOfSplit, splitSize );
	}

	// decide whether any of the data / parity server needs to use remapping flow
	if ( ! client->config.client.degraded.disabled ) {
		for ( uint32_t i = 0; i < ClientWorker::parityChunkCount; i++ ) {
			struct sockaddr_in addr = this->parityServerSockets[ i ]->getAddr();
			if ( client->stateTransitHandler.useCoordinatedFlow( addr, true, true ) ) {
				return this->handleDegradedSetRequest( event, buf, size );
			}
		}
		for ( uint32_t splitIndex = 0; splitIndex < numOfSplit; splitIndex++ ) {
			struct sockaddr_in addr = this->dataServerSockets[ ( chunkId + splitIndex ) % ClientWorker::dataChunkCount ]->getAddr();
			if ( client->stateTransitHandler.useCoordinatedFlow( addr, true, true ) ) {
				return this->handleDegradedSetRequest( event, buf, size );
			}
		}
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	KeyValue keyValue;
	uint16_t instanceId = Client::instanceId;
	uint32_t requestId = ClientWorker::idGenerator->nextVal( this->workerId );

	keyValue._dup( header.key, header.keySize, header.value, header.valueSize );
	key = keyValue.key();

	// Insert into application pending set
	if ( ! ClientWorker::pending->insertKeyValue( PT_APPLICATION_SET, event.instanceId, event.requestId, ( void * ) event.socket, keyValue, true, true, Client::getInstance()->timestamp.nextVal() ) ) {
		__ERROR__( "ClientWorker", "handleSetRequest", "Cannot insert into application SET pending map." );
	}

	// Insert into server pending set
	for ( uint32_t splitIndex = 0; splitIndex < numOfSplit; splitIndex++ ) {
		socket = this->dataServerSockets[ ( chunkId + splitIndex ) % ClientWorker::dataChunkCount ];

		for ( uint32_t i = 0; i < ClientWorker::parityChunkCount + 1; i++ ) {
			if ( ! ClientWorker::pending->insertKey(
				PT_SERVER_SET, Client::instanceId, event.instanceId, requestId, event.requestId,
				( void * )( i == 0 ? socket : this->parityServerSockets[ i - 1 ] ),
				key
			) ) {
				__ERROR__( "ClientWorker", "handleSetRequest", "Cannot insert into server SET pending map." );
			}
		}
	}

	for ( uint32_t splitIndex = 0; splitIndex < numOfSplit; splitIndex++ ) {
		splitOffset = LargeObjectUtil::getValueOffsetAtSplit( header.keySize, header.valueSize, splitIndex );

		// Split the value
		Packet *packet = client->packetPool.malloc();
		packet->setReferenceCount( 1 + ClientWorker::parityChunkCount );
		buffer.data = packet->data;
		this->protocol.reqSet(
			buffer.size, instanceId, requestId,
			header.key, header.keySize,
			header.value + splitOffset, header.valueSize,
			splitOffset, splitSize,
			buffer.data
		);
		packet->size = buffer.size;

		// fprintf(
		// 	stderr, "#%u [%.*s]: Offset at %u --> data server #%u; request size: %lu.\n",
		// 	splitIndex, header.keySize, header.key, splitOffset,
		// 	( chunkId + splitIndex ) % ClientWorker::dataChunkCount,
		// 	buffer.size
		// );

		// Choose data server
		socket = this->dataServerSockets[ ( chunkId + splitIndex ) % ClientWorker::dataChunkCount ];

		// Send SET requests
		for ( uint32_t i = 0; i < ClientWorker::parityChunkCount + 1; i++ ) {
			if ( ClientWorker::updateInterval ) {
				// Mark the time when request is sent
				ClientWorker::pending->recordRequestStartTime(
					PT_SERVER_SET,
					Client::instanceId, event.instanceId,
					requestId, event.requestId,
					i < ClientWorker::parityChunkCount ? this->parityServerSockets[ i ] : socket,
					i < ClientWorker::parityChunkCount ? this->parityServerSockets[ i ]->getAddr() : socket->getAddr()
				);
			}

			ServerEvent serverEvent;
			serverEvent.send(
				i < ClientWorker::parityChunkCount ? this->parityServerSockets[ i ] : socket,
				packet
			);
#ifdef CLIENT_WORKER_SEND_REPLICAS_PARALLEL
			ClientWorker::eventQueue->prioritizedInsert( serverEvent );
#else
			this->dispatch( serverEvent );
#endif
		}
	}

	return true;
}

bool ClientWorker::handleGetRequest( ApplicationEvent event, char *buf, size_t size ) {
	struct KeyHeader header;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "ClientWorker", "handleGetRequest", "Invalid GET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ClientWorker", "handleGetRequest",
		"[GET] Key: %.*s (key size = %u).",
		( int ) header.keySize, header.key, header.keySize
	);

	return this->handleGetRequest( event, header, false );
}

bool ClientWorker::handleGetRequest( ApplicationEvent event, struct KeyHeader &header, bool isGettingSplit ) {
	uint32_t *original, *reconstructed, reconstructedCount;
	bool useCoordinatedFlow;
	ServerSocket *socket;
	if ( ! this->getServers(
		PROTO_OPCODE_GET,
		header.key, header.keySize,
		original, reconstructed, reconstructedCount,
		socket, useCoordinatedFlow,
		isGettingSplit
	) ) {
		Key key;
		key.set( header.keySize, header.key );
		event.resGet( event.socket, event.instanceId, event.requestId, key, false );
		this->dispatch( event );
		return false;
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	ssize_t sentBytes;
	bool connected;
	uint16_t instanceId = Client::instanceId;
	uint32_t requestId = ClientWorker::idGenerator->nextVal( this->workerId );

	key.dup( header.keySize, header.key, 0 );
	if ( ! isGettingSplit ) {
		if ( ! ClientWorker::pending->insertKey( PT_APPLICATION_GET, event.instanceId, event.requestId, ( void * ) event.socket, key, true, true, Client::getInstance()->timestamp.nextVal() ) ) {
			__ERROR__( "ClientWorker", "handleGetRequest", "Cannot insert into application GET pending map." );
		}
	}

	if ( useCoordinatedFlow ) {
		// Acquire degraded lock from the coordinator
		__DEBUG__(
			BLUE, "ClientWorker", "handleGetRequest",
			"[GET] Key: %.*s (key size = %u): acquiring lock.",
			( int ) header.keySize, header.key, header.keySize
		);
		if ( isGettingSplit ) {
			key.size -= SPLIT_OFFSET_SIZE;
			key.isLarge = true;
		}
		return this->sendDegradedLockRequest(
			event.instanceId, event.requestId, PROTO_OPCODE_GET,
			original, reconstructed, reconstructedCount,
			key.data, key.size, key.isLarge
		);
	} else {
		buffer.data = this->protocol.reqGet(
			buffer.size, instanceId, requestId,
			header.key, header.keySize
		);

		if ( ! ClientWorker::pending->insertKey( PT_SERVER_GET, instanceId, event.instanceId, requestId, event.requestId, ( void * ) socket, key ) ) {
			__ERROR__( "ClientWorker", "handleGetRequest", "Cannot insert into server GET pending map." );
		}

		if ( ClientWorker::updateInterval ) {
			// Mark the time when request is sent
			ClientWorker::pending->recordRequestStartTime( PT_SERVER_GET, instanceId, event.instanceId, requestId, event.requestId, ( void * ) socket, socket->getAddr() );
		}

		// Send GET request
		assert( buffer.data[ 0 ] != 0 && buffer.data[ 1 ] != 0 );
		sentBytes = socket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "ClientWorker", "handleGetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
			return false;
		}

		return true;
	}
}

bool ClientWorker::handleUpdateRequest( ApplicationEvent event, char *buf, size_t size ) {
	struct KeyValueUpdateHeader header;
	if ( ! this->protocol.parseKeyValueUpdateHeader( header, true, buf, size ) ) {
		__ERROR__( "ClientWorker", "handleUpdateRequest", "Invalid UPDATE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ClientWorker", "handleUpdateRequest",
		"[UPDATE] Key: %.*s (key size = %u); Value: (offset = %u, value update size = %u)",
		( int ) header.keySize, header.key, header.keySize,
		header.valueUpdateOffset, header.valueUpdateSize
	);

	uint32_t *original, *reconstructed, reconstructedCount, splitSize, splitStartIndex, splitEndIndex;
	bool useCoordinatedFlow;
	ServerSocket *socket;
	bool isLarge = LargeObjectUtil::isLarge(
		header.keySize,
		header.valueUpdateOffset + header.valueUpdateSize,
		0, &splitSize
	);
	if ( isLarge ) {
		splitStartIndex = LargeObjectUtil::getSplitIndex(
			header.keySize, 0,
			header.valueUpdateOffset,
			isLarge
		);
		splitEndIndex = LargeObjectUtil::getSplitIndex(
			header.keySize, 0,
			header.valueUpdateOffset + header.valueUpdateSize - 1,
			isLarge
		);
	} else {
		splitStartIndex = splitEndIndex = 0;
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	KeyValueUpdate keyValueUpdate;
	ssize_t sentBytes;
	bool connected;
	uint16_t instanceId = Client::instanceId;
	uint32_t requestId = ClientWorker::idGenerator->nextVal( this->workerId );
	// TODO handle degraded mode
	uint32_t requestTimestamp;
	char backup[ SPLIT_OFFSET_SIZE ];
	bool ret = true;

	char* valueUpdate = new char [ header.valueUpdateSize ];
	memcpy( valueUpdate, header.valueUpdate, header.valueUpdateSize );
	keyValueUpdate.dup( header.keySize, header.key, valueUpdate );
	keyValueUpdate.offset = header.valueUpdateOffset;
	keyValueUpdate.length = header.valueUpdateSize;
	keyValueUpdate.remaining = splitEndIndex - splitStartIndex + 1;
	if ( ! ClientWorker::pending->insertKeyValueUpdate( PT_APPLICATION_UPDATE, event.instanceId, event.requestId, ( void * ) event.socket, keyValueUpdate, true, true, Client::getInstance()->timestamp.nextVal() ) ) {
		__ERROR__( "ClientWorker", "handleUpdateRequest", "Cannot insert into application UPDATE pending map." );
	}

	if ( isLarge ) {
		memcpy( backup, header.key + header.keySize, SPLIT_OFFSET_SIZE );
		header.keySize += SPLIT_OFFSET_SIZE;
	}

	for ( uint32_t splitIndex = splitStartIndex; splitIndex <= splitEndIndex; splitIndex++ ) {
		if ( isLarge ) {
			LargeObjectUtil::writeSplitOffset(
				header.key + header.keySize - SPLIT_OFFSET_SIZE,
				LargeObjectUtil::getValueOffsetAtSplit(
					header.keySize - SPLIT_OFFSET_SIZE,
					header.valueUpdateOffset + header.valueUpdateSize,
					splitIndex
				)
			);
		}

		if ( ! this->getServers(
			PROTO_OPCODE_UPDATE,
			header.key, header.keySize,
			original, reconstructed, reconstructedCount,
			socket, useCoordinatedFlow,
			isLarge
		) ) {
			KeyValueUpdate keyValueUpdate;
			keyValueUpdate.set( header.keySize, header.key, event.socket );
			keyValueUpdate.offset = header.valueUpdateOffset;
			keyValueUpdate.length = header.valueUpdateSize;
			event.resUpdate( event.socket, event.instanceId, event.requestId, keyValueUpdate, false, false );
			this->dispatch( event );
			ret = false;
			goto client_update_exit;
		}

		requestTimestamp = socket->timestamp.current.nextVal();

		// Calculate offset for this split
		uint32_t _offset = 0, _size;
		if ( header.valueUpdateOffset > splitIndex * splitSize )
			_offset = header.valueUpdateOffset - splitIndex * splitSize;
		_size = header.valueUpdateOffset + header.valueUpdateSize - splitIndex * splitSize - _offset;
		if ( _offset + _size > splitSize )
			_size = splitSize - _offset;
		if ( _size > splitSize )
			_size = splitSize;

		if ( useCoordinatedFlow ) {
			// Acquire degraded lock from the coordinator
			this->sendDegradedLockRequest(
				event.instanceId, event.requestId, PROTO_OPCODE_UPDATE,
				original, reconstructed, reconstructedCount,
				header.key, header.keySize - ( isLarge ? SPLIT_OFFSET_SIZE : 0 ), isLarge,
				_size,   // keyValueUpdate.length,
				_offset, // keyValueUpdate.offset,
				( char * ) keyValueUpdate.ptr + _offset + splitIndex * splitSize - header.valueUpdateOffset
			);
		} else {
			buffer.data = this->protocol.reqUpdate(
				buffer.size, instanceId, requestId,
				header.key, header.keySize,
				valueUpdate + _offset + splitIndex * splitSize - header.valueUpdateOffset,
				_offset, // header.valueUpdateOffset,
				_size, // header.valueUpdateSize,
				requestTimestamp
			);
			// add pending timestamp to ack
			socket->timestamp.pendingAck.insertUpdate( Timestamp( requestTimestamp ), event.requestId );

			if ( ! ClientWorker::pending->insertKeyValueUpdate( PT_SERVER_UPDATE, Client::instanceId, event.instanceId, requestId, event.requestId, ( void * ) socket, keyValueUpdate, true, true, requestTimestamp ) ) {
				__ERROR__( "ClientWorker", "handleUpdateRequest", "Cannot insert into server UPDATE pending map." );
			}

			// Send UPDATE request
			sentBytes = socket->send( buffer.data, buffer.size, connected );
			if ( sentBytes != ( ssize_t ) buffer.size ) {
				__ERROR__( "ClientWorker", "handleUpdateRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
				ret = false;
				goto client_update_exit;
			}
		}
	}

client_update_exit:
	if ( isLarge ) {
		memcpy( header.key + header.keySize, backup, SPLIT_OFFSET_SIZE );
	}

	return ret;
}

bool ClientWorker::handleDeleteRequest( ApplicationEvent event, char *buf, size_t size ) {
	struct KeyHeader header;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "ClientWorker", "handleDeleteRequest", "Invalid DELETE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ClientWorker", "handleDeleteRequest",
		"[DELETE] Key: %.*s (key size = %u).",
		( int ) header.keySize, header.key, header.keySize
	);

	uint32_t *original, *reconstructed, reconstructedCount;
	bool useCoordinatedFlow;
	ServerSocket *socket;

	if ( ! this->getServers(
		PROTO_OPCODE_DELETE,
		header.key, header.keySize,
		original, reconstructed, reconstructedCount,
		socket, useCoordinatedFlow
	) ) {
		Key key;
		key.set( header.keySize, header.key );
		event.resDelete( event.socket, event.instanceId, event.requestId, key, false, false );
		this->dispatch( event );
		return false;
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	ssize_t sentBytes;
	bool connected;
	uint16_t instanceId = Client::instanceId;
	uint32_t requestId = ClientWorker::idGenerator->nextVal( this->workerId );
	// TODO handle degraded mode
	uint32_t requestTimestamp = socket->timestamp.current.nextVal();

	key.dup( header.keySize, header.key, ( void * ) event.socket );
	if ( ! ClientWorker::pending->insertKey( PT_APPLICATION_DEL, event.instanceId, event.requestId, ( void * ) event.socket, key, true, true, Client::getInstance()->timestamp.nextVal() ) ) {
		__ERROR__( "ClientWorker", "handleDeleteRequest", "Cannot insert into application DELETE pending map." );
	}

	if ( useCoordinatedFlow ) {
		// Acquire degraded lock from the coordinator
		return this->sendDegradedLockRequest(
			event.instanceId, event.requestId, PROTO_OPCODE_DELETE,
			original, reconstructed, reconstructedCount,
			key.data, key.size, false
		);
	} else {
		buffer.data = this->protocol.reqDelete(
			buffer.size, instanceId, requestId,
			header.key, header.keySize,
			requestTimestamp
		);
		// add pending timestamp to ack.
		socket->timestamp.pendingAck.insertDel( Timestamp( requestTimestamp ), event.requestId );

		if ( ! ClientWorker::pending->insertKey( PT_SERVER_DEL, Client::instanceId, event.instanceId, requestId, event.requestId, ( void * ) socket, key, true, true, requestTimestamp ) ) {
			__ERROR__( "ClientWorker", "handleDeleteRequest", "Cannot insert into server DELETE pending map." );
		}

		// Send DELETE requests
		sentBytes = socket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "ClientWorker", "handleDeleteRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
			return false;
		}

		return true;
	}
}
