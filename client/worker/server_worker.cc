#include "worker.hh"
#include "../main/client.hh"

void ClientWorker::dispatch( ServerEvent event ) {
	bool connected, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;
	uint16_t instanceId = Client::instanceId;

	buffer.data = this->protocol.buffer.send;

	switch( event.type ) {
		case SERVER_EVENT_TYPE_REGISTER_REQUEST:
			buffer.size = this->protocol.generateAddressHeader(
				PROTO_MAGIC_REQUEST,
				PROTO_MAGIC_TO_SERVER,
				PROTO_OPCODE_REGISTER,
				instanceId,
				ClientWorker::idGenerator->nextVal( this->workerId ),
				event.message.address.addr,
				event.message.address.port
			);
			isSend = true;
			break;
		case SERVER_EVENT_TYPE_SEND:
			event.message.send.packet->read( buffer.data, buffer.size );
			isSend = true;
			break;
		case SERVER_EVENT_TYPE_SYNC_METADATA:
		{
			uint32_t sealedCount, opsCount;
			bool isCompleted;
			struct sockaddr_in addr = event.socket->getAddr();

			buffer.size = this->protocol.generateMetadataBackupMessage(
				PROTO_MAGIC_HEARTBEAT, PROTO_MAGIC_TO_COORDINATOR,
				PROTO_OPCODE_SYNC,
				instanceId, ClientWorker::idGenerator->nextVal( this->workerId ),
				addr.sin_addr.s_addr, addr.sin_port,
				&event.socket->backup.lock,
				event.socket->backup.sealed, sealedCount,
				event.socket->backup.ops, opsCount,
				isCompleted
			);

			if ( ! isCompleted )
				ClientWorker::eventQueue->insert( event );

			isSend = false; // Send to coordinator instead
		}
			break;
		case SERVER_EVENT_TYPE_ACK_PARITY_DELTA:
		case SERVER_EVENT_TYPE_REVERT_DELTA:
		{
			bool isAck = ( event.type == SERVER_EVENT_TYPE_ACK_PARITY_DELTA );

			uint32_t requestId = ClientWorker::idGenerator->nextVal( this->workerId );

			AcknowledgementInfo ackInfo(
				event.message.ack.condition,
				event.message.ack.lock,
				event.message.ack.counter
			);

			ClientWorker::pending->insertAck(
				( isAck )? PT_ACK_REMOVE_PARITY : PT_ACK_REVERT_DELTA,
				event.socket->instanceId, requestId, 0,
				ackInfo
			);

			std::vector<uint32_t> timestamps;
			std::vector<Key> requests;
			if ( ! event.message.ack.timestamps )
				event.message.ack.timestamps = &timestamps;
			if ( ! event.message.ack.requests )
				event.message.ack.requests = &requests;

			if ( isAck ) {
				buffer.size = this->protocol.generateDeltaAcknowledgementHeader(
					PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
					PROTO_OPCODE_ACK_PARITY_DELTA,
					instanceId, requestId,
					*event.message.ack.timestamps,
					std::vector<Key>(),
					event.message.ack.targetId
				);
			} else {
				buffer.size = this->protocol.generateDeltaAcknowledgementHeader(
					PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
					PROTO_OPCODE_REVERT_DELTA,
					instanceId, requestId,
					*event.message.ack.timestamps,
					*event.message.ack.requests,
					event.message.ack.targetId
				);
			}

			if ( event.message.ack.timestamps != &timestamps )
				delete event.message.ack.timestamps;
			if ( event.message.ack.requests != &requests ) {
				for( Key &k : requests )
					k.free();
				delete event.message.ack.requests;
			}

			isSend = true;
		}
			break;
		case SERVER_EVENT_TYPE_PENDING:
			isSend = false;
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "ClientWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );

		if ( event.type == SERVER_EVENT_TYPE_SEND )
			Client::getInstance()->packetPool.free( event.message.send.packet );
	} else if ( event.type == SERVER_EVENT_TYPE_SYNC_METADATA ) {
		std::vector<CoordinatorSocket *> &coordinators = Client::getInstance()->sockets.coordinators.values;
		for ( int i = 0, len = coordinators.size(); i < len; i++ ) {
			ret = coordinators[ i ]->send( buffer.data, buffer.size, connected );

			if ( ret != ( ssize_t ) buffer.size )
				__ERROR__( "ClientWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
		}
	} else {
		// Parse responses from servers
		ClientStateTransitHandler &csth = Client::getInstance()->stateTransitHandler;
		const struct sockaddr_in &addr = event.socket->getAddr();

		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "ClientWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_SERVER ) {
				__ERROR__( "ClientWorker", "dispatch", "Invalid message source from server." );
			} else {
				bool success;
				switch( header.magic ) {
					case PROTO_MAGIC_RESPONSE_SUCCESS:
					case PROTO_MAGIC_ACKNOWLEDGEMENT:
						success = true;
						break;
					case PROTO_MAGIC_RESPONSE_FAILURE:
						success = false;
						break;
					default:
						__ERROR__( "ClientWorker", "dispatch", "Invalid magic code from server." );
						goto quit_1;
				}

				event.instanceId = header.instanceId;
				event.requestId = header.requestId;
				switch( header.opcode ) {
					case PROTO_OPCODE_REGISTER:
						if ( success ) {
							event.socket->registered = true;
							event.socket->instanceId = header.instanceId;
							Client *client = Client::getInstance();
							LOCK( &client->sockets.serversIdToSocketLock );
							client->sockets.serversIdToSocketMap[ header.instanceId ] = event.socket;
							UNLOCK( &client->sockets.serversIdToSocketLock );
						} else {
							__ERROR__( "ClientWorker", "dispatch", "Failed to register with server." );
						}
						break;
					case PROTO_OPCODE_GET:
						if ( ! csth.acceptNormalResponse( addr ) ) {
							__INFO__( YELLOW, "Client", "dispatch", "Ignoring normal GET response: ID = %u (port: %u).", header.requestId, ntohs( addr.sin_port ) );
							// break;
						}
					case PROTO_OPCODE_DEGRADED_GET:
						this->handleGetResponse( event, success, header.opcode == PROTO_OPCODE_DEGRADED_GET, buffer.data, header.length );
						break;
					case PROTO_OPCODE_SET:
						if ( ! csth.acceptNormalResponse( addr ) ) {
							//__INFO__( YELLOW, "Client", "dispatch", "Ignoring normal SET response..." );
							break;
						}
						this->handleSetResponse( event, success, buffer.data, header.length );
						break;
					case PROTO_OPCODE_DEGRADED_SET:
						this->handleDegradedSetResponse( event, success, buffer.data, header.length );
						break;
					case PROTO_OPCODE_UPDATE:
						if ( ! csth.acceptNormalResponse( addr ) ) {
							// __INFO__( YELLOW, "Client", "dispatch", "[port: %u] Ignoring normal UPDATE response...", ntohs( addr.sin_port ) );
							break;
						}
					case PROTO_OPCODE_DEGRADED_UPDATE:
						this->handleUpdateResponse( event, success, header.opcode == PROTO_OPCODE_DEGRADED_UPDATE, buffer.data, header.length );
						break;
					case PROTO_OPCODE_DELETE:
						if ( ! csth.acceptNormalResponse( addr ) ) {
							// __INFO__( YELLOW, "Client", "dispatch", "Ignoring normal DELETE response..." );
							break;
						}
					case PROTO_OPCODE_DEGRADED_DELETE:
						this->handleDeleteResponse( event, success, header.opcode == PROTO_OPCODE_DEGRADED_DELETE, buffer.data, header.length );
						break;
					case PROTO_OPCODE_ACK_METADATA:
						this->handleAcknowledgement( event, header.opcode, buffer.data, header.length );
						break;
					case PROTO_OPCODE_ACK_PARITY_DELTA:
					case PROTO_OPCODE_REVERT_DELTA:
						this->handleDeltaAcknowledgement( event, header.opcode, buffer.data, header.length );
						break;
					default:
						__ERROR__( "ClientWorker", "dispatch", "Invalid opcode from server." );
						goto quit_1;
				}
			}
quit_1:
			buffer.data += header.length;
			buffer.size -= header.length;
		}
		if ( connected ) event.socket->done();
	}
	if ( ! connected ) {
		__ERROR__( "ClientWorker", "dispatch", "The server is disconnected." );
		this->removePending( event.socket );
	}
}

bool ClientWorker::handleSetResponse( ServerEvent event, bool success, char *buf, size_t size ) {
	uint8_t keySize;
	char *keyStr;
	if ( success ) {
		struct KeyBackupHeader header;
		if ( ! this->protocol.parseKeyBackupHeader( header, buf, size ) ) {
			__ERROR__( "ClientWorker", "handleSetResponse", "Invalid SET response." );
			return false;
		}
		if ( header.isParity ) {
			__DEBUG__(
				BLUE, "ClientWorker", "handleSetResponse",
				"[SET] Key: %.*s (key size = %u); is large? %s",
				( int ) header.keySize, header.key, header.keySize,
				header.isLarge ? "true" : "false"
			);

			keySize = header.keySize;
			keyStr = header.key;
		} else {
			__DEBUG__(
				BLUE, "ClientWorker", "handleSetResponse",
				"[SET] [%u] Key: %.*s.%u (key size = %u) at (%u, %u, %u); is large? %s",
				header.timestamp,
				( int ) header.keySize, header.key,
				header.isLarge ? LargeObjectUtil::readSplitOffset( header.key + header.keySize ) : 0,
				header.keySize,
				header.listId, header.stripeId, header.chunkId,
				header.isLarge ? "true" : "false"
			);

			keySize = header.keySize;
			keyStr = header.key;

			if ( header.sealedCount ) {
				event.socket->backup.insert(
					keySize, keyStr, header.isLarge,
					PROTO_OPCODE_SET,
					header.timestamp,
					header.listId,
					header.stripeId,
					header.chunkId,
					header.sealedCount,
					header.sealed
				);
			} else {
				event.socket->backup.insert(
					keySize, keyStr, header.isLarge,
					PROTO_OPCODE_SET,
					header.timestamp,
					header.listId,
					header.stripeId,
					header.chunkId
				);
			}
		}
	} else {
		struct KeyBackupHeader header;
		if ( ! this->protocol.parseKeyBackupHeader( header, buf, size ) ) {
			__ERROR__( "ClientWorker", "handleSetResponse", "Invalid SET response." );
			return false;
		}
		event.socket->printAddress( stderr );
		__DEBUG__(
			BLUE, "ClientWorker", "handleSetResponse",
			"[SET (failed)] Key: %.*s (key size = %u)",
			( int ) header.keySize, header.key, header.keySize
		);

		keySize = header.keySize;
		keyStr = header.key;
	}

	int pending = 0;
	ApplicationEvent applicationEvent;
	PendingIdentifier pid;
	Key key;
	KeyValue keyValue;
	Client* client = Client::getInstance();

	if ( ! ClientWorker::pending->eraseKey( PT_SERVER_SET, event.instanceId, event.requestId, event.socket, &pid, &key, true, false ) ) {
		UNLOCK( &ClientWorker::pending->servers.setLock );
		__ERROR__( "ClientWorker", "handleSetResponse", "Cannot find a pending server SET request that matches the response. This message will be discarded. (ID: (%u, %u); key: %.*s)", event.instanceId, event.requestId, keySize, keyStr );
		return false;
	}

	// Check pending server SET requests
	pending = ClientWorker::pending->count( PT_SERVER_SET, pid.instanceId, pid.requestId, false, true );

	// Mark the elapse time as latency
	if ( ClientWorker::updateInterval ) {
		struct timespec elapsedTime;
		RequestStartTime rst;

		if ( ! ClientWorker::pending->eraseRequestStartTime( PT_SERVER_SET, pid.instanceId, pid.requestId, ( void * ) event.socket, elapsedTime, 0, &rst ) ) {
			__ERROR__( "ClientWorker", "handleSetResponse", "Cannot find a pending stats SET request that matches the response." );
		} else {
			int index = -1;
			LOCK( &client->serverLoading.lock );
			std::set<Latency> *latencyPool = client->serverLoading.past.set.get( rst.addr, &index );
			// init. the set if it is not there
			if ( index == -1 ) {
				client->serverLoading.past.set.set( rst.addr, new std::set<Latency>() );
			}
			// insert the latency to the set
			// TODO use time when Response came, i.e. event created for latency cal.
			Latency latency = Latency ( elapsedTime );
			if ( index == -1 )
				latencyPool = client->serverLoading.past.set.get( rst.addr );
			latencyPool->insert( latency );
			UNLOCK( &client->serverLoading.lock );
		}
	}

	if ( pending == 0 ) {
		// Only send application SET response when the number of pending server SET requests equal 0
		if ( ! ClientWorker::pending->eraseKeyValue( PT_APPLICATION_SET, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &keyValue, true, true, true, keyStr ) ) {
			__ERROR__( "ClientWorker", "handleSetResponse", "Cannot find a pending application SET request that matches the response. This message will be discarded. (Key = %.*s, ID = (%u, %u))", key.size, key.data, pid.parentInstanceId, pid.parentRequestId );
			return false;
		}

		// not to response if the request is "canceled" due to replay
		if ( pid.ptr ) {
			applicationEvent.resSet( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, keyValue, success );
			this->dispatch( applicationEvent );
		}

		// Check if all normal requests completes
		ServerSocket *server = 0;
		struct sockaddr_in addr;
		ClientStateTransitHandler *stateTransitHandler = &client->stateTransitHandler;
		client->stripeList->get( keyStr, key.size, 0, this->parityServerSockets );
		for ( uint32_t i = 0; i < this->parityChunkCount; i++ ) {
			server = this->parityServerSockets[ i ];
			addr = server->getAddr();
			// if ( ! stateTransitHandler->useCoordinatedFlow( addr, true, true ) || stateTransitHandler->stateTransitInfo.at( addr ).isCompleted() )
			// 	continue;
			__DEBUG__( GREEN, "ClientWorker", "handleSetResponse", "Ack transition for server id = %u (request ID = %u).", server->instanceId, pid.requestId );
			if ( stateTransitHandler->stateTransitInfo.at( addr ).removePendingRequest( pid.requestId ) == 0 && stateTransitHandler->stateTransitInfo.at( addr ).setCompleted() ) {
				// __INFO__( GREEN, "ClientWorker", "handleSetResponse", "Ack transition for server id = %u (request ID = %u).", server->instanceId, pid.requestId );
				stateTransitHandler->ackTransit( addr );
			}
		}

	}

	return true;
}

bool ClientWorker::handleGetResponse( ServerEvent event, bool success, bool isDegraded, char *buf, size_t size ) {
	Key key;
	uint32_t valueSize = 0, splitSize, numOfSplit;
	char *valueStr = 0;
	void *memToBeFreed = 0;
	bool isLarge = false;
	ApplicationEvent applicationEvent;
	PendingIdentifier pid;
	Client* client = Client::getInstance();
	struct KeyValueHeader header;

	if ( success ) {
		if ( this->protocol.parseKeyValueHeader( header, buf, size, 0, true ) ) {
			key.set( header.keySize, header.key, ( void * ) event.socket );
			valueSize = header.valueSize;
			valueStr = header.value;

			isLarge = LargeObjectUtil::isLarge( header.keySize, header.valueSize, &numOfSplit, &splitSize );

			__DEBUG__(
				BLUE, "ClientWorker", "handleGetResponse",
				"[GET] Key: %.*s (key size = %u); Value: (value size = %u); split offset = %u, split size = %u, is large? %s; buffer size = %lu.",
				header.keySize, header.key, header.keySize,
				header.valueSize, header.splitOffset, splitSize,
				isLarge ? "yes" : "no",
				size
			);
		} else {
			__ERROR__( "ClientWorker", "handleGetResponse", "Invalid GET response." );
			return false;
		}
	} else {
		struct KeyHeader header;
		if ( this->protocol.parseKeyHeader( header, buf, size ) ) {
			key.set( header.keySize, header.key, ( void * ) event.socket );

			__DEBUG__(
				BLUE, "ClientWorker", "handleGetResponse",
				"[GET (Failed)] Key: %.*s (key size = %u).",
				header.keySize, header.key, header.keySize
			);

			uint32_t listId, chunkId;
			ServerSocket *originalDataServer = this->getServers( key.data, key.size, listId, chunkId );
			if ( originalDataServer != event.socket ) {
				fprintf( stderr, "MISMATCH: key: %.*s (size=%u)!\n", key.size, key.data, key.size );
				event.socket->printAddress( stderr );

				if ( ! ClientWorker::pending->eraseKey( PT_SERVER_GET, event.instanceId, event.requestId, event.socket, &pid, &key, true, true ) ) {
					__ERROR__( "ClientWorker", "handleGetResponse", "Cannot find a pending server GET request that matches the response. This message will be discarded (key = %.*s).", key.size, key.data );
					return false;
				}

				// Retry on original server
				struct {
					size_t size;
					char *data;
				} buffer;
				ssize_t sentBytes;
				bool connected;

				buffer.data = this->protocol.reqGet(
					buffer.size, pid.instanceId, pid.requestId,
					key.data, key.size
				);

				if ( ! ClientWorker::pending->insertKey( PT_SERVER_GET, pid.instanceId, pid.parentInstanceId, pid.requestId, pid.parentRequestId, ( void * ) originalDataServer, key ) ) {
					__ERROR__( "ClientWorker", "handleGetRequest", "Cannot insert into server GET pending map." );
				}

				if ( ClientWorker::updateInterval ) {
					// Mark the time when request is sent
					ClientWorker::pending->recordRequestStartTime( PT_SERVER_GET, pid.instanceId, pid.parentInstanceId, pid.requestId, pid.parentRequestId, ( void * ) originalDataServer, originalDataServer->getAddr() );
				}

				// Send GET request
				sentBytes = originalDataServer->send( buffer.data, buffer.size, connected );
				if ( sentBytes != ( ssize_t ) buffer.size ) {
					__ERROR__( "ClientWorker", "handleGetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
					return false;
				}

				return true;
			}

			if ( ! isDegraded ) {
				event.socket->printAddress( stderr );
				__ERROR__( "ClientWorker", "handleGetResponse", "GET request id = %u failed: key = %.*s", event.requestId, header.keySize, header.key );
			}
		} else {
			__ERROR__( "ClientWorker", "handleGetResponse", "Invalid GET response." );
			return false;
		}
	}

	if ( ! ClientWorker::pending->eraseKey( PT_SERVER_GET, event.instanceId, event.requestId, event.socket, &pid, &key, true, true ) ) {
		__ERROR__( "ClientWorker", "handleGetResponse", "Cannot find a pending server GET request that matches the response. This message will be discarded (key = %.*s).", key.size, key.data );
		return false;
	}

	// Mark the elapse time as latency
	if ( ! isDegraded && ClientWorker::updateInterval ) {
		struct timespec elapsedTime;
		RequestStartTime rst;

		if ( ! ClientWorker::pending->eraseRequestStartTime( PT_SERVER_GET, pid.instanceId, pid.requestId, ( void * ) event.socket, elapsedTime, 0, &rst ) ) {
			__ERROR__( "ClientWorker", "handleGetResponse", "Cannot find a pending stats GET request that matches the response (ID: (%u, %u)).", pid.instanceId, pid.requestId );
		} else {
			int index = -1;
			LOCK( &client->serverLoading.lock );
			std::set<Latency> *latencyPool = client->serverLoading.past.get.get( rst.addr, &index );
			// init. the set if it is not there
			if ( index == -1 ) {
				client->serverLoading.past.get.set( rst.addr, new std::set<Latency>() );
			}
			// insert the latency to the set
			// TODO use time when Response came, i.e. event created for latency cal.
			Latency latency = Latency ( elapsedTime );
			if ( index == -1 )
				latencyPool = client->serverLoading.past.get.get( rst.addr );
			latencyPool->insert( latency );
			UNLOCK( &client->serverLoading.lock );
		}
	}

	// Key serverKeyBackup = key;

	if ( isLarge ) {
		uint32_t splitIndex = LargeObjectUtil::getSplitIndex( header.keySize, header.valueSize, header.splitOffset, isLarge );
		// if ( header.splitOffset != 0 )
		// 	serverKeyBackup.free();

		if ( ! ClientWorker::pending->findKey( PT_APPLICATION_GET, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &key, true, true, true, key.data, numOfSplit + valueSize ) ) {
			// ::free( key.ptr );
			__ERROR__( "ClientWorker", "handleGetResponse", "1 Cannot find a pending application GET request that matches the response. This message will be discarded (key = %.*s).", key.size, key.data );
			return false;
		}

		pthread_mutex_t *lock = ( pthread_mutex_t * )( ( char * ) key.ptr + numOfSplit + valueSize );

		// Store split
		// fprintf( stderr, "key: %.*s; splitOffset = %u, splitIndex = %u; key.ptr = %p\n", header.keySize, header.key, header.splitOffset, splitIndex, key.ptr );
		pthread_mutex_lock( lock );
		if ( header.splitOffset + splitSize > header.valueSize )
			splitSize = header.valueSize - header.splitOffset;
		memcpy( ( char * ) key.ptr + numOfSplit + header.splitOffset, header.value, splitSize );
		( ( char * ) key.ptr )[ splitIndex ] = 1;

		if ( header.splitOffset == 0 ) {
			pthread_mutex_unlock( lock );

			// Get remaining split
			ApplicationEvent applicationEvent;
			applicationEvent.socket = ( ApplicationSocket * ) pid.ptr;
			applicationEvent.instanceId = pid.instanceId;
			applicationEvent.requestId = pid.requestId;

			struct KeyHeader keyHeader;
			keyHeader.key = header.key;
			keyHeader.keySize = header.keySize + SPLIT_OFFSET_SIZE;

			char backup[ SPLIT_OFFSET_SIZE ];
			memcpy( backup, keyHeader.key + keyHeader.keySize - SPLIT_OFFSET_SIZE, SPLIT_OFFSET_SIZE );

			for ( uint32_t sOffset = splitSize; sOffset < valueSize; sOffset += splitSize ) {
				LargeObjectUtil::writeSplitOffset(
					keyHeader.key + keyHeader.keySize - SPLIT_OFFSET_SIZE, // buf
					sOffset
				);
				this->handleGetRequest(
					applicationEvent,
					keyHeader,
					true
				);
			}

			memcpy( keyHeader.key + keyHeader.keySize - SPLIT_OFFSET_SIZE, backup, SPLIT_OFFSET_SIZE );

			return true;
		} else {
			// Check if all split are received
			bool isCompleted = true;
			for ( uint32_t i = 0; i < numOfSplit; i++ ) {
				if ( ! ( ( char * ) key.ptr )[ i ] ) {
					isCompleted = false;
					break;
				}
			}

			pthread_mutex_unlock( lock );

			if ( ! isCompleted )
				return true;
			else {
				valueStr = ( char * )( key.ptr ) + numOfSplit;
				memToBeFreed = key.ptr;
			}
		}
	}

	if ( ! ClientWorker::pending->eraseKey( PT_APPLICATION_GET, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &key, true, true, true, key.data ) ) {
		__ERROR__( "ClientWorker", "handleGetResponse", "2 Cannot find a pending application GET request that matches the response. This message will be discarded (key = %.*s, size = %u, is large? %s).", key.size, key.data, key.size, key.isLarge ? "true" : "false" );
		return false;
	}

	if ( pid.ptr ) {
		if ( success ) {
			applicationEvent.resGet(
				( ApplicationSocket * ) pid.ptr,
				pid.instanceId, pid.requestId,
				key.size,
				valueSize,
				key.data,
				valueStr,
				false
			);
		} else {
			uint32_t dataChunkIndex;
			client->stripeList->get( key.data, key.size, this->dataServerSockets, 0, &dataChunkIndex );
			if ( isDegraded && ! client->stateTransitHandler.useCoordinatedFlow( this->dataServerSockets[ dataChunkIndex ]->getAddr() ) ) {
				// degraded GET failed, but server returns to normal
				__DEBUG__( CYAN, "ClientWorker", "handleGetResponse", "Retry on failed degraded GET request id = %u", pid.requestId );
				applicationEvent.replayGetRequest( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, key );
			} else {
				// event.socket->printAddress();
				// printf( ": handleGetResponse(): Key %.*s not found.\n", key.size, key.data );
				if ( ! isDegraded ) {
					__ERROR__( "ClientWorker", "handleGetResponse", "GET request id = %u failed ", pid.requestId );
				}
				applicationEvent.resGet( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, key, false );
			}
		}
		this->dispatch( applicationEvent );
	}
	key.free();
	if ( memToBeFreed )
		::free( memToBeFreed );
	return true;
}

bool ClientWorker::handleUpdateResponse( ServerEvent event, bool success, bool isDegraded, char *buf, size_t size ) {
	struct KeyValueUpdateHeader header;
	if ( ! this->protocol.parseKeyValueUpdateHeader( header, false, buf, size ) ) {
		__ERROR__( "ClientWorker", "handleUpdateResponse", "Invalid UPDATE Response." );
		return false;
	}

	if ( ! success && ! isDegraded ) {
		__ERROR__( "ClientWorker", "handleUpdateResponse",
			"[UPDATE (%s)] From %d Updated key: %.*s (key size = %u); update value size = %u at offset: %u.",
			success ? "Success" : "Fail",
			event.socket->instanceId,
			( int ) header.keySize, header.key, header.keySize,
			header.valueUpdateSize, header.valueUpdateOffset
		);
		fprintf( stderr, "----- WAITING FOR RETRY ----------\n" );
		return false;
	}

	KeyValueUpdate keyValueUpdate;
	ApplicationEvent applicationEvent;
	PendingIdentifier pid;

	// Find the cooresponding request
	if ( ! ClientWorker::pending->eraseKeyValueUpdate( PT_SERVER_UPDATE, event.instanceId, event.requestId, ( void * ) event.socket, &pid, &keyValueUpdate ) ) {
		__ERROR__( "ClientWorker", "handleUpdateResponse", "Cannot find a pending server UPDATE request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	uint32_t timestamp = pid.timestamp;

	if ( ! ClientWorker::pending->eraseKeyValueUpdate( PT_APPLICATION_UPDATE, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &keyValueUpdate, true, true, true, header.key ) ) {
		__ERROR__( "ClientWorker", "handleUpdateResponse", "Cannot find a pending application UPDATE request that matches the response. This message will be discarded (%u, %u).", pid.parentInstanceId, pid.parentRequestId );
		return false;
	}
	// remove pending timestamp
	// TODO handle degraded mode
	Client *client = Client::getInstance();
	if ( ! isDegraded ) {
		event.socket->timestamp.pendingAck.eraseUpdate( timestamp, pid.requestId );
	}

	uint32_t dataChunkIndex;
	client->stripeList->get( header.key, header.keySize, this->dataServerSockets, 0, &dataChunkIndex );
	bool needsReplay = pid.ptr && isDegraded && ! success && ! client->stateTransitHandler.useCoordinatedFlow( this->dataServerSockets[ dataChunkIndex ]->getAddr() );

	if ( pid.ptr ) {
		if ( needsReplay ) {
			// make a copy of the update
			KeyValueUpdate kv;
			kv.size = keyValueUpdate.size;
			kv.dup( keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.ptr );
			kv.offset = keyValueUpdate.offset;
			kv.length = keyValueUpdate.length;
			kv.isDegraded = keyValueUpdate.isDegraded;
			applicationEvent.replayUpdateRequest( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, kv );
			// do not use dispatch, since ClientWorker::replayUpdate() overwrites recv buffer
			ClientWorker::eventQueue->insert( applicationEvent );
		} else if ( keyValueUpdate.remaining == 0 ) {
			applicationEvent.resUpdate( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, keyValueUpdate, success, false );
			this->dispatch( applicationEvent );
		}
	}

	if ( ! needsReplay && keyValueUpdate.remaining == 0 ) {
		// free the updated value
		delete[] ( ( char * )( keyValueUpdate.ptr ) );
	}

	// Check if all normal requests completes
	if ( ! isDegraded ) {
		ServerSocket *server = 0;
		struct sockaddr_in addr;
		ClientStateTransitHandler *stateTransitHandler = &client->stateTransitHandler;
		client->stripeList->get( header.key, header.keySize, 0, this->parityServerSockets );
		for ( uint32_t i = 0; i < this->parityChunkCount; i++ ) {
			server = this->parityServerSockets[ i ];
			addr = server->getAddr();
			if ( ! stateTransitHandler->useCoordinatedFlow( addr, true, true ) || stateTransitHandler->stateTransitInfo.at( addr ).isCompleted() )
				continue;
			if ( stateTransitHandler->stateTransitInfo.at( addr ).removePendingRequest( pid.requestId ) == 0 ) {
				__INFO__( GREEN, "ClientWorker", "handleUpdateResponse", "Ack transition for server id = %u.", server->instanceId );
				stateTransitHandler->stateTransitInfo.at( addr ).setCompleted();
				stateTransitHandler->ackTransit( addr );
			}
		}
	}

	// check if ack is necessary
	// TODO handle degraded mode
	if ( ! isDegraded )
		client->ackParityDelta( 0, event.socket );

	return true;
}

bool ClientWorker::handleDeleteResponse( ServerEvent event, bool success, bool isDegraded, char *buf, size_t size ) {
	char *keyStr;
	uint8_t keySize;
	if ( success ) {
		struct KeyBackupHeader header;
		if ( ! this->protocol.parseKeyBackupHeader( header, buf, size ) ) {
			__ERROR__( "ClientWorker", "handleDeleteResponse", "Invalid DELETE Response." );
			return false;
		}
		keyStr = header.key;
		keySize = header.keySize;

		if ( header.sealedCount ) {
			event.socket->backup.insert(
				keySize, keyStr, header.isLarge,
				PROTO_OPCODE_DELETE,
				header.timestamp,
				header.listId,
				header.stripeId,
				header.chunkId,
				header.sealedCount,
				header.sealed
			);
		} else {
			event.socket->backup.insert(
				keySize, keyStr, header.isLarge,
				PROTO_OPCODE_DELETE,
				header.timestamp,
				header.listId,
				header.stripeId,
				header.chunkId
			);
		}
	} else {
		struct KeyHeader header;
		if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
			__ERROR__( "ClientWorker", "handleDeleteResponse", "Invalid DELETE Response." );
			return false;
		}
		keyStr = header.key;
		keySize = header.keySize;
	}

	ApplicationEvent applicationEvent;
	PendingIdentifier pid;
	Key key;

	if ( ! ClientWorker::pending->eraseKey( PT_SERVER_DEL, event.instanceId, event.requestId, ( void * ) event.socket, &pid, &key ) ) {
		__ERROR__( "ClientWorker", "handleDeleteResponse", "Cannot find a pending server DELETE request that matches the response. This message will be discarded." );
		return false;
	}

	uint32_t timestamp = pid.timestamp;

	if ( ! ClientWorker::pending->eraseKey( PT_APPLICATION_DEL, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &key, true, true, true, keyStr ) ) {
		__ERROR__( "ClientWorker", "handleDeleteResponse", "Cannot find a pending application DELETE request that matches the response. This message will be discarded. ID: (%u, %u); key: %.*s.", pid.parentInstanceId, pid.parentRequestId, keySize, keyStr );
		return false;
	}

	// remove pending timestamp
	// TODO handle degraded mode
	Client *client = Client::getInstance();
	if ( !isDegraded ) {
		event.socket->timestamp.pendingAck.eraseDel( timestamp, pid.requestId );
	}

	if ( pid.ptr ) {
		applicationEvent.resDelete( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, key, success );
		this->dispatch( applicationEvent );
	}

	// Check if all normal requests completes
	ServerSocket *server = 0;
	struct sockaddr_in addr;
	ClientStateTransitHandler *stateTransitHandler = &client->stateTransitHandler;
	client->stripeList->get( keyStr, key.size, 0, this->parityServerSockets );
	for ( uint32_t i = 0; i < this->parityChunkCount; i++ ) {
		server = this->parityServerSockets[ i ];
		addr = server->getAddr();
		if ( ! stateTransitHandler->useCoordinatedFlow( addr, true, true ) || stateTransitHandler->stateTransitInfo.at( addr ).isCompleted() )
			continue;
		if ( stateTransitHandler->stateTransitInfo.at( addr ).removePendingRequest( pid.requestId ) == 0 ) {
			__INFO__( GREEN, "ClientWorker", "handleDeleteResponse", "Ack transition for server id = %u.", server->instanceId );
			stateTransitHandler->stateTransitInfo.at( addr ).setCompleted();
			stateTransitHandler->ackTransit( addr );
		}
	}

	// check if ack is necessary
	// TODO handle degraded mode
	if ( ! isDegraded ) client->ackParityDelta( 0, event.socket );

	return true;
}

bool ClientWorker::handleAcknowledgement( ServerEvent event, uint8_t opcode, char *buf, size_t size ) {
	struct AcknowledgementHeader header;
	if ( ! this->protocol.parseAcknowledgementHeader( header, buf, size ) ) {
		__ERROR__( "ClientWorker", "handleAcknowledgement", "Invalid ACK." );
		return false;
	}

	__DEBUG__( YELLOW, "ClientWorker", "handleAcknowledgement", "Timestamp = (%u, %u).", header.fromTimestamp, header.toTimestamp );

	event.socket->backup.erase( header.fromTimestamp, header.toTimestamp );

	return true;
}

bool ClientWorker::handleDeltaAcknowledgement( ServerEvent event, uint8_t opcode, char *buf, size_t size ) {
	struct DeltaAcknowledgementHeader header;
	PendingIdentifier pid;
	if ( ! this->protocol.parseDeltaAcknowledgementHeader( header, 0, 0, buf, size ) ) {
		__ERROR__( "ClientWorker", "handleDeltaAcknowledgement", "Invalid ACK." );
		return false;
	}

	bool ret = true;
	AcknowledgementInfo ackInfo;

	switch( opcode ) {
		case PROTO_OPCODE_ACK_PARITY_DELTA:
			ret = ClientWorker::pending->eraseAck( PT_ACK_REMOVE_PARITY, event.instanceId, event.requestId, 0, &pid, &ackInfo );
			break;
		case PROTO_OPCODE_REVERT_DELTA:
			ret = ClientWorker::pending->eraseAck( PT_ACK_REVERT_DELTA, event.instanceId, event.requestId, 0, &pid, &ackInfo );
			break;
		default:
			__ERROR__( "ClientWorker", "handleDeltaAcknowledgement",
					"Unknown ack for instance id = %u from instance id = %u request id = %u",
					header.targetId, event.instanceId, event.requestId
			);
			return false;
	}

	if ( ! ret ) {
		__ERROR__( "ClientWorker", "handleDeltaAcknowledgement",
			"Cannot find the pending ack info for instance id = %u from instance id = %u request id = %u",
			header.targetId, event.instanceId, event.requestId
		);
		return ret;
	}

	if ( ackInfo.lock ) LOCK( ackInfo.lock );
	if ( ackInfo.counter ) {
		*ackInfo.counter -= 1;
		if ( *ackInfo.counter == 0 ) {
			__INFO__( GREEN, "ClientWorker", "handleDeltaAcknowledgement",
					"Complete ack for counter at %p", ackInfo.counter
			);
			if ( ackInfo.condition )
				pthread_cond_signal( ackInfo.condition );
		}
	}
	if ( ackInfo.lock ) UNLOCK( ackInfo.lock );

	Client *client = Client::getInstance();
	switch( opcode ) {
		case PROTO_OPCODE_ACK_PARITY_DELTA:
			break;
		case PROTO_OPCODE_REVERT_DELTA:
			LOCK( &client->sockets.serversIdToSocketLock );
			try {
				struct sockaddr_in saddr = client->sockets.serversIdToSocketMap.at( header.targetId )->getAddr();
				client->stateTransitHandler.ackTransit( saddr );
			} catch ( std::out_of_range &e ) {
				__ERROR__( "ClientWorker", "handleDeltaAcknowledgement",
					"Cannot find server socket for instacne id = %u", header.targetId
				);
			}
			UNLOCK( &client->sockets.serversIdToSocketLock );
			break;
		default:
			return false;
	}
	return ret;
}
