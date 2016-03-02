#include "worker.hh"
#include "../main/client.hh"

void ClientWorker::dispatch( ServerEvent event ) {
	bool connected, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;
	uint16_t instanceId = Master::instanceId;

	switch( event.type ) {
		case SERVER_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterSlave(
				buffer.size,
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

			buffer.data = this->protocol.syncMetadataBackup(
				buffer.size,
				instanceId,
				ClientWorker::idGenerator->nextVal( this->workerId ),
				addr.sin_addr.s_addr,
				addr.sin_port,
				&event.socket->backup.lock,
				event.socket->backup.sealed, sealedCount,
				event.socket->backup.ops, opsCount,
				isCompleted
			);

			if ( ! isCompleted )
				ClientWorker::eventQueue->insert( event );

			// printf( "Sealed: %u; ops: %u\n", sealedCount, opsCount );
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
				buffer.data = this->protocol.ackParityDeltaBackup(
					buffer.size,
					instanceId, requestId,
					*event.message.ack.timestamps,
					event.message.ack.targetId
				);
			} else {
				buffer.data = this->protocol.revertDelta(
					buffer.size,
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

		if ( event.type == SERVER_EVENT_TYPE_SEND ) {
			ClientWorker::packetPool->free( event.message.send.packet );
			// fprintf( stderr, "- After free(): " );
			// ClientWorker::packetPool->print( stderr );
		}
	} else if ( event.type == SERVER_EVENT_TYPE_SYNC_METADATA ) {
		std::vector<CoordinatorSocket *> &coordinators = Master::getInstance()->sockets.coordinators.values;
		for ( int i = 0, len = coordinators.size(); i < len; i++ ) {
			ret = coordinators[ i ]->send( buffer.data, buffer.size, connected );

			if ( ret != ( ssize_t ) buffer.size )
				__ERROR__( "ClientWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
		}
	} else {
		// Parse responses from slaves
		MasterRemapMsgHandler &mrmh = Master::getInstance()->remapMsgHandler;
		const struct sockaddr_in &addr = event.socket->getAddr();

		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "ClientWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_SERVER ) {
				__ERROR__( "ClientWorker", "dispatch", "Invalid message source from slave." );
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
						__ERROR__( "ClientWorker", "dispatch", "Invalid magic code from slave." );
						goto quit_1;
				}

				event.instanceId = header.instanceId;
				event.requestId = header.requestId;
				switch( header.opcode ) {
					case PROTO_OPCODE_REGISTER:
						if ( success ) {
							event.socket->registered = true;
							event.socket->instanceId = header.instanceId;
							Master *master = Master::getInstance();
							LOCK( &master->sockets.slavesIdToSocketLock );
							master->sockets.slavesIdToSocketMap[ header.instanceId ] = event.socket;
							UNLOCK( &master->sockets.slavesIdToSocketLock );
						} else {
							__ERROR__( "ClientWorker", "dispatch", "Failed to register with slave." );
						}
						break;
					case PROTO_OPCODE_GET:
						if ( ! mrmh.acceptNormalResponse( addr ) ) {
							// __INFO__( YELLOW, "Master", "dispatch", "Ignoring normal GET response..." );
							break;
						}
					case PROTO_OPCODE_DEGRADED_GET:
						this->handleGetResponse( event, success, header.opcode == PROTO_OPCODE_DEGRADED_GET, buffer.data, header.length );
						break;
					case PROTO_OPCODE_SET:
						if ( ! mrmh.acceptNormalResponse( addr ) ) {
							// __INFO__( YELLOW, "Master", "dispatch", "Ignoring normal SET response..." );
							break;
						}
						this->handleSetResponse( event, success, buffer.data, header.length );
						break;
					case PROTO_OPCODE_REMAPPING_SET:
						this->handleRemappingSetResponse( event, success, buffer.data, header.length );
						break;
					case PROTO_OPCODE_UPDATE:
						if ( ! mrmh.acceptNormalResponse( addr ) ) {
							__INFO__( YELLOW, "Master", "dispatch", "Ignoring normal UPDATE response..." );
							break;
						}
					case PROTO_OPCODE_DEGRADED_UPDATE:
						this->handleUpdateResponse( event, success, header.opcode == PROTO_OPCODE_DEGRADED_UPDATE, buffer.data, header.length );
						break;
					case PROTO_OPCODE_DELETE:
						if ( ! mrmh.acceptNormalResponse( addr ) ) {
							// __INFO__( YELLOW, "Master", "dispatch", "Ignoring normal DELETE response..." );
							break;
						}
					case PROTO_OPCODE_DEGRADED_DELETE:
						this->handleDeleteResponse( event, success, header.opcode == PROTO_OPCODE_DEGRADED_DELETE, buffer.data, header.length );
						break;
					case PROTO_OPCODE_ACK_METADATA:
					case PROTO_OPCODE_ACK_REQUEST:
						this->handleAcknowledgement( event, header.opcode, buffer.data, header.length );
						break;
					case PROTO_OPCODE_ACK_PARITY_DELTA:
					case PROTO_OPCODE_REVERT_DELTA:
						this->handleDeltaAcknowledgement( event, header.opcode, buffer.data, header.length );
						break;
					default:
						__ERROR__( "ClientWorker", "dispatch", "Invalid opcode from slave." );
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
		__ERROR__( "ClientWorker", "dispatch", "The slave is disconnected." );
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
				"[SET] Key: %.*s (key size = %u)",
				( int ) header.keySize, header.key, header.keySize
			);

			keySize = header.keySize;
			keyStr = header.key;
		} else {
			__DEBUG__(
				BLUE, "ClientWorker", "handleSetResponse",
				"[SET] [%u] Key: %.*s (key size = %u) at (%u, %u, %u)",
				header.timestamp,
				( int ) header.keySize, header.key, header.keySize,
				header.listId, header.stripeId, header.chunkId
			);

			keySize = header.keySize;
			keyStr = header.key;

			if ( header.isSealed ) {
				event.socket->backup.insert(
					keySize, keyStr,
					PROTO_OPCODE_SET,
					header.timestamp,
					header.listId,
					header.stripeId,
					header.chunkId,
					header.sealedListId,
					header.sealedStripeId,
					header.sealedChunkId
				);
			} else {
				event.socket->backup.insert(
					keySize, keyStr,
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
		__DEBUG__(
			BLUE, "ClientWorker", "handleSetResponse",
			"[SET] Key: %.*s (key size = %u)",
			( int ) header.keySize, header.key, header.keySize
		);

		keySize = header.keySize;
		keyStr = header.key;
	}

	int pending;
	ApplicationEvent applicationEvent;
	PendingIdentifier pid;
	Key key;
	KeyValue keyValue;

	if ( ! ClientWorker::pending->eraseKey( PT_SLAVE_SET, event.instanceId, event.requestId, event.socket, &pid, &key, true, false ) ) {
		UNLOCK( &ClientWorker::pending->slaves.setLock );
		__ERROR__( "ClientWorker", "handleSetResponse", "Cannot find a pending slave SET request that matches the response. This message will be discarded. (ID: (%u, %u); key: %.*s)", event.instanceId, event.requestId, keySize, keyStr );
		return false;
	}

	// FOR REPLAY TESTING ONLY
	//PendingIdentifier dpid = pid;

	// Check pending slave SET requests
	pending = ClientWorker::pending->count( PT_SLAVE_SET, pid.instanceId, pid.requestId, false, true );

	// Mark the elapse time as latency
	Master* master = Master::getInstance();
	if ( ClientWorker::updateInterval ) {
		struct timespec elapsedTime;
		RequestStartTime rst;

		if ( ! ClientWorker::pending->eraseRequestStartTime( PT_SLAVE_SET, pid.instanceId, pid.requestId, ( void * ) event.socket, elapsedTime, 0, &rst ) ) {
			__ERROR__( "ClientWorker", "handleSetResponse", "Cannot find a pending stats SET request that matches the response." );
		} else {
			int index = -1;
			LOCK( &master->slaveLoading.lock );
			std::set<Latency> *latencyPool = master->slaveLoading.past.set.get( rst.addr, &index );
			// init. the set if it is not there
			if ( index == -1 ) {
				master->slaveLoading.past.set.set( rst.addr, new std::set<Latency>() );
			}
			// insert the latency to the set
			// TODO use time when Response came, i.e. event created for latency cal.
			Latency latency = Latency ( elapsedTime );
			if ( index == -1 )
				latencyPool = master->slaveLoading.past.set.get( rst.addr );
			latencyPool->insert( latency );
			UNLOCK( &master->slaveLoading.lock );
		}
	}

	// __ERROR__( "ClientWorker", "handleSetResponse", "Pending slave SET requests = %d.", pending );

	if ( pending == 0 ) {
		// Only send application SET response when the number of pending slave SET requests equal 0
		if ( ! ClientWorker::pending->eraseKeyValue( PT_APPLICATION_SET, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &keyValue, true, true, true, keyStr ) ) {
			__ERROR__( "ClientWorker", "handleSetResponse", "Cannot find a pending application SET request that matches the response. This message will be discarded. (Key = %.*s, ID = (%u, %u))", key.size, key.data, pid.parentInstanceId, pid.parentRequestId );
			return false;
		}

		// FOR REPLAY TESTING ONLY
		// SET //
		//char *valueStr;
		//uint32_t valueSize, chunkId;
		//keyValue.deserialize( key.data, key.size, valueStr, valueSize );
		//KeyValue kv;
		//kv.dup( key.data, key.size, valueStr, valueSize );
		//ClientWorker::pending->insertKeyValue( PT_APPLICATION_SET, pid.instanceId, pid.requestId, 0, kv, true, true, pid.timestamp );
		//key = kv.key();
		//this->stripeList->get( key.data, key.size, this->dataServerSockets, 0, &chunkId );
		//ClientWorker::pending->insertKey( PT_SLAVE_SET, dpid.instanceId, dpid.parentInstanceId, dpid.requestId, dpid.parentRequestId, this->dataServerSockets[ chunkId ], key );

		// not to response if the request is "canceled" due to replay
		if ( pid.ptr ) {
			applicationEvent.resSet( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, keyValue, success );
			this->dispatch( applicationEvent );
		}

		// Check if all normal requests completes
		ServerSocket *slave = 0;
		struct sockaddr_in addr;
		MasterRemapMsgHandler *remapMsgHandler = &Master::getInstance()->remapMsgHandler;
		this->stripeList->get( keyStr, key.size, 0, this->parityServerSockets );
		for ( uint32_t i = 0; i < this->parityChunkCount; i++ ) {
			slave = this->parityServerSockets[ i ];
			addr = slave->getAddr();
			if ( ! remapMsgHandler->useCoordinatedFlow( addr ) || remapMsgHandler->stateTransitInfo.at( addr ).isCompleted() )
				continue;
			if ( remapMsgHandler->stateTransitInfo.at( addr ).removePendingRequest( event.requestId ) == 0 ) {
				__INFO__( GREEN, "ClientWorker", "handleSetResponse", "Ack transition for slave id = %u.", slave->instanceId );
				remapMsgHandler->stateTransitInfo.at( addr ).setCompleted();
				remapMsgHandler->ackTransit( addr );
			}
		}

	}

	return true;
}

bool ClientWorker::handleGetResponse( ServerEvent event, bool success, bool isDegraded, char *buf, size_t size ) {
	Key key;
	uint32_t valueSize = 0;
	char *valueStr = 0;
	if ( success ) {
		struct KeyValueHeader header;
		if ( this->protocol.parseKeyValueHeader( header, buf, size ) ) {
			key.set( header.keySize, header.key, ( void * ) event.socket );
			valueSize = header.valueSize;
			valueStr = header.value;
		} else {
			__ERROR__( "ClientWorker", "handleGetResponse", "Invalid GET response." );
			return false;
		}
	} else {
		struct KeyHeader header;
		if ( this->protocol.parseKeyHeader( header, buf, size ) ) {
			key.set( header.keySize, header.key, ( void * ) event.socket );
		} else {
			__ERROR__( "ClientWorker", "handleGetResponse", "Invalid GET response." );
			return false;
		}
	}

	ApplicationEvent applicationEvent;
	PendingIdentifier pid;

	if ( ! ClientWorker::pending->eraseKey( PT_SLAVE_GET, event.instanceId, event.requestId, event.socket, &pid, &key, true, true ) ) {
		__ERROR__( "ClientWorker", "handleGetResponse", "Cannot find a pending slave GET request that matches the response. This message will be discarded (key = %.*s).", key.size, key.data );
		return false;
	}

	// FOR REPLAY TESTING ONLY
	//PendingIdentifier dpid = pid;

	// Mark the elapse time as latency
	if ( ! isDegraded && ClientWorker::updateInterval ) {
		Master* master = Master::getInstance();
		struct timespec elapsedTime;
		RequestStartTime rst;

		if ( ! ClientWorker::pending->eraseRequestStartTime( PT_SLAVE_GET, pid.instanceId, pid.requestId, ( void * ) event.socket, elapsedTime, 0, &rst ) ) {
			__ERROR__( "ClientWorker", "handleGetResponse", "Cannot find a pending stats GET request that matches the response (ID: (%u, %u)).", pid.instanceId, pid.requestId );
		} else {
			int index = -1;
			LOCK( &master->slaveLoading.lock );
			std::set<Latency> *latencyPool = master->slaveLoading.past.get.get( rst.addr, &index );
			// init. the set if it is not there
			if ( index == -1 ) {
				master->slaveLoading.past.get.set( rst.addr, new std::set<Latency>() );
			}
			// insert the latency to the set
			// TODO use time when Response came, i.e. event created for latency cal.
			Latency latency = Latency ( elapsedTime );
			if ( index == -1 )
				latencyPool = master->slaveLoading.past.get.get( rst.addr );
			latencyPool->insert( latency );
			UNLOCK( &master->slaveLoading.lock );
		}
	}

	if ( ! ClientWorker::pending->eraseKey( PT_APPLICATION_GET, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &key, true, true, true, key.data ) ) {
		__ERROR__( "ClientWorker", "handleGetResponse", "Cannot find a pending application GET request that matches the response. This message will be discarded (key = %.*s).", key.size, key.data );
		return false;
	}

	// FOR REPLAY TESTING ONLY
	// GET //
	//Key k;
	//k.dup( key.size, key.data, key.ptr );
	//ClientWorker::pending->insertKey( PT_APPLICATION_GET, pid.instanceId, pid.requestId, 0, k, true, true, pid.timestamp );
	//ClientWorker::pending->insertKey( PT_SLAVE_GET, dpid.instanceId, dpid.parentInstanceId, dpid.requestId, dpid.parentRequestId, dpid.ptr, key );

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
			// event.socket->printAddress();
			// printf( ": handleGetResponse(): Key %.*s not found.\n", key.size, key.data );
			applicationEvent.resGet( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, key, false );
		}
		this->dispatch( applicationEvent );
	}
	key.free();
	return true;
}

bool ClientWorker::handleUpdateResponse( ServerEvent event, bool success, bool isDegraded, char *buf, size_t size ) {
	struct KeyValueUpdateHeader header;
	if ( ! this->protocol.parseKeyValueUpdateHeader( header, false, buf, size ) ) {
		__ERROR__( "ClientWorker", "handleUpdateResponse", "Invalid UPDATE Response." );
		return false;
	}
	__DEBUG__(
		BLUE, "ClientWorker", "handleUpdateResponse",
		"[UPDATE (%s)] Updated key: %.*s (key size = %u); update value size = %u at offset: %u.",
		success ? "Success" : "Fail",
		( int ) header.keySize, header.key, header.keySize,
		header.valueUpdateSize, header.valueUpdateOffset
	);

	KeyValueUpdate keyValueUpdate;
	ApplicationEvent applicationEvent;
	PendingIdentifier pid;

	// Find the cooresponding request
	if ( ! ClientWorker::pending->eraseKeyValueUpdate( PT_SLAVE_UPDATE, event.instanceId, event.requestId, ( void * ) event.socket, &pid, &keyValueUpdate ) ) {
		__ERROR__( "ClientWorker", "handleUpdateResponse", "Cannot find a pending slave UPDATE request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	// FOR REPLAY TESTING ONLY
	//PendingIdentifier dpid = pid;

	uint32_t timestamp = pid.timestamp;

	if ( ! ClientWorker::pending->eraseKeyValueUpdate( PT_APPLICATION_UPDATE, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &keyValueUpdate, true, true, true, header.key ) ) {
		__ERROR__( "ClientWorker", "handleUpdateResponse", "Cannot find a pending application UPDATE request that matches the response. This message will be discarded (%u, %u).", pid.parentInstanceId, pid.parentRequestId );
		return false;
	}

	// free the updated value
	delete[] ( ( char * )( keyValueUpdate.ptr ) );

	// remove pending timestamp
	// TODO handle degraded mode
	Master *master = Master::getInstance();
	if ( ! isDegraded ) {
		event.socket->timestamp.pendingAck.eraseUpdate( timestamp );
	}

		// FOR REPLAY TESTING ONLY
		// UPDATE //
		//KeyValueUpdate kvu;
		//kvu.dup( keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.ptr );
		//kvu.offset = keyValueUpdate.offset;
		//kvu.length = keyValueUpdate.length;
		//ClientWorker::pending->insertKeyValueUpdate( PT_APPLICATION_UPDATE, pid.instanceId, pid.requestId, 0, kvu, true, true, pid.timestamp );
		//ClientWorker::pending->insertKeyValueUpdate( PT_SLAVE_UPDATE, dpid.instanceId, dpid.parentInstanceId, dpid.requestId, dpid.parentRequestId, dpid.ptr, kvu );

	if ( pid.ptr ) {
		applicationEvent.resUpdate( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, keyValueUpdate, success );
		this->dispatch( applicationEvent );
	}

	// Check if all normal requests completes
	if ( ! isDegraded ) {
		ServerSocket *slave = 0;
		struct sockaddr_in addr;
		MasterRemapMsgHandler *remapMsgHandler = &Master::getInstance()->remapMsgHandler;
		this->stripeList->get( header.key, header.keySize, 0, this->parityServerSockets );
		for ( uint32_t i = 0; i < this->parityChunkCount; i++ ) {
			slave = this->parityServerSockets[ i ];
			addr = slave->getAddr();
			if ( ! remapMsgHandler->useCoordinatedFlow( addr ) || remapMsgHandler->stateTransitInfo.at( addr ).isCompleted() )
				continue;
			if ( remapMsgHandler->stateTransitInfo.at( addr ).removePendingRequest( pid.requestId ) == 0 ) {
				__INFO__( GREEN, "ClientWorker", "handleUpdateResponse", "Ack transition for slave id = %u.", slave->instanceId );
				remapMsgHandler->stateTransitInfo.at( addr ).setCompleted();
				remapMsgHandler->ackTransit( addr );
			}
		}
	}

	// check if ack is necessary
	// TODO handle degraded mode
	if ( ! isDegraded )
		master->ackParityDelta( 0, event.socket );

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

		if ( header.isSealed ) {
			event.socket->backup.insert(
				keySize, keyStr,
				PROTO_OPCODE_DELETE,
				header.timestamp,
				header.listId,
				header.stripeId,
				header.chunkId,
				header.sealedListId,
				header.sealedStripeId,
				header.sealedChunkId
			);
		} else {
			event.socket->backup.insert(
				keySize, keyStr,
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

	if ( ! ClientWorker::pending->eraseKey( PT_SLAVE_DEL, event.instanceId, event.requestId, ( void * ) event.socket, &pid, &key ) ) {
		__ERROR__( "ClientWorker", "handleDeleteResponse", "Cannot find a pending slave DELETE request that matches the response. This message will be discarded." );
		return false;
	}

	uint32_t timestamp = pid.timestamp;

	if ( ! ClientWorker::pending->eraseKey( PT_APPLICATION_DEL, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &key, true, true, true, keyStr ) ) {
		__ERROR__( "ClientWorker", "handleDeleteResponse", "Cannot find a pending application DELETE request that matches the response. This message will be discarded. ID: (%u, %u); key: %.*s.", pid.parentInstanceId, pid.parentRequestId, keySize, keyStr );
		return false;
	}

	// remove pending timestamp
	// TODO handle degraded mode
	Master *master = Master::getInstance();
	if ( !isDegraded ) {
		event.socket->timestamp.pendingAck.eraseDel( timestamp );
	}

	if ( pid.ptr ) {
		applicationEvent.resDelete( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, key, success );
		this->dispatch( applicationEvent );
	}

	// Check if all normal requests completes
	ServerSocket *slave = 0;
	struct sockaddr_in addr;
	MasterRemapMsgHandler *remapMsgHandler = &Master::getInstance()->remapMsgHandler;
	this->stripeList->get( keyStr, key.size, 0, this->parityServerSockets );
	for ( uint32_t i = 0; i < this->parityChunkCount; i++ ) {
		slave = this->parityServerSockets[ i ];
		addr = slave->getAddr();
		if ( ! remapMsgHandler->useCoordinatedFlow( addr ) || remapMsgHandler->stateTransitInfo.at( addr ).isCompleted() )
			continue;
		if ( remapMsgHandler->stateTransitInfo.at( addr ).removePendingRequest( pid.requestId ) == 0 ) {
			__INFO__( GREEN, "ClientWorker", "handleDeleteResponse", "Ack transition for slave id = %u.", slave->instanceId );
			remapMsgHandler->stateTransitInfo.at( addr ).setCompleted();
			remapMsgHandler->ackTransit( addr );
		}
	}

	// check if ack is necessary
	// TODO handle degraded mode
	if ( ! isDegraded ) master->ackParityDelta( 0, event.socket );

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

	Master *master = Master::getInstance();
	switch( opcode ) {
		case PROTO_OPCODE_ACK_PARITY_DELTA:
			break;
		case PROTO_OPCODE_REVERT_DELTA:
			LOCK( &master->sockets.slavesIdToSocketLock );
			try {
				struct sockaddr_in saddr = master->sockets.slavesIdToSocketMap.at( header.targetId )->getAddr();
				master->remapMsgHandler.ackTransit( saddr );
			} catch ( std::out_of_range &e ) {
				__ERROR__( "ClientWorker", "handleDeltaAcknowledgement",
					"Cannot find slave socket for instacne id = %u", header.targetId
				);
			}
			UNLOCK( &master->sockets.slavesIdToSocketLock );
			break;
		default:
			return false;
	}
	return ret;
}
