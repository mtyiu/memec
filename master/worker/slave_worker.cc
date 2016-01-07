#include "worker.hh"
#include "../main/master.hh"

void MasterWorker::dispatch( SlaveEvent event ) {
	bool connected, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;
	uint16_t instanceId = Master::instanceId;

	switch( event.type ) {
		case SLAVE_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterSlave(
				buffer.size,
				instanceId,
				MasterWorker::idGenerator->nextVal( this->workerId ),
				event.message.address.addr,
				event.message.address.port
			);
			isSend = true;
			break;
		case SLAVE_EVENT_TYPE_SEND:
			event.message.send.packet->read( buffer.data, buffer.size );
			isSend = true;
			break;
		case SLAVE_EVENT_TYPE_SYNC_METADATA:
		{
			uint32_t sealedCount, opsCount;
			bool isCompleted;
			struct sockaddr_in addr = event.socket->getAddr();

			buffer.data = this->protocol.syncMetadataBackup(
				buffer.size,
				instanceId,
				MasterWorker::idGenerator->nextVal( this->workerId ),
				addr.sin_addr.s_addr,
				addr.sin_port,
				&event.socket->backup.lock,
				event.socket->backup.sealed, sealedCount,
				event.socket->backup.ops, opsCount,
				isCompleted
			);

			if ( ! isCompleted )
				MasterWorker::eventQueue->insert( event );

			printf( "Sealed: %u; ops: %u\n", sealedCount, opsCount );
			isSend = false; // Send to coordinator instead
		}
			break;
		case SLAVE_EVENT_TYPE_ACK_PARITY_DELTA:
		{
			uint32_t requestId = MasterWorker::idGenerator->nextVal( this->workerId );

			AcknowledgementInfo ackInfo(
				Timestamp( event.message.ack.fromTimestamp ),
				Timestamp( event.message.ack.toTimestamp ),
				event.message.ack.condition,
				event.message.ack.lock,
				event.message.ack.counter
			);

			MasterWorker::pending->insertAck(
				PT_ACK_REMOVE_PARITY,
				event.socket->instanceId, requestId, 0,
				ackInfo
			);

			buffer.data = this->protocol.ackParityDeltaBackup(
				buffer.size,
				instanceId, requestId,
				event.message.ack.fromTimestamp, event.message.ack.toTimestamp,
				event.message.ack.targetId
			);
			isSend = true;
		}
			break;
		case SLAVE_EVENT_TYPE_REVERT_PARITY_DELTA:
		{
			uint32_t requestId = MasterWorker::idGenerator->nextVal( this->workerId );

			AcknowledgementInfo ackInfo(
				Timestamp( event.message.ack.fromTimestamp ),
				Timestamp( event.message.ack.toTimestamp ),
				event.message.ack.condition,
				event.message.ack.lock,
				event.message.ack.counter
			);

			MasterWorker::pending->insertAck(
				PT_ACK_REVERT_PARITY,
				event.socket->instanceId, requestId, 0,
				ackInfo
			);

			buffer.data = this->protocol.revertParityDelta(
				buffer.size,
				instanceId, requestId,
				event.message.ack.fromTimestamp, event.message.ack.toTimestamp,
				event.message.ack.targetId
			);
			isSend = true;
		}
			break;
		case SLAVE_EVENT_TYPE_PENDING:
			isSend = false;
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "MasterWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );

		if ( event.type == SLAVE_EVENT_TYPE_SEND ) {
			MasterWorker::packetPool->free( event.message.send.packet );
			// fprintf( stderr, "- After free(): " );
			// MasterWorker::packetPool->print( stderr );
		}
	} else if ( event.type == SLAVE_EVENT_TYPE_SYNC_METADATA ) {
		std::vector<CoordinatorSocket *> &coordinators = Master::getInstance()->sockets.coordinators.values;
		for ( int i = 0, len = coordinators.size(); i < len; i++ ) {
			ret = coordinators[ i ]->send( buffer.data, buffer.size, connected );

			if ( ret != ( ssize_t ) buffer.size )
				__ERROR__( "MasterWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
		}
	} else {
		// Parse responses from slaves
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "MasterWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_SLAVE ) {
				__ERROR__( "MasterWorker", "dispatch", "Invalid message source from slave." );
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
						__ERROR__( "MasterWorker", "dispatch", "Invalid magic code from slave." );
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
							__ERROR__( "MasterWorker", "dispatch", "Failed to register with slave." );
						}
						break;
					case PROTO_OPCODE_GET:
					case PROTO_OPCODE_DEGRADED_GET:
						this->handleGetResponse( event, success, header.opcode == PROTO_OPCODE_DEGRADED_GET, buffer.data, header.length );
						break;
					case PROTO_OPCODE_SET:
						this->handleSetResponse( event, success, buffer.data, header.length );
						break;
					case PROTO_OPCODE_REMAPPING_SET:
						this->handleRemappingSetResponse( event, success, buffer.data, header.length );
						break;
					case PROTO_OPCODE_UPDATE:
					case PROTO_OPCODE_DEGRADED_UPDATE:
						this->handleUpdateResponse( event, success, header.opcode == PROTO_OPCODE_DEGRADED_UPDATE, buffer.data, header.length );
						break;
					case PROTO_OPCODE_DELETE:
					case PROTO_OPCODE_DEGRADED_DELETE:
						this->handleDeleteResponse( event, success, header.opcode == PROTO_OPCODE_DEGRADED_DELETE, buffer.data, header.length );
						break;
					case PROTO_OPCODE_ACK_METADATA:
					case PROTO_OPCODE_ACK_REQUEST:
						this->handleAcknowledgement( event, header.opcode, buffer.data, header.length );
						break;
					case PROTO_OPCODE_ACK_PARITY_DELTA:
					case PROTO_OPCODE_REVERT_PARITY_DELTA:
						this->handleParityDeltaAcknowledgement( event, header.opcode, buffer.data, header.length );
						break;
					default:
						__ERROR__( "MasterWorker", "dispatch", "Invalid opcode from slave." );
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
		__ERROR__( "MasterWorker", "dispatch", "The slave is disconnected." );
		this->removePending( event.socket );
	}
}

bool MasterWorker::handleSetResponse( SlaveEvent event, bool success, char *buf, size_t size ) {
	uint8_t keySize;
	char *keyStr;
	if ( success ) {
		struct KeyBackupHeader header;
		if ( ! this->protocol.parseKeyBackupHeader( header, buf, size ) ) {
			__ERROR__( "MasterWorker", "handleSetResponse", "Invalid SET response." );
			return false;
		}
		if ( header.isParity ) {
			__DEBUG__(
				BLUE, "MasterWorker", "handleSetResponse",
				"[SET] Key: %.*s (key size = %u)",
				( int ) header.keySize, header.key, header.keySize
			);

			keySize = header.keySize;
			keyStr = header.key;
		} else {
			__DEBUG__(
				BLUE, "MasterWorker", "handleSetResponse",
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
		struct KeyHeader header;
		if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
			__ERROR__( "MasterWorker", "handleSetResponse", "Invalid SET response." );
			return false;
		}
		__DEBUG__(
			BLUE, "MasterWorker", "handleSetResponse",
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

	if ( ! MasterWorker::pending->eraseKey( PT_SLAVE_SET, event.instanceId, event.requestId, event.socket, &pid, &key, true, false ) ) {
		UNLOCK( &MasterWorker::pending->slaves.setLock );
		__ERROR__( "MasterWorker", "handleSetResponse", "Cannot find a pending slave SET request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}
	// Check pending slave SET requests
	pending = MasterWorker::pending->count( PT_SLAVE_SET, pid.instanceId, pid.requestId, false, true );

	// Mark the elapse time as latency
	Master* master = Master::getInstance();
	if ( MasterWorker::updateInterval ) {
		struct timespec elapsedTime;
		RequestStartTime rst;

		if ( ! MasterWorker::pending->eraseRequestStartTime( PT_SLAVE_SET, pid.instanceId, pid.requestId, ( void * ) event.socket, elapsedTime, 0, &rst ) ) {
			__ERROR__( "MasterWorker", "handleSetResponse", "Cannot find a pending stats SET request that matches the response." );
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

	// __ERROR__( "MasterWorker", "handleSetResponse", "Pending slave SET requests = %d.", pending );

	if ( pending == 0 ) {
		// Only send application SET response when the number of pending slave SET requests equal 0
		if ( ! MasterWorker::pending->eraseKeyValue( PT_APPLICATION_SET, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &keyValue, true, true, true, keyStr ) ) {
			__ERROR__( "MasterWorker", "handleSetResponse", "Cannot find a pending application SET request that matches the response. This message will be discarded. (Key = %.*s, ID = (%u, %u))", key.size, key.data, pid.parentInstanceId, pid.parentRequestId );
			return false;
		}

		applicationEvent.resSet( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, keyValue, success );
		this->dispatch( applicationEvent );
	}
	return true;
}

bool MasterWorker::handleGetResponse( SlaveEvent event, bool success, bool isDegraded, char *buf, size_t size ) {
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
			__ERROR__( "MasterWorker", "handleGetResponse", "Invalid GET response." );
			return false;
		}
	} else {
		struct KeyHeader header;
		if ( this->protocol.parseKeyHeader( header, buf, size ) ) {
			key.set( header.keySize, header.key, ( void * ) event.socket );
		} else {
			__ERROR__( "MasterWorker", "handleGetResponse", "Invalid GET response." );
			return false;
		}
	}

	ApplicationEvent applicationEvent;
	PendingIdentifier pid;

	if ( ! MasterWorker::pending->eraseKey( PT_SLAVE_GET, event.instanceId, event.requestId, event.socket, &pid, &key, true, true ) ) {
		__ERROR__( "MasterWorker", "handleGetResponse", "Cannot find a pending slave GET request that matches the response. This message will be discarded (key = %.*s).", key.size, key.data );
		return false;
	}

	// Mark the elapse time as latency
	if ( ! isDegraded && MasterWorker::updateInterval ) {
		Master* master = Master::getInstance();
		struct timespec elapsedTime;
		RequestStartTime rst;

		if ( ! MasterWorker::pending->eraseRequestStartTime( PT_SLAVE_GET, pid.instanceId, pid.requestId, ( void * ) event.socket, elapsedTime, 0, &rst ) ) {
			__ERROR__( "MasterWorker", "handleGetResponse", "Cannot find a pending stats GET request that matches the response (ID: (%u, %u)).", pid.instanceId, pid.requestId );
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

	if ( ! MasterWorker::pending->eraseKey( PT_APPLICATION_GET, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &key, true, true, true, key.data ) ) {
		__ERROR__( "MasterWorker", "handleGetResponse", "Cannot find a pending application GET request that matches the response. This message will be discarded (key = %.*s).", key.size, key.data );
		return false;
	}

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
		applicationEvent.resGet( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, key, false );
	}
	this->dispatch( applicationEvent );
	key.free();
	return true;
}

bool MasterWorker::handleUpdateResponse( SlaveEvent event, bool success, bool isDegraded, char *buf, size_t size ) {
	struct KeyValueUpdateHeader header;
	if ( ! this->protocol.parseKeyValueUpdateHeader( header, false, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleUpdateResponse", "Invalid UPDATE Response." );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleUpdateResponse",
		"[UPDATE (%s)] Updated key: %.*s (key size = %u); update value size = %u at offset: %u.",
		success ? "Success" : "Fail",
		( int ) header.keySize, header.key, header.keySize,
		header.valueUpdateSize, header.valueUpdateOffset
	);

	KeyValueUpdate keyValueUpdate;
	ApplicationEvent applicationEvent;
	PendingIdentifier pid;

	// Find the cooresponding request
	if ( ! MasterWorker::pending->eraseKeyValueUpdate( PT_SLAVE_UPDATE, event.instanceId, event.requestId, ( void * ) event.socket, &pid, &keyValueUpdate ) ) {
		__ERROR__( "MasterWorker", "handleUpdateResponse", "Cannot find a pending slave UPDATE request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	if ( ! MasterWorker::pending->eraseKeyValueUpdate( PT_APPLICATION_UPDATE, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &keyValueUpdate, true, true, true, header.key ) ) {
		__ERROR__( "MasterWorker", "handleUpdateResponse", "Cannot find a pending application UPDATE request that matches the response. This message will be discarded." );
		return false;
	}

	// free the updated value
	delete[] ( ( char * )( keyValueUpdate.ptr ) );

	// remove pending timestamp
	// TODO handle degraded mode
	Master *master = Master::getInstance();
	if ( ! isDegraded ) {
		event.socket->timestamp.pendingAck.eraseUpdate( pid.timestamp );
	}

	applicationEvent.resUpdate( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, keyValueUpdate, success );
	this->dispatch( applicationEvent );

	// check if ack is necessary
	// TODO handle degraded mode
	if ( ! isDegraded )
		master->ackParityDelta( 0, event.socket );

	return true;
}

bool MasterWorker::handleDeleteResponse( SlaveEvent event, bool success, bool isDegraded, char *buf, size_t size ) {
	char *keyStr;
	uint8_t keySize;
	if ( success ) {
		struct KeyBackupHeader header;
		if ( ! this->protocol.parseKeyBackupHeader( header, buf, size ) ) {
			__ERROR__( "MasterWorker", "handleDeleteResponse", "Invalid DELETE Response." );
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
			__ERROR__( "MasterWorker", "handleDeleteResponse", "Invalid DELETE Response." );
			return false;
		}
		keyStr = header.key;
		keySize = header.keySize;
	}

	ApplicationEvent applicationEvent;
	PendingIdentifier pid;
	Key key;

	if ( ! MasterWorker::pending->eraseKey( PT_SLAVE_DEL, event.instanceId, event.requestId, ( void * ) event.socket, &pid, &key ) ) {
		__ERROR__( "MasterWorker", "handleDeleteResponse", "Cannot find a pending slave DELETE request that matches the response. This message will be discarded." );
		return false;
	}

	if ( ! MasterWorker::pending->eraseKey( PT_APPLICATION_DEL, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &key, true, true, true, keyStr ) ) {
		__ERROR__( "MasterWorker", "handleDeleteResponse", "Cannot find a pending application DELETE request that matches the response. This message will be discarded. ID: (%u, %u); key: %.*s.", pid.parentInstanceId, pid.parentRequestId, keySize, keyStr );
		return false;
	}

	// remove pending timestamp
	// TODO handle degraded mode
	Master *master = Master::getInstance();
	if ( !isDegraded ) {
		event.socket->timestamp.pendingAck.eraseDel( pid.timestamp );
	}

	applicationEvent.resDelete( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, key, success );
	this->dispatch( applicationEvent );

	// check if ack is necessary
	// TODO handle degraded mode
	if ( ! isDegraded ) master->ackParityDelta( 0, event.socket );

	return true;
}

bool MasterWorker::handleAcknowledgement( SlaveEvent event, uint8_t opcode, char *buf, size_t size ) {
	struct AcknowledgementHeader header;
	if ( ! this->protocol.parseAcknowledgementHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleAcknowledgement", "Invalid ACK." );
		return false;
	}

	__DEBUG__( YELLOW, "MasterWorker", "handleAcknowledgement", "Timestamp = (%u, %u).", header.fromTimestamp, header.toTimestamp );

	event.socket->backup.erase( header.fromTimestamp, header.toTimestamp );

	return true;
}

bool MasterWorker::handleParityDeltaAcknowledgement( SlaveEvent event, uint8_t opcode, char *buf, size_t size ) {
	struct ParityDeltaAcknowledgementHeader header;
	PendingIdentifier pid;
	if ( ! this->protocol.parseParityDeltaAcknowledgementHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleParityDeltaAcknowledgement", "Invalid ACK." );
		return false;
	}

	bool ret = true;
	AcknowledgementInfo ackInfo;

	switch( opcode ) {
		case PROTO_OPCODE_ACK_PARITY_DELTA:
			ret = MasterWorker::pending->eraseAck( PT_ACK_REMOVE_PARITY, event.instanceId, event.requestId, 0, &pid, &ackInfo );
			break;
		case PROTO_OPCODE_REVERT_PARITY_DELTA:
			ret = MasterWorker::pending->eraseAck( PT_ACK_REVERT_PARITY, event.instanceId, event.requestId, 0, &pid, &ackInfo );
			break;
		default:
			__ERROR__( "MasterWorker", "handleParityDeltaAcknowledgement",
					"Unknown ack for instance id = %u from instance id = %u request id = %u",
					header.targetId, event.instanceId, event.requestId
			);
			return false;
	}

	if ( ! ret ) {
		__ERROR__( "MasterWorker", "handleParityDeltaAcknowledgement",
			"Cannot find the pending ack info for instance id = %u from instance id = %u request id = %u",
			header.targetId, event.instanceId, event.requestId
		);
		return ret;
	}

	if ( ackInfo.lock ) LOCK( ackInfo.lock );
	if ( ackInfo.counter ) {
		*ackInfo.counter -= 1;
		if ( *ackInfo.counter == 0 ) {
			__INFO__( GREEN, "MasterWorker", "handleParityDeltaAcknowledgement",
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
		case PROTO_OPCODE_REVERT_PARITY_DELTA:
			LOCK( &master->sockets.slavesIdToSocketLock );
			try {
				struct sockaddr_in saddr = master->sockets.slavesIdToSocketMap.at( header.targetId )->getAddr();
				master->remapMsgHandler.ackTransit( saddr );
			} catch ( std::out_of_range &e ) {
				__ERROR__( "MasterWorker", "handleParityDeltaAcknowledgement",
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
