#include "worker.hh"
#include "../main/master.hh"

void MasterWorker::dispatch( SlaveEvent event ) {
	bool connected, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	switch( event.type ) {
		case SLAVE_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterSlave(
				buffer.size,
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
						success = true;
						break;
					case PROTO_MAGIC_RESPONSE_FAILURE:
						success = false;
						break;
					default:
						__ERROR__( "MasterWorker", "dispatch", "Invalid magic code from slave." );
						goto quit_1;
				}

				event.id = header.id;
				switch( header.opcode ) {
					case PROTO_OPCODE_REGISTER:
						if ( success ) {
							event.socket->registered = true;
						} else {
							__ERROR__( "MasterWorker", "dispatch", "Failed to register with slave." );
						}
						break;
					case PROTO_OPCODE_GET:
					case PROTO_OPCODE_DEGRADED_GET:
						this->handleGetResponse( event, success, header.opcode == PROTO_OPCODE_DEGRADED_GET, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_SET:
						this->handleSetResponse( event, success, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_REMAPPING_SET:
						this->handleRemappingSetResponse( event, success, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_UPDATE:
					case PROTO_OPCODE_DEGRADED_UPDATE:
						this->handleUpdateResponse( event, success, header.opcode == PROTO_OPCODE_DEGRADED_UPDATE, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_DELETE:
					case PROTO_OPCODE_DEGRADED_DELETE:
						this->handleDeleteResponse( event, success, header.opcode == PROTO_OPCODE_DEGRADED_DELETE, buffer.data, buffer.size );
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
	if ( ! connected )
		__ERROR__( "MasterWorker", "dispatch", "The slave is disconnected." );
}

bool MasterWorker::handleSetResponse( SlaveEvent event, bool success, char *buf, size_t size ) {
	char *keyStr;
	if ( success ) {
		struct ChunkKeyHeader header;
		if ( ! this->protocol.parseChunkKeyHeader( header, buf, size ) ) {
			__ERROR__( "MasterWorker", "handleSetResponse", "Invalid SET response." );
			return false;
		}
		__DEBUG__(
			BLUE, "MasterWorker", "handleSetResponse",
			"[SET] Key: %.*s (key size = %u) at (%u, %u, %u)",
			( int ) header.keySize, header.key, header.keySize,
			header.listId, header.stripeId, header.chunkId
		);

		keyStr = header.key;
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

		keyStr = header.key;
	}

	int pending;
	ApplicationEvent applicationEvent;
	PendingIdentifier pid;
	Key key;

	if ( ! MasterWorker::pending->eraseKey( PT_SLAVE_SET, event.id, event.socket, &pid, &key, true, false ) ) {
		UNLOCK( &MasterWorker::pending->slaves.setLock );
		__ERROR__( "MasterWorker", "handleSetResponse", "Cannot find a pending slave SET request that matches the response. This message will be discarded. (ID: %u)", event.id );
		return false;
	}
	// Check pending slave SET requests
	pending = MasterWorker::pending->count( PT_SLAVE_SET, pid.id, false, true );

	// Mark the elapse time as latency
	Master* master = Master::getInstance();
	if ( MasterWorker::updateInterval ) {
		struct timespec elapsedTime;
		RequestStartTime rst;

		if ( ! MasterWorker::pending->eraseRequestStartTime( PT_SLAVE_SET, pid.id, ( void * ) event.socket, elapsedTime, 0, &rst ) ) {
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
		if ( ! MasterWorker::pending->eraseKey( PT_APPLICATION_SET, pid.parentId, 0, &pid, &key, true, true, true, keyStr ) ) {
			__ERROR__( "MasterWorker", "handleSetResponse", "Cannot find a pending application SET request that matches the response. This message will be discarded. (Key = %.*s, ID = %u)", key.size, key.data, pid.parentId );
			return false;
		}

		applicationEvent.resSet( ( ApplicationSocket * ) key.ptr, pid.id, key, success );
		MasterWorker::eventQueue->insert( applicationEvent );
		uint32_t originalListId, originalChunkId;
		SlaveSocket *original = this->getSlaves( key.data, key.size, originalListId, originalChunkId );
		int sockfd = original->getSocket();
		MasterWorker::slaveSockets->get( sockfd )->counter.decreaseNormal();
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
	SlaveSocket *original;
	int sockfd;

	if ( ! MasterWorker::pending->eraseKey( PT_SLAVE_GET, event.id, event.socket, &pid, &key, true, true ) ) {
		__ERROR__( "MasterWorker", "handleGetResponse", "Cannot find a pending slave GET request that matches the response. This message will be discarded (key = %.*s).", key.size, key.data );
		return false;
	}
	original = ( SlaveSocket * ) key.ptr;

	// Mark the elapse time as latency
	if ( ! isDegraded && MasterWorker::updateInterval ) {
		Master* master = Master::getInstance();
		struct timespec elapsedTime;
		RequestStartTime rst;

		if ( ! MasterWorker::pending->eraseRequestStartTime( PT_SLAVE_GET, pid.id, ( void * ) event.socket, elapsedTime, 0, &rst ) ) {
			__ERROR__( "MasterWorker", "handleGetResponse", "Cannot find a pending stats GET request that matches the response." );
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

	key.ptr = 0;
	if ( ! MasterWorker::pending->eraseKey( PT_APPLICATION_GET, pid.parentId, 0, &pid, &key, true, true, true, key.data ) ) {
		__ERROR__( "MasterWorker", "handleGetResponse", "Cannot find a pending application GET request that matches the response. This message will be discarded (key = %.*s).", key.size, key.data );
		return false;
	}

	if ( isDegraded ) {
		sockfd = original->getSocket();
		MasterWorker::slaveSockets->get( sockfd )->counter.decreaseDegraded();
		Master::getInstance()->remapMsgHandler.ackTransit( original->getAddr() );
	} else {
		sockfd = event.socket->getSocket();
		MasterWorker::slaveSockets->get( sockfd )->counter.decreaseNormal();
		Master::getInstance()->remapMsgHandler.ackTransit( event.socket->getAddr() );
	}

	if ( success ) {
		applicationEvent.resGet(
			( ApplicationSocket * ) key.ptr,
			pid.id,
			key.size,
			valueSize,
			key.data,
			valueStr,
			false
		);
	} else {
		applicationEvent.resGet( ( ApplicationSocket * ) key.ptr, pid.id, key, false );
	}
	// MasterWorker::eventQueue->insert( applicationEvent );
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
	SlaveSocket *original;
	int sockfd;

	// Find the cooresponding request
	if ( ! MasterWorker::pending->eraseKeyValueUpdate( PT_SLAVE_UPDATE, event.id, ( void * ) event.socket, &pid, &keyValueUpdate ) ) {
		__ERROR__( "MasterWorker", "handleUpdateResponse", "Cannot find a pending slave UPDATE request that matches the response. This message will be discarded. (ID: %u)", event.id );
		return false;
	}
	original = ( SlaveSocket * ) keyValueUpdate.ptr;

	if ( ! MasterWorker::pending->eraseKeyValueUpdate( PT_APPLICATION_UPDATE, pid.parentId, 0, &pid, &keyValueUpdate, true, true, true, header.key ) ) {
		__ERROR__( "MasterWorker", "handleUpdateResponse", "Cannot find a pending application UPDATE request that matches the response. This message will be discarded." );
		return false;
	}

	// free the updated value
	delete[] ( ( char * )( keyValueUpdate.ptr ) );

	if ( isDegraded ) {
		sockfd = original->getSocket();
		MasterWorker::slaveSockets->get( sockfd )->counter.decreaseDegraded();
		Master::getInstance()->remapMsgHandler.ackTransit( original->getAddr() );
	} else {
		sockfd = event.socket->getSocket();
		MasterWorker::slaveSockets->get( sockfd )->counter.decreaseNormal();
		Master::getInstance()->remapMsgHandler.ackTransit( event.socket->getAddr() );
	}

	applicationEvent.resUpdate( ( ApplicationSocket * ) pid.ptr, pid.id, keyValueUpdate, success );
	MasterWorker::eventQueue->insert( applicationEvent );

	return true;
}

bool MasterWorker::handleDeleteResponse( SlaveEvent event, bool success, bool isDegraded, char *buf, size_t size ) {
	struct KeyHeader header;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleDeleteResponse", "Invalid DELETE Response." );
		return false;
	}

	ApplicationEvent applicationEvent;
	PendingIdentifier pid;
	Key key;
	SlaveSocket *original;
	int sockfd;

	if ( ! MasterWorker::pending->eraseKey( PT_SLAVE_DEL, event.id, ( void * ) event.socket, &pid, &key ) ) {
		__ERROR__( "MasterWorker", "handleDeleteResponse", "Cannot find a pending slave DELETE request that matches the response. This message will be discarded." );
		return false;
	}
	original = ( SlaveSocket * ) key.ptr;

	if ( ! MasterWorker::pending->eraseKey( PT_APPLICATION_DEL, pid.parentId, 0, &pid, &key, true, true, true, header.key ) ) {
		__ERROR__( "MasterWorker", "handleDeleteResponse", "Cannot find a pending application DELETE request that matches the response. This message will be discarded." );
		return false;
	}

	if ( isDegraded ) {
		sockfd = original->getSocket();
		MasterWorker::slaveSockets->get( sockfd )->counter.decreaseDegraded();
		Master::getInstance()->remapMsgHandler.ackTransit( original->getAddr() );
	} else {
		sockfd = event.socket->getSocket();
		MasterWorker::slaveSockets->get( sockfd )->counter.decreaseNormal();
		Master::getInstance()->remapMsgHandler.ackTransit( event.socket->getAddr() );
	}

	applicationEvent.resDelete( ( ApplicationSocket * ) key.ptr, pid.id, key, success );
	MasterWorker::eventQueue->insert( applicationEvent );

	// TODO remove remapping records

	return true;
}
