#include "worker.hh"
#include "../main/master.hh"
#include "../../common/util/debug.hh"

#define WORKER_COLOR	YELLOW

uint32_t MasterWorker::dataChunkCount;
uint32_t MasterWorker::parityChunkCount;
Pending *MasterWorker::pending;
MasterEventQueue *MasterWorker::eventQueue;
StripeList<SlaveSocket> *MasterWorker::stripeList;

void MasterWorker::dispatch( MixedEvent event ) {
	switch( event.type ) {
		case EVENT_TYPE_APPLICATION:
			this->dispatch( event.event.application );
			break;
		case EVENT_TYPE_COORDINATOR:
			this->dispatch( event.event.coordinator );
			break;
		case EVENT_TYPE_MASTER:
			this->dispatch( event.event.master );
			break;
		case EVENT_TYPE_SLAVE:
			this->dispatch( event.event.slave );
			break;
		default:
			break;
	}
}

void MasterWorker::dispatch( ApplicationEvent event ) {
	bool success = true, connected, isSend;
	ssize_t ret;
	uint32_t valueSize;
	Key key;
	KeyValueUpdate keyValueUpdate;
	char *value;
	struct {
		size_t size;
		char *data;
	} buffer;
	std::set<Key>::iterator it;
	std::set<KeyValueUpdate>::iterator kvUpdateit;

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
			buffer.data = this->protocol.resRegisterApplication( buffer.size, success );
			break;
		case APPLICATION_EVENT_TYPE_GET_RESPONSE_SUCCESS:
			event.message.keyValue.deserialize(
				key.data, key.size,
				value, valueSize
			);
			buffer.data = this->protocol.resGet(
				buffer.size, success,
				key.size, key.data,
				valueSize, value
			);
			key.ptr = ( void * ) event.socket;

			pthread_mutex_lock( &MasterWorker::pending->applications.getLock );
			it = MasterWorker::pending->applications.get.find( key );
			key = *it;
			MasterWorker::pending->applications.get.erase( it );
			pthread_mutex_unlock( &MasterWorker::pending->applications.getLock );

			key.free();
			event.message.keyValue.free();
			break;
		case APPLICATION_EVENT_TYPE_GET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resGet(
				buffer.size,
				success,
				event.message.key.size,
				event.message.key.data
			);

			pthread_mutex_lock( &MasterWorker::pending->applications.getLock );
			it = MasterWorker::pending->applications.get.find( event.message.key );
			key = *it;
			MasterWorker::pending->applications.get.erase( it );
			pthread_mutex_unlock( &MasterWorker::pending->applications.getLock );

			key.free();
			break;
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resSet(
				buffer.size,
				success,
				event.message.key.size,
				event.message.key.data
			);

			pthread_mutex_lock( &MasterWorker::pending->applications.setLock );
			it = MasterWorker::pending->applications.set.find( event.message.key );
			key = *it;
			MasterWorker::pending->applications.set.erase( it );
			pthread_mutex_unlock( &MasterWorker::pending->applications.setLock );

			key.free();
			break;
		case APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
			buffer.data = this->protocol.resUpdate(
				buffer.size,
				success,
				event.message.update.key.size,
				event.message.update.key.data,
				event.message.update.offset,
				event.message.update.length
			);
			keyValueUpdate.size = event.message.update.key.size;
			keyValueUpdate.data = event.message.update.key.data;
			keyValueUpdate.ptr = event.socket;
			keyValueUpdate.offset = event.message.update.offset;
			keyValueUpdate.length = event.message.update.length;

			pthread_mutex_lock( &MasterWorker::pending->applications.updateLock );
			kvUpdateit = MasterWorker::pending->applications.update.find( keyValueUpdate );
			keyValueUpdate = *kvUpdateit;
			MasterWorker::pending->applications.update.erase( kvUpdateit );
			pthread_mutex_unlock( &MasterWorker::pending->applications.updateLock );

			keyValueUpdate.free();
			break;
		case APPLICATION_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
			buffer.data = this->protocol.resDelete(
				buffer.size,
				success,
				event.message.key.size,
				event.message.key.data
			);

			pthread_mutex_lock( &MasterWorker::pending->applications.delLock );
			it = MasterWorker::pending->applications.del.find( event.message.key );
			key = *it;
			MasterWorker::pending->applications.del.erase( it );
			pthread_mutex_unlock( &MasterWorker::pending->applications.delLock );

			key.free();
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
		ret = event.socket->recv(
			this->protocol.buffer.recv,
			this->protocol.buffer.size,
			connected,
			false
		);
		buffer.data = this->protocol.buffer.recv;
		buffer.size = ret > 0 ? ( size_t ) ret : 0;
		while ( buffer.size > 0 ) {
			if ( ! this->protocol.parseHeader( header, buffer.data, buffer.size ) ) {
				__ERROR__( "MasterWorker", "dispatch", "Undefined message (remaining bytes = %lu).", buffer.size );
				break;
			}
			__DEBUG__( YELLOW, "MasterWorker", "dispatch", "header.length = %u", header.length );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			if (
				header.magic != PROTO_MAGIC_REQUEST ||
				header.from != PROTO_MAGIC_FROM_APPLICATION
			) {
				__ERROR__( "MasterWorker", "dispatch", "Invalid protocol header." );
				return;
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
					__ERROR__( "MasterWorker", "dispatch", "Invalid opcode from application." );
					return;
			}
			buffer.data += header.length;
			buffer.size -= header.length;
		}
	}

	if ( ! connected )
		__ERROR__( "MasterWorker", "dispatch", "The application is disconnected." );
}

void MasterWorker::dispatch( CoordinatorEvent event ) {
	bool connected, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	switch( event.type ) {
		case COORDINATOR_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterCoordinator( buffer.size, event.message.address.addr, event.message.address.port );
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
			__ERROR__( "MasterWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
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
		while ( buffer.size > 0 ) {
			if ( ! this->protocol.parseHeader( header, buffer.data, buffer.size ) ) {
				__ERROR__( "MasterWorker", "dispatch", "Undefined message (remaining bytes = %lu).", buffer.size );
				break;
			}

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_COORDINATOR ) {
				__ERROR__( "MasterWorker", "dispatch", "Invalid message source from coordinator." );
				continue;
			}
			switch( header.opcode ) {
				case PROTO_OPCODE_REGISTER:
					switch( header.magic ) {
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							event.socket->registered = true;
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							__ERROR__( "MasterWorker", "dispatch", "Failed to register with coordinator." );
							break;
						default:
							__ERROR__( "MasterWorker", "dispatch", "Invalid magic code from coordinator." );
							break;
					}
					break;
				default:
					__ERROR__( "MasterWorker", "dispatch", "Invalid opcode from coordinator." );
					continue;
			}
			buffer.data += header.length;
			buffer.size -= header.length;
		}
	}
	if ( ! connected )
		__ERROR__( "MasterWorker", "dispatch", "The coordinator is disconnected." );
}

void MasterWorker::dispatch( MasterEvent event ) {
}

void MasterWorker::dispatch( SlaveEvent event ) {
	bool connected, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	switch( event.type ) {
		case SLAVE_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterSlave( buffer.size, event.message.address.addr, event.message.address.port );
			isSend = true;
			break;
		case SLAVE_EVENT_TYPE_SEND:
			buffer.data = event.message.send.protocol->buffer.send;
			buffer.size = event.message.send.size;
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
			event.message.send.protocol->status[ event.message.send.index ] = false;
		}
	} else {
		// Parse responses from slaves
		ProtocolHeader header;
		ret = event.socket->recv(
			this->protocol.buffer.recv,
			this->protocol.buffer.size,
			connected,
			false
		);
		buffer.data = this->protocol.buffer.recv;
		buffer.size = ret > 0 ? ( size_t ) ret : 0;
		while ( buffer.size > 0 ) {
			if ( ! this->protocol.parseHeader( header, buffer.data, buffer.size ) ) {
				__ERROR__( "MasterWorker", "dispatch", "Undefined message (remaining bytes = %lu).", buffer.size );
				break;
			}

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_SLAVE ) {
				__ERROR__( "MasterWorker", "dispatch", "Invalid message source from slave." );
				continue;
			}

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
					continue;
			}

			switch( header.opcode ) {
				case PROTO_OPCODE_REGISTER:
					if ( success ) {
						event.socket->registered = true;
					} else {
						__ERROR__( "MasterWorker", "dispatch", "Failed to register with slave." );
					}
					break;
				case PROTO_OPCODE_GET:
					this->handleGetResponse( event, success, buffer.data, buffer.size );
					break;
				case PROTO_OPCODE_SET:
					this->handleSetResponse( event, success, buffer.data, buffer.size );
					break;
				case PROTO_OPCODE_UPDATE:
					this->handleUpdateResponse( event, success, buffer.data, buffer.size );
					break;
				case PROTO_OPCODE_DELETE:
					this->handleDeleteResponse( event, success, buffer.data, buffer.size );
					break;
				default:
					__ERROR__( "MasterWorker", "dispatch", "Invalid opcode from slave." );
					continue;
			}
			buffer.data += header.length;
			buffer.size -= header.length;
		}
	}
	if ( ! connected )
		__ERROR__( "MasterWorker", "dispatch", "The slave is disconnected." );
}

SlaveSocket *MasterWorker::getSlave( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId, bool allowDegraded, bool *isDegraded ) {
	SlaveSocket *ret;
	listId = MasterWorker::stripeList->get(
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
		for ( uint32_t i = 0; i < MasterWorker::dataChunkCount + MasterWorker::parityChunkCount; i++ ) {
			ret = MasterWorker::stripeList->get( listId, chunkId, i );
			if ( ret->ready() )
				return ret;
		}
		__ERROR__( "MasterWorker", "getSlave", "Cannot find a slave for performing degraded operation." );
		return 0;
	}

	return 0;
}

SlaveSocket *MasterWorker::getSlaves( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId, bool allowDegraded, bool *isDegraded ) {
	SlaveSocket *ret = this->getSlave( data, size, listId, chunkId, allowDegraded, isDegraded );

	if ( isDegraded ) *isDegraded = false;
	for ( uint32_t i = 0; i < MasterWorker::parityChunkCount; i++ ) {
		if ( ! this->paritySlaveSockets[ i ]->ready() ) {
			if ( ! allowDegraded )
				return 0;
			if ( isDegraded ) *isDegraded = true;

			for ( uint32_t i = 0; i < MasterWorker::dataChunkCount + MasterWorker::parityChunkCount; i++ ) {
				SlaveSocket *s = MasterWorker::stripeList->get( listId, chunkId, i );
				if ( s->ready() ) {
					this->paritySlaveSockets[ i ] = s;
					break;
				} else if ( i == MasterWorker::dataChunkCount + MasterWorker::parityChunkCount - 1 ) {
					__ERROR__( "MasterWorker", "getSlave", "Cannot find a slave for performing degraded operation." );
					return 0;
				}
			}
		}
	}
	return ret;
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

	uint32_t listId, chunkId;
	bool isDegraded, connected;
	SlaveSocket *socket;

	socket = this->getSlave(
		header.key, header.keySize,
		listId, chunkId, true, &isDegraded
	);
	if ( ! socket ) {
		Key key;
		key.set( header.keySize, header.key );
		event.resGet( event.socket, key );
		this->dispatch( event );
		return false;
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	ssize_t sentBytes;

	buffer.data = this->protocol.reqGet( buffer.size, header.key, header.keySize );

	key.dup( header.keySize, header.key, ( void * ) event.socket );

	pthread_mutex_lock( &MasterWorker::pending->applications.getLock );
	MasterWorker::pending->applications.get.insert( key );
	pthread_mutex_unlock( &MasterWorker::pending->applications.getLock );

	key.ptr = ( void * ) socket;

	pthread_mutex_lock( &MasterWorker::pending->slaves.getLock );
	MasterWorker::pending->slaves.get.insert( key );
	pthread_mutex_unlock( &MasterWorker::pending->slaves.getLock );

	// Send GET request
	sentBytes = socket->send( buffer.data, buffer.size, connected );
	if ( sentBytes != ( ssize_t ) buffer.size ) {
		__ERROR__( "MasterWorker", "handleGetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
		return false;
	}

	return true;
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
	bool isDegraded, connected;
	SlaveSocket *socket;

	socket = this->getSlaves(
		header.key, header.keySize,
		listId, chunkId, true, &isDegraded
	);
	if ( ! socket ) {
		Key key;
		key.set( header.keySize, header.key );
		event.resSet( event.socket, key, false );
		this->dispatch( event );
		return false;
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	ssize_t sentBytes;

	buffer.data = this->protocol.reqSet( buffer.size, header.key, header.keySize, header.value, header.valueSize );

	key.dup( header.keySize, header.key, ( void * ) event.socket );

	pthread_mutex_lock( &MasterWorker::pending->applications.setLock );
	MasterWorker::pending->applications.set.insert( key );
	pthread_mutex_unlock( &MasterWorker::pending->applications.setLock );

	key.ptr = ( void * ) socket;

	pthread_mutex_lock( &MasterWorker::pending->slaves.setLock );
	MasterWorker::pending->slaves.set.insert( key );
	pthread_mutex_unlock( &MasterWorker::pending->slaves.setLock );

	// Send SET requests
	for ( uint32_t i = 0; i < MasterWorker::parityChunkCount; i++ ) {
		key.ptr = ( void * ) this->paritySlaveSockets[ i ];

		pthread_mutex_lock( &MasterWorker::pending->slaves.setLock );
		MasterWorker::pending->slaves.set.insert( key );
		pthread_mutex_unlock( &MasterWorker::pending->slaves.setLock );

#ifdef MASTER_WORKER_SEND_REPLICAS_PARALLEL
		SlaveEvent slaveEvent;
		this->protocol.status[ i ] = true;

		slaveEvent.send( this->paritySlaveSockets[ i ], &this->protocol, buffer.size, i );
		MasterWorker::eventQueue->insert( slaveEvent );
#else
		sentBytes = this->paritySlaveSockets[ i ]->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "MasterWorker", "handleSetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
		}
#endif
	}

	sentBytes = socket->send( buffer.data, buffer.size, connected );
	if ( sentBytes != ( ssize_t ) buffer.size ) {
		__ERROR__( "MasterWorker", "handleSetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
		return false;
	}

#ifdef MASTER_WORKER_SEND_REPLICAS_PARALLEL
	// Wait until all replicas are sent
	for ( uint32_t i = 0; i < MasterWorker::parityChunkCount; i++ ) {
		while( this->protocol.status[ i ] ); // Busy waiting
	}
#endif

	return true;
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

	uint32_t listId, chunkId;
	bool isDegraded, connected;
	SlaveSocket *socket;

	socket = this->getSlave(
		header.key, header.keySize,
		listId, chunkId, true, &isDegraded
	);

	if ( ! socket ) {
		Key key;
		key.set( header.keySize, header.key );
		event.resUpdate( event.socket, key, header.valueUpdateOffset, header.valueUpdateSize, false );
		this->dispatch( event );
		return false;
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	KeyValueUpdate keyValueUpdate;
	ssize_t sentBytes;

	buffer.data = this->protocol.reqUpdate(
		buffer.size, header.key, header.keySize,
		header.valueUpdate, header.valueUpdateOffset, header.valueUpdateSize
	);

	keyValueUpdate.dup( header.keySize, header.key, ( void * ) event.socket );
	keyValueUpdate.offset = header.valueUpdateOffset;
	keyValueUpdate.length = header.valueUpdateSize;

	pthread_mutex_lock( &MasterWorker::pending->applications.updateLock );
	MasterWorker::pending->applications.update.insert( keyValueUpdate );
	pthread_mutex_unlock( &MasterWorker::pending->applications.updateLock );

	keyValueUpdate.ptr = ( void * ) socket;
	pthread_mutex_lock( &MasterWorker::pending->slaves.updateLock );
	MasterWorker::pending->slaves.update.insert( keyValueUpdate );
	pthread_mutex_unlock( &MasterWorker::pending->slaves.updateLock );

	// Send UPDATE request
	sentBytes = socket->send( buffer.data, buffer.size, connected );
	if ( sentBytes != ( ssize_t ) buffer.size ) {
		__ERROR__( "MasterWorker", "handleUpdateRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
		return false;
	}

	return true;
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

	uint32_t listId, chunkId;
	bool isDegraded, connected;
	SlaveSocket *socket;

	socket = this->getSlave(
		header.key, header.keySize,
		listId, chunkId, true, &isDegraded
	);

	if ( ! socket ) {
		Key key;
		key.set( header.keySize, header.key );
		event.resDelete( event.socket, key, false );
		this->dispatch( event );
		return false;
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	ssize_t sentBytes;

	buffer.data = this->protocol.reqDelete( buffer.size, header.key, header.keySize );

	key.dup( header.keySize, header.key, ( void * ) event.socket );
	pthread_mutex_lock( &MasterWorker::pending->applications.delLock );
	MasterWorker::pending->applications.del.insert( key );
	pthread_mutex_unlock( &MasterWorker::pending->applications.delLock );

	key.ptr = ( void * ) socket;
	pthread_mutex_lock( &MasterWorker::pending->slaves.delLock );
	MasterWorker::pending->slaves.del.insert( key );
	pthread_mutex_unlock( &MasterWorker::pending->slaves.delLock );

	// Send DELETE requests
	sentBytes = socket->send( buffer.data, buffer.size, connected );
	if ( sentBytes != ( ssize_t ) buffer.size ) {
		__ERROR__( "MasterWorker", "handleDeleteRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
		return false;
	}

	return true;
}

bool MasterWorker::handleGetResponse( SlaveEvent event, bool success, char *buf, size_t size ) {
	Key key;
	KeyValue keyValue;
	if ( success ) {
		struct KeyValueHeader header;
		if ( this->protocol.parseKeyValueHeader( header, buf, size ) ) {
			key.set( header.keySize, header.key, ( void * ) event.socket );
			keyValue.dup( header.key, header.keySize, header.value, header.valueSize );
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

	std::set<Key>::iterator it;
	ApplicationEvent applicationEvent;

	pthread_mutex_lock( &MasterWorker::pending->slaves.getLock );
	it = MasterWorker::pending->slaves.get.find( key );
	if ( it == MasterWorker::pending->slaves.get.end() ) {
		pthread_mutex_unlock( &MasterWorker::pending->slaves.getLock );
		__ERROR__( "MasterWorker", "handleGetResponse", "Cannot find a pending slave GET request that matches the response. This message will be discarded." );
		if ( success ) keyValue.free();
		return false;
	}
	MasterWorker::pending->slaves.get.erase( it );
	pthread_mutex_unlock( &MasterWorker::pending->slaves.getLock );

	key.ptr = 0;
	pthread_mutex_lock( &MasterWorker::pending->applications.getLock );
	it = MasterWorker::pending->applications.get.lower_bound( key );
	if ( it == MasterWorker::pending->applications.get.end() || ! key.equal( *it ) ) {
		pthread_mutex_unlock( &MasterWorker::pending->applications.getLock );
		__ERROR__( "MasterWorker", "handleGetResponse", "Cannot find a pending application GET request that matches the response. This message will be discarded." );
		if ( success ) keyValue.free();
		return false;
	}
	key = *it;
	pthread_mutex_unlock( &MasterWorker::pending->applications.getLock );

	if ( success )
		applicationEvent.resGet( ( ApplicationSocket * ) key.ptr, keyValue );
	else
		applicationEvent.resGet( ( ApplicationSocket * ) key.ptr, key );
	MasterWorker::eventQueue->insert( applicationEvent );
	return true;
}

bool MasterWorker::handleSetResponse( SlaveEvent event, bool success, char *buf, size_t size ) {
	struct KeyHeader header;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleSetResponse", "Invalid SET response." );
		return false;
	}

	int pending;
	std::set<Key>::iterator it;
	ApplicationEvent applicationEvent;
	Key key;

	key.set( header.keySize, header.key, ( void * ) event.socket );

	pthread_mutex_lock( &MasterWorker::pending->slaves.setLock );
	it = MasterWorker::pending->slaves.set.find( key );
	if ( it == MasterWorker::pending->slaves.set.end() ) {
		pthread_mutex_unlock( &MasterWorker::pending->slaves.setLock );
		__ERROR__( "MasterWorker", "handleSetResponse", "Cannot find a pending slave SET request that matches the response. This message will be discarded." );
		return false;
	}
	MasterWorker::pending->slaves.set.erase( it );

	// Check pending slave SET requests
	key.ptr = 0;
	it = MasterWorker::pending->slaves.set.lower_bound( key );
	for ( pending = 0; it != MasterWorker::pending->slaves.set.end() && key.equal( *it ); pending++, it++ );
	pthread_mutex_unlock( &MasterWorker::pending->slaves.setLock );
	__ERROR__( "MasterWorker", "handleSetResponse", "Pending slave SET requests = %d.", pending );

	if ( pending == 0 ) {
		// Only send application SET response when the number of pending slave SET requests equal 0
		pthread_mutex_lock( &MasterWorker::pending->applications.setLock );
		it = MasterWorker::pending->applications.set.lower_bound( key );
		if ( it == MasterWorker::pending->applications.set.end() || ! key.equal( *it ) ) {
			pthread_mutex_unlock( &MasterWorker::pending->applications.setLock );
			__ERROR__( "MasterWorker", "handleSetResponse", "Cannot find a pending application SET request that matches the response. This message will be discarded." );
			return false;
		}
		key = *it;
		pthread_mutex_unlock( &MasterWorker::pending->applications.setLock );

		applicationEvent.resSet( ( ApplicationSocket * ) key.ptr, key, success );
		MasterWorker::eventQueue->insert( applicationEvent );
	}
	return true;
}

bool MasterWorker::handleUpdateResponse( SlaveEvent event, bool success, char *buf, size_t size ) {
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

	std::set<KeyValueUpdate>::iterator it;
	KeyValueUpdate keyValueUpdate;
	ApplicationEvent applicationEvent;

	// Find the cooresponding request
	keyValueUpdate.set( header.keySize, header.key, ( void * ) event.socket );
	keyValueUpdate.offset = header.valueUpdateOffset;
	keyValueUpdate.length = header.valueUpdateSize;

	pthread_mutex_lock( &MasterWorker::pending->slaves.updateLock );
	it = MasterWorker::pending->slaves.update.find( keyValueUpdate );
	if ( it == MasterWorker::pending->slaves.update.end() ) {
		pthread_mutex_unlock( &MasterWorker::pending->slaves.updateLock );
		__ERROR__( "MasterWorker", "handleUpdateResponse", "Cannot find a pending slave UPDATE request that matches the response. This message will be discarded." );
		return false;
	}
	MasterWorker::pending->slaves.update.erase( it );
	pthread_mutex_unlock( &MasterWorker::pending->slaves.updateLock );

	keyValueUpdate.ptr = 0;
	pthread_mutex_lock( &MasterWorker::pending->applications.updateLock );
	it = MasterWorker::pending->applications.update.lower_bound( keyValueUpdate );
	if ( it == MasterWorker::pending->applications.update.end() || ! keyValueUpdate.equal( *it ) ) {
		pthread_mutex_unlock( &MasterWorker::pending->applications.updateLock );
		__ERROR__( "MasterWorker", "handleUpdateResponse", "Cannot find a pending application UPDATE request that matches the response. This message will be discarded." );
		return false;
	}
	pthread_mutex_unlock( &MasterWorker::pending->applications.updateLock );

	keyValueUpdate = *it;
	applicationEvent.resUpdate(
		( ApplicationSocket * ) keyValueUpdate.ptr,
		keyValueUpdate,
		keyValueUpdate.offset,
		keyValueUpdate.length,
		success
	);
	MasterWorker::eventQueue->insert( applicationEvent );

	return true;
}

bool MasterWorker::handleDeleteResponse( SlaveEvent event, bool success, char *buf, size_t size ) {
	struct KeyHeader header;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleDeleteResponse", "Invalid DELETE Response." );
		return false;
	}

	ApplicationEvent applicationEvent;
	std::set<Key>::iterator it;
	Key key;

	key.set( header.keySize, header.key, ( void * ) event.socket );

	pthread_mutex_lock( &MasterWorker::pending->slaves.delLock );
	it = MasterWorker::pending->slaves.del.find( key );
	if ( it == MasterWorker::pending->slaves.del.end() ) {
		pthread_mutex_unlock( &MasterWorker::pending->slaves.delLock );
		__ERROR__( "MasterWorker", "handleDeleteResponse", "Cannot find a pending slave DELETE request that matches the response. This message will be discarded." );
		return false;
	}
	MasterWorker::pending->slaves.del.erase( it );
	pthread_mutex_unlock( &MasterWorker::pending->slaves.delLock );

	key.ptr = 0;
	pthread_mutex_lock( &MasterWorker::pending->applications.delLock );
	it = MasterWorker::pending->applications.del.lower_bound( key );
	if ( it == MasterWorker::pending->applications.del.end() || ! key.equal( *it ) ) {
		pthread_mutex_unlock( &MasterWorker::pending->applications.delLock );
		__ERROR__( "MasterWorker", "handleDeleteResponse", "Cannot find a pending application DELETE request that matches the response. This message will be discarded." );
		return false;
	}
	key = *it;
	pthread_mutex_unlock( &MasterWorker::pending->applications.delLock );

	applicationEvent.resDelete( ( ApplicationSocket * ) key.ptr, key, success );
	MasterWorker::eventQueue->insert( applicationEvent );

	return true;
}

void MasterWorker::free() {
	this->protocol.free();
	delete[] this->dataSlaveSockets;
	delete[] this->paritySlaveSockets;
}

void *MasterWorker::run( void *argv ) {
	MasterWorker *worker = ( MasterWorker * ) argv;
	WorkerRole role = worker->getRole();
	MasterEventQueue *eventQueue = MasterWorker::eventQueue;

#define MASTER_WORKER_EVENT_LOOP(_EVENT_TYPE_, _EVENT_QUEUE_) \
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
			MASTER_WORKER_EVENT_LOOP(
				MixedEvent,
				eventQueue->mixed
			);
			break;
		case WORKER_ROLE_APPLICATION:
			MASTER_WORKER_EVENT_LOOP(
				ApplicationEvent,
				eventQueue->separated.application
			);
			break;
		case WORKER_ROLE_COORDINATOR:
			MASTER_WORKER_EVENT_LOOP(
				CoordinatorEvent,
				eventQueue->separated.coordinator
			);
			break;
		case WORKER_ROLE_MASTER:
			MASTER_WORKER_EVENT_LOOP(
				MasterEvent,
				eventQueue->separated.master
			);
			break;
		case WORKER_ROLE_SLAVE:
			MASTER_WORKER_EVENT_LOOP(
				SlaveEvent,
				eventQueue->separated.slave
			);
			break;
		default:
			break;
	}

	worker->free();
	pthread_exit( 0 );
	return 0;
}

bool MasterWorker::init() {
	Master *master = Master::getInstance();

	MasterWorker::dataChunkCount = master->config.global.coding.params.getDataChunkCount();
	MasterWorker::parityChunkCount = master->config.global.coding.params.getParityChunkCount();
	MasterWorker::pending = &master->pending;
	MasterWorker::eventQueue = &master->eventQueue;
	MasterWorker::stripeList = master->stripeList;
	return true;
}

bool MasterWorker::init( GlobalConfig &config, WorkerRole role ) {
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			config.size.key,
			config.size.chunk
		),
		MasterWorker::parityChunkCount
	);
	this->dataSlaveSockets = new SlaveSocket*[ MasterWorker::dataChunkCount ];
	this->paritySlaveSockets = new SlaveSocket*[ MasterWorker::parityChunkCount ];
	this->role = role;
	return role != WORKER_ROLE_UNDEFINED;
}

bool MasterWorker::start() {
	this->isRunning = true;
	if ( pthread_create( &this->tid, NULL, MasterWorker::run, ( void * ) this ) != 0 ) {
		__ERROR__( "MasterWorker", "start", "Cannot start worker thread." );
		return false;
	}
	return true;
}

void MasterWorker::stop() {
	this->isRunning = false;
}

void MasterWorker::print( FILE *f ) {
	char role[ 16 ];
	switch( this->role ) {
		case WORKER_ROLE_MIXED:
			strcpy( role, "Mixed" );
			break;
		case WORKER_ROLE_APPLICATION:
			strcpy( role, "Application" );
			break;
		case WORKER_ROLE_COORDINATOR:
			strcpy( role, "Coordinator" );
			break;
		case WORKER_ROLE_MASTER:
			strcpy( role, "Master" );
			break;
		case WORKER_ROLE_SLAVE:
			strcpy( role, "Slave" );
			break;
		default:
			return;
	}
	fprintf( f, "%11s worker (Thread ID = %lu): %srunning\n", role, this->tid, this->isRunning ? "" : "not " );
}
