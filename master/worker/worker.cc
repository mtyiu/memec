#include "worker.hh"
#include "../main/master.hh"
#include "../../common/util/debug.hh"

#define WORKER_COLOR	YELLOW

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
	char *value;
	struct {
		size_t size;
		char *data;
	} buffer;
	std::set<Key>::iterator it;

	switch( event.type ) {
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_GET_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS:
			success = true;
			isSend = true;
			break;
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
		case APPLICATION_EVENT_TYPE_GET_RESPONSE_FAILURE:
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE:
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
			it = MasterWorker::pending->applications.get.find( key );
			key = *it;
			key.free();
			event.message.keyValue.free();
			MasterWorker::pending->applications.get.erase( it );
			break;
		case APPLICATION_EVENT_TYPE_GET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resGet(
				buffer.size,
				success,
				event.message.key.size,
				event.message.key.data
			);
			it = MasterWorker::pending->applications.get.find( event.message.key );
			key = *it;
			key.free();
			MasterWorker::pending->applications.get.erase( it );
			break;
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resSet(
				buffer.size,
				success,
				event.message.key.size,
				event.message.key.data
			);
			it = MasterWorker::pending->applications.set.find( event.message.key );
			key = *it;
			key.free();
			MasterWorker::pending->applications.set.erase( it );
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
		if ( ! this->protocol.parseHeader( header ) ) {
			__ERROR__( "MasterWorker", "dispatch", "Undefined message." );
		} else {
			if (
				header.magic != PROTO_MAGIC_REQUEST ||
				header.from != PROTO_MAGIC_FROM_APPLICATION ||
				header.to != PROTO_MAGIC_TO_MASTER
			) {
				__ERROR__( "MasterWorker", "dispatch", "Invalid protocol header." );
			}

			struct KeyHeader keyHeader;
			struct KeyValueHeader keyValueHeader;
			unsigned int dataIndex;
			bool success = false;

			switch( header.opcode ) {
				case PROTO_OPCODE_GET:
					if ( this->protocol.parseKeyHeader( keyHeader ) ) {
						__DEBUG__(
							BLUE, "MasterWorker", "dispatch",
							"[GET] Key: %.*s (key size = %u).",
							( int ) keyHeader.keySize,
							keyHeader.key,
							keyHeader.keySize
						);
						buffer.data = this->protocol.reqGet(
							buffer.size,
							keyHeader.key,
							keyHeader.keySize
						);

						MasterWorker::stripeList->get(
							keyHeader.key,
							( size_t ) keyHeader.keySize,
							this->dataSlaveSockets,
							this->paritySlaveSockets,
							&dataIndex,
							true
						);

						Key key;
						key.dup( keyHeader.keySize, keyHeader.key, ( void * ) event.socket );
						MasterWorker::pending->applications.get.insert( key );

						key.ptr = ( void * ) this->dataSlaveSockets[ dataIndex ];
						MasterWorker::pending->slaves.get.insert( key );

						success = true;
					}
					break;
				case PROTO_OPCODE_SET:
					if ( this->protocol.parseKeyValueHeader( keyValueHeader ) ) {
						__DEBUG__(
							BLUE, "MasterWorker", "dispatch",
							"[SET] Key: %.*s (key size = %u); Value: (value size = %u)",
							( int ) keyValueHeader.keySize,
							keyValueHeader.key,
							keyValueHeader.keySize,
							keyValueHeader.valueSize
						);
						buffer.data = this->protocol.reqSet(
							buffer.size,
							keyValueHeader.key,
							keyValueHeader.keySize,
							keyValueHeader.value,
							keyValueHeader.valueSize
						);

						MasterWorker::stripeList->get(
							keyValueHeader.key,
							( size_t ) keyValueHeader.keySize,
							this->dataSlaveSockets,
							this->paritySlaveSockets,
							&dataIndex,
							true
						);

						Key key;
						key.dup( keyValueHeader.keySize, keyValueHeader.key, ( void * ) event.socket );
						MasterWorker::pending->applications.set.insert( key );

						key.ptr = ( void * ) this->dataSlaveSockets[ dataIndex ];
						MasterWorker::pending->slaves.set.insert( key );
						for ( uint32_t i = 0; i < this->parityChunkCount; i++ ) {
							key.ptr = ( void * ) this->paritySlaveSockets[ i ];
							MasterWorker::pending->slaves.set.insert( key );
						}

						success = true;
					}
					break;
			}

			if ( ! success )
				return;

			// Send request
			ret = this->dataSlaveSockets[ dataIndex ]->send( buffer.data, buffer.size, connected );

			if ( header.opcode == PROTO_OPCODE_SET ) {
				for ( uint32_t i = 0; i < this->parityChunkCount; i++ ) {
					ret = this->paritySlaveSockets[ i ]->send( buffer.data, buffer.size, connected );
				}
			}
			if ( ret != ( ssize_t ) buffer.size )
				__ERROR__( "ApplicationWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
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
			buffer.data = this->protocol.reqRegisterCoordinator( buffer.size );
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

		ret = event.socket->recv( this->protocol.buffer.recv, PROTO_HEADER_SIZE, connected, true );
		if ( ret == PROTO_HEADER_SIZE && connected ) {
			this->protocol.parseHeader( header, this->protocol.buffer.recv, ret );
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_COORDINATOR ) {
				__ERROR__( "MasterWorker", "dispatch", "Invalid message source from coordinator." );
				return;
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
					return;
			}
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
	int pending;
	struct {
		size_t size;
		char *data;
	} buffer;

	switch( event.type ) {
		case SLAVE_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterSlave( buffer.size );
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
	} else {
		// Parse responses from slaves
		ProtocolHeader header;
		struct KeyHeader keyHeader;
		struct KeyValueHeader keyValueHeader;
		Key key;
		std::set<Key>::iterator it;
		ApplicationEvent applicationEvent;

		ret = event.socket->recv(
			this->protocol.buffer.recv,
			PROTO_HEADER_SIZE,
			connected,
			true
		);
		if ( ret == PROTO_HEADER_SIZE && connected ) {
			bool success;

			this->protocol.parseHeader( header, this->protocol.buffer.recv, ret );

			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_SLAVE ) {
				__ERROR__( "MasterWorker", "dispatch", "Invalid message source from slave." );
				return;
			}

			// Receive remaining messages
			if ( header.length ) {
				ret = event.socket->recv(
					this->protocol.buffer.recv + PROTO_HEADER_SIZE,
					header.length,
					connected,
					true
				);
			}

			switch( header.magic ) {
				case PROTO_MAGIC_RESPONSE_SUCCESS:
					success = true;
					break;
				case PROTO_MAGIC_RESPONSE_FAILURE:
					success = false;
					break;
				default:
					__ERROR__( "MasterWorker", "dispatch", "Invalid magic code from slave." );
					return;
			}

			switch( header.opcode ) {
				case PROTO_OPCODE_REGISTER:
					if ( success ) {
						event.socket->registered = true;
					} else {
						__ERROR__( "MasterWorker", "dispatch", "Failed to register with slave." );
					}
					break;
				case PROTO_OPCODE_SET:
					this->protocol.parseKeyHeader( keyHeader );
					key.size = keyHeader.keySize;
					key.data = keyHeader.key;
					key.ptr = ( void * ) event.socket;
					it = MasterWorker::pending->slaves.set.find( key );
					if ( it == MasterWorker::pending->slaves.set.end() ) {
						__ERROR__( "MasterWorker", "dispatch", "Cannot find a pending slave SET request that matches the response. This message will be discarded." );
						return;
					}
					MasterWorker::pending->slaves.set.erase( it );

					// Check pending slave SET requests
					key.ptr = 0;
					it = MasterWorker::pending->slaves.set.lower_bound( key );
					for ( pending = 0; key.equal( *it ); pending++, it++ );
					__ERROR__( "MasterWorker", "dispatch", "Pending slave SET requests = %d.", pending );
					if ( pending == 0 ) {
						// Only send application SET response when the number of pending slave SET requests equal 0
						it = MasterWorker::pending->applications.set.lower_bound( key );
						if ( it == MasterWorker::pending->applications.set.end() ) {
							__ERROR__( "MasterWorker", "dispatch", "Cannot find a pending application SET request that matches the response. This message will be discarded." );
							return;
						}
						key = *it;
						applicationEvent.resSet( ( ApplicationSocket * ) ( *it ).ptr, key, success );
						MasterWorker::eventQueue->insert( applicationEvent );
					}
					break;
				case PROTO_OPCODE_GET:
					this->protocol.parseKeyValueHeader( keyValueHeader );
					key.size = keyValueHeader.keySize;
					key.data = keyValueHeader.key;
					key.ptr = ( void * ) event.socket;
					it = MasterWorker::pending->slaves.get.find( key );
					if ( it == MasterWorker::pending->slaves.get.end() ) {
						__ERROR__( "MasterWorker", "dispatch", "Cannot find a pending slave GET request that matches the response. This message will be discarded." );
						return;
					}
					MasterWorker::pending->slaves.get.erase( it );

					it = MasterWorker::pending->applications.get.lower_bound( key );
					if ( it == MasterWorker::pending->applications.get.end() ) {
						__ERROR__( "MasterWorker", "dispatch", "Cannot find a pending application GET request that matches the response. This message will be discarded." );
						return;
					}
					key = *it;

					if ( success ) {
						KeyValue keyValue;
						keyValue.dup(
							keyValueHeader.key,
							keyValueHeader.keySize,
							keyValueHeader.value,
							keyValueHeader.valueSize
						);
						applicationEvent.resGet( ( ApplicationSocket * ) ( *it ).ptr, keyValue );
					} else {
						applicationEvent.resGet( ( ApplicationSocket * ) ( *it ).ptr, key );
					}
					MasterWorker::eventQueue->insert( applicationEvent );
					MasterWorker::pending->slaves.get.erase( it );

					break;
				default:
					__ERROR__( "MasterWorker", "dispatch", "Invalid opcode from slave." );
					return;
			}
		}
	}
	if ( ! connected )
		__ERROR__( "MasterWorker", "dispatch", "The slave is disconnected." );
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
		)
	);
	this->dataChunkCount = config.coding.params.getDataChunkCount();
	this->parityChunkCount = config.coding.params.getParityChunkCount();
	this->dataSlaveSockets = new SlaveSocket*[ this->dataChunkCount ];
	this->paritySlaveSockets = new SlaveSocket*[ this->parityChunkCount ];
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
