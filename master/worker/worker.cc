#include "worker.hh"
#include "../../common/util/debug.hh"

#define WORKER_COLOR	YELLOW

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
	bool connected, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	switch( event.type ) {
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resRegisterApplication( buffer.size, true );
			isSend = true;
			break;
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRegisterApplication( buffer.size, false );
			isSend = true;
			break;
		case APPLICATION_EVENT_TYPE_PENDING:
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
		// Parse requests from applications
		ProtocolHeader header;
		ret = event.socket->recv(
			this->protocol.buffer.data,
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

			switch( header.opcode ) {
				SlaveSocket *dataSockets[ 3 ];
				SlaveSocket *paritySockets;
				unsigned int index;

				case PROTO_OPCODE_GET:
					if ( this->protocol.parseKeyHeader( keyHeader, PROTO_HEADER_SIZE ) ) {
						__DEBUG__(
							BLUE, "MasterWorker", "dispatch",
							"[GET] Key: %.*s (key size = %u).",
							( int ) keyHeader.keySize,
							keyHeader.key,
							keyHeader.keySize
						);

						index = MasterWorker::stripeList->get(
							keyHeader.key,
							( size_t ) keyHeader.keySize,
							dataSockets,
							&paritySockets,
							true
						);

						// Test for consistent hashing to stripe list
						printf(
							"Key: %.*s hashes to ((",
							( int ) keyHeader.keySize,
							keyHeader.key
						);
						for ( int i = 0; i < 3; i++ ) {
							if ( i > 0 )
								printf( ", " );
							dataSockets[ i ]->printAddress();
							if ( i == index )
								printf( "*" );
						}
						printf( ", (" );
						paritySockets->printAddress();
						printf( "))\n" );
					}
					break;
				case PROTO_OPCODE_SET:
					if ( this->protocol.parseKeyValueHeader( keyValueHeader, PROTO_HEADER_SIZE ) ) {
						__DEBUG__(
							BLUE, "MasterWorker", "dispatch",
							"[SET] Key: %.*s (key size = %u); Value: %.*s (value size = %u)",
							( int ) keyValueHeader.keySize,
							keyValueHeader.key,
							keyValueHeader.keySize,
							( int ) keyValueHeader.valueSize,
							keyValueHeader.value,
							keyValueHeader.valueSize
						);
					}
					break;
			}
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

		ret = event.socket->recv( this->protocol.buffer.data, PROTO_HEADER_SIZE, connected, true );
		if ( ret == PROTO_HEADER_SIZE && connected ) {
			this->protocol.parseHeader( header, this->protocol.buffer.data, ret );
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
		ProtocolHeader header;

		ret = event.socket->recv( this->protocol.buffer.data, PROTO_HEADER_SIZE, connected, true );
		if ( ret == PROTO_HEADER_SIZE && connected ) {
			this->protocol.parseHeader( header, this->protocol.buffer.data, ret );
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_SLAVE ) {
				__ERROR__( "MasterWorker", "dispatch", "Invalid message source from slave." );
				return;
			}
			switch( header.opcode ) {
				case PROTO_OPCODE_REGISTER:
					switch( header.magic ) {
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							event.socket->registered = true;
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							__ERROR__( "MasterWorker", "dispatch", "Failed to register with slave." );
							break;
						default:
							__ERROR__( "MasterWorker", "dispatch", "Invalid magic code from slave." );
							break;
					}
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
}

void *MasterWorker::run( void *argv ) {
	MasterWorker *worker = ( MasterWorker * ) argv;
	WorkerRole role = worker->getRole();
	MasterEventQueue *eventQueue = worker->getEventQueue();

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

bool MasterWorker::init( MasterEventQueue *eventQueue, StripeList<SlaveSocket> *stripeList ) {
	MasterWorker::eventQueue = eventQueue;
	MasterWorker::stripeList = stripeList;
	return true;
}

bool MasterWorker::init( GlobalConfig &config, WorkerRole role ) {
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			config.size.key,
			config.size.chunk
		)
	);
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
