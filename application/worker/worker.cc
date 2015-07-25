#include "worker.hh"
#include "../../common/util/debug.hh"

#define WORKER_COLOR	YELLOW

void ApplicationWorker::dispatch( MixedEvent event ) {
	switch( event.type ) {
		case EVENT_TYPE_APPLICATION:
			this->dispatch( event.event.application );
			break;
		case EVENT_TYPE_MASTER:
			this->dispatch( event.event.master );
			break;
		default:
			break;
	}
}

void ApplicationWorker::dispatch( ApplicationEvent event ) {
}

void ApplicationWorker::dispatch( MasterEvent event ) {
	bool connected, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;
	size_t valueSize;

	switch( event.type ) {
		case MASTER_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterMaster( buffer.size );
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_SET_REQUEST:
			// Read contents from file
			valueSize = ::read( event.message.set.fd, this->buffer.value, this->buffer.valueSize );
			buffer.data = this->protocol.reqSet(
				buffer.size,
				event.message.set.key,
				event.message.set.keySize,
				this->buffer.value,
				valueSize
			);
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_GET_REQUEST:
			buffer.data = this->protocol.reqGet(
				buffer.size,
				event.message.get.key,
				event.message.get.keySize
			);
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_PENDING:
			isSend = false;
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "ApplicationWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else {
		ProtocolHeader header;

		ret = event.socket->recv( this->protocol.buffer.recv, PROTO_HEADER_SIZE, connected, true );
		if ( ret == PROTO_HEADER_SIZE && connected ) {
			this->protocol.parseHeader( header, this->protocol.buffer.recv, ret );
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_MASTER ) {
				__ERROR__( "ApplicationWorker", "dispatch", "Invalid message source from master." );
				return;
			}
			switch( header.opcode ) {
				case PROTO_OPCODE_REGISTER:
					switch( header.magic ) {
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							event.socket->registered = true;
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							__ERROR__( "ApplicationWorker", "dispatch", "Failed to register with master." );
							break;
						default:
							__ERROR__( "ApplicationWorker", "dispatch", "Invalid magic code from master." );
							break;
					}
					break;
				default:
					__ERROR__( "ApplicationWorker", "dispatch", "Invalid opcode from master." );
					return;
			}
		}
	}
	if ( ! connected )
		__ERROR__( "ApplicationWorker", "dispatch", "The master is disconnected." );
}


void ApplicationWorker::free() {
	this->protocol.free();
	delete[] this->buffer.value;
}

void *ApplicationWorker::run( void *argv ) {
	ApplicationWorker *worker = ( ApplicationWorker * ) argv;
	WorkerRole role = worker->getRole();
	ApplicationEventQueue *eventQueue = worker->getEventQueue();

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
		case WORKER_ROLE_MASTER:
			MASTER_WORKER_EVENT_LOOP(
				MasterEvent,
				eventQueue->separated.master
			);
			break;
		default:
			break;
	}

	worker->free();
	pthread_exit( 0 );
	return 0;
}

bool ApplicationWorker::init( ApplicationConfig &config, WorkerRole role, ApplicationEventQueue *eventQueue ) {
	this->buffer.value = new char[ config.size.chunk ];
	this->buffer.valueSize = config.size.chunk;

	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			config.size.key,
			config.size.chunk
		)
	);
	this->role = role;
	this->eventQueue = eventQueue;
	return role != WORKER_ROLE_UNDEFINED;
}

bool ApplicationWorker::start() {
	this->isRunning = true;
	if ( pthread_create( &this->tid, NULL, ApplicationWorker::run, ( void * ) this ) != 0 ) {
		__ERROR__( "ApplicationWorker", "start", "Cannot start worker thread." );
		return false;
	}
	return true;
}

void ApplicationWorker::stop() {
	this->isRunning = false;
}

void ApplicationWorker::print( FILE *f ) {
	char role[ 16 ];
	switch( this->role ) {
		case WORKER_ROLE_MIXED:
			strcpy( role, "Mixed" );
			break;
		case WORKER_ROLE_APPLICATION:
			strcpy( role, "Application" );
			break;
		case WORKER_ROLE_MASTER:
			strcpy( role, "Master" );
			break;
		default:
			return;
	}
	fprintf( f, "%11s worker (Thread ID = %lu): %srunning\n", role, this->tid, this->isRunning ? "" : "not " );
}
