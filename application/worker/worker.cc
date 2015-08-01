#include <cerrno>
#include "worker.hh"
#include "../main/application.hh"
#include "../../common/util/debug.hh"

#define WORKER_COLOR	YELLOW

ApplicationEventQueue *ApplicationWorker::eventQueue;
Pending *ApplicationWorker::pending;

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
			ret = ::read( event.message.set.fd, this->buffer.value, this->buffer.valueSize );
			::close( event.message.set.fd );
			if ( ret == -1 ) {
				__ERROR__( "ApplicationWorker", "dispatch", "read(): %s.", strerror( errno ) );
				return;
			}
			valueSize = ( size_t ) ret;
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
		Key key;
		switch( event.type ) {
			case MASTER_EVENT_TYPE_SET_REQUEST:
				key.dup(
					event.message.set.keySize,
					event.message.set.key,
					( void * ) ( uint64_t ) event.message.set.fd
				);
				ApplicationWorker::pending->application.set.insert( key );

				key.ptr = ( void * ) event.socket;
				ApplicationWorker::pending->masters.set.insert( key );
			case MASTER_EVENT_TYPE_GET_REQUEST:
				key.dup(
					event.message.get.keySize,
					event.message.get.key,
					( void * ) ( uint64_t ) event.message.set.fd
				);
				ApplicationWorker::pending->application.get.insert( key );

				key.ptr = ( void * ) event.socket;
				ApplicationWorker::pending->masters.get.insert( key );
				break;
			default:
				break;
		}

		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "ApplicationWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else {
		ProtocolHeader header;
		struct KeyHeader keyHeader;
		struct KeyValueHeader keyValueHeader;
		Key key;
		std::set<Key>::iterator it;
		int fd;

		ret = event.socket->recv( this->protocol.buffer.recv, PROTO_HEADER_SIZE, connected, true );
		if ( ret == PROTO_HEADER_SIZE && connected ) {
			bool success;

			this->protocol.parseHeader( header, this->protocol.buffer.recv, ret );
			
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_MASTER ) {
				__ERROR__( "ApplicationWorker", "dispatch", "Invalid message source from master." );
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
					__ERROR__( "ApplicationWorker", "dispatch", "Invalid magic code from master." );
					return;
			}

			switch( header.opcode ) {
				case PROTO_OPCODE_REGISTER:
					if ( success ) {
						event.socket->registered = true;
					} else {
						__ERROR__( "ApplicationWorker", "dispatch", "Failed to register with master." );
					}
					break;
				case PROTO_OPCODE_SET:
					this->protocol.parseKeyHeader( keyHeader );
					key.size = keyHeader.keySize;
					key.data = keyHeader.key;
					key.ptr = ( void * ) event.socket;
					it = ApplicationWorker::pending->masters.set.find( key );
					if ( it == ApplicationWorker::pending->masters.set.end() ) {
						__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending master SET request that matches the response. This message will be discarded." );
						return;
					}
					ApplicationWorker::pending->masters.set.erase( it );

					key.ptr = 0;
					it = ApplicationWorker::pending->application.set.lower_bound( key );
					if ( it == ApplicationWorker::pending->application.set.end() || ! key.equal( *it ) ) {
						__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending application SET request that matches the response. This message will be discarded." );
						return;
					}
					key = *it;
					ApplicationWorker::pending->application.set.erase( it );

					// Report result
					if ( success ) {
						__ERROR__( "ApplicationWorker", "dispatch", "The key: %.*s is SET successfully.", key.size, key.data );
					} else {
						__ERROR__( "ApplicationWorker", "dispatch", "The key: %.*s is NOT SET.", key.size, key.data );
					}
					
					key.free();

					break;
				case PROTO_OPCODE_GET:
					if ( success ) {
						this->protocol.parseKeyValueHeader( keyValueHeader );
						key.size = keyValueHeader.keySize;
						key.data = keyValueHeader.key;
						key.ptr = ( void * ) event.socket;
					} else {
						this->protocol.parseKeyHeader( keyHeader );
						key.size = keyHeader.keySize;
						key.data = keyHeader.key;
						key.ptr = ( void * ) event.socket;
					}

					it = ApplicationWorker::pending->masters.get.find( key );
					if ( it == ApplicationWorker::pending->masters.get.end() ) {
						__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending master GET request that matches the response. This message will be discarded. (key = %.*s)", key.size, key.data );
						return;
					}
					ApplicationWorker::pending->masters.get.erase( it );

					key.ptr = 0;
					it = ApplicationWorker::pending->application.get.lower_bound( key );
					if ( it == ApplicationWorker::pending->application.get.end() || ! key.equal( *it ) ) {
						__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending application GET request that matches the response. This message will be discarded." );
						return;
					}
					key = *it;
					ApplicationWorker::pending->application.get.erase( it );
					fd = ( int )( uint64_t ) key.ptr;

					if ( success ) {
						ret = ::write( fd, keyValueHeader.value, keyValueHeader.valueSize );
						__ERROR__( "ApplicationWorker", "dispatch", "The key: %.*s is GET successfully.", key.size, key.data );
						if ( ret == -1 ) {
							__ERROR__( "ApplicationWorker", "dispatch", "write(): %s.", strerror( errno ) );
						} else if ( ret < keyValueHeader.valueSize ) {
							__ERROR__( "ApplicationWorker", "dispatch", "The number of bytes written (%ld bytes) is not equal to the value size (%u bytes).", ret, keyValueHeader.valueSize );
						}
					} else {
						__ERROR__( "ApplicationWorker", "dispatch", "The key: %.*s does not exist.", key.size, key.data );
					}
					::close( fd );
					key.free();
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
	ApplicationEventQueue *eventQueue = ApplicationWorker::eventQueue;

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

bool ApplicationWorker::init() {
	Application *application = Application::getInstance();

	ApplicationWorker::eventQueue = &application->eventQueue;
	ApplicationWorker::pending = &application->pending;

	return true;
}

bool ApplicationWorker::init( ApplicationConfig &config, WorkerRole role ) {
	this->buffer.value = new char[ config.size.chunk ];
	this->buffer.valueSize = config.size.chunk;

	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			config.size.key,
			config.size.chunk
		)
	);
	this->role = role;
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
