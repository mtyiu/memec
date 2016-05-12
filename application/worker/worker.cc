#include <cerrno>
#include "worker.hh"
#include "../main/application.hh"
#include "../../common/util/debug.hh"

#define WORKER_COLOR	YELLOW

IDGenerator *ApplicationWorker::idGenerator;
ApplicationEventQueue *ApplicationWorker::eventQueue;
Pending *ApplicationWorker::pending;

void ApplicationWorker::dispatch( MixedEvent event ) {
	switch( event.type ) {
		case EVENT_TYPE_CLIENT:
			this->dispatch( event.event.client );
			break;
		default:
			__ERROR__( "ApplicationWorker", "dispatch", "Unsupported event type." );
			break;
	}
}

void ApplicationWorker::dispatch( ClientEvent event ) {
	bool connected, isSend;
	uint16_t instanceId = Application::instanceId;
	uint32_t requestId;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;
	size_t valueSize;

	if ( event.type != CLIENT_EVENT_TYPE_PENDING )
		requestId = ApplicationWorker::idGenerator->nextVal( this->workerId );

	switch( event.type ) {
		case CLIENT_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterClient( buffer.size, requestId );
			isSend = true;
			break;
		case CLIENT_EVENT_TYPE_SET_REQUEST:
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
				instanceId, requestId,
				event.message.set.key,
				event.message.set.keySize,
				this->buffer.value,
				valueSize
			);
			isSend = true;
			break;
		case CLIENT_EVENT_TYPE_GET_REQUEST:
			buffer.data = this->protocol.reqGet(
				buffer.size,
				instanceId, requestId,
				event.message.get.key,
				event.message.get.keySize
			);
			isSend = true;
			break;
		case CLIENT_EVENT_TYPE_UPDATE_REQUEST:
			// Read contents from file
			ret = ::read( event.message.update.fd, this->buffer.value, this->buffer.valueSize );
			::close( event.message.update.fd );
			if ( ret == -1 ) {
				__ERROR__( "ApplicationWorker", "dispatch", "read(): %s.", strerror( errno ) );
				return;
			}
			valueSize = ( size_t ) ret;
			buffer.data = this->protocol.reqUpdate(
				buffer.size,
				instanceId, requestId,
				event.message.update.key,
				event.message.update.keySize,
				this->buffer.value,
				event.message.update.offset,
				valueSize
			);
			isSend = true;
			break;
		case CLIENT_EVENT_TYPE_DELETE_REQUEST:
			buffer.data = this->protocol.reqDelete(
				buffer.size,
				instanceId, requestId,
				event.message.del.key,
				event.message.del.keySize
			);
			isSend = true;
			break;
		case CLIENT_EVENT_TYPE_PENDING:
			isSend = false;
			break;
		default:
			return;
	}

	if ( isSend ) {
		Key key;
		KeyValueUpdate keyValueUpdate;
		switch( event.type ) {
			case CLIENT_EVENT_TYPE_SET_REQUEST:
				key.dup(
					event.message.set.keySize,
					event.message.set.key,
					( void * ) ( uint64_t ) event.message.set.fd
				);

				LOCK( &ApplicationWorker::pending->application.setLock );
				ApplicationWorker::pending->application.set.insert( key );
				UNLOCK( &ApplicationWorker::pending->application.setLock );

				key.ptr = ( void * ) event.socket;

				LOCK( &ApplicationWorker::pending->clients.setLock );
				ApplicationWorker::pending->clients.set.insert( key );
				UNLOCK( &ApplicationWorker::pending->clients.setLock );
				break;
			case CLIENT_EVENT_TYPE_GET_REQUEST:
				key.dup(
					event.message.get.keySize,
					event.message.get.key,
					( void * ) ( uint64_t ) event.message.set.fd
				);

				LOCK( &ApplicationWorker::pending->application.getLock );
				ApplicationWorker::pending->application.get.insert( key );
				UNLOCK( &ApplicationWorker::pending->application.getLock );

				key.ptr = ( void * ) event.socket;

				LOCK( &ApplicationWorker::pending->clients.getLock );
				ApplicationWorker::pending->clients.get.insert( key );
				UNLOCK( &ApplicationWorker::pending->clients.getLock );
				break;
			case CLIENT_EVENT_TYPE_UPDATE_REQUEST:
				keyValueUpdate.dup(
					event.message.update.keySize,
					event.message.update.key,
					( void * ) ( uint64_t ) event.message.update.fd
				);
				keyValueUpdate.offset = event.message.update.offset;
				keyValueUpdate.length = valueSize;

				LOCK( &ApplicationWorker::pending->application.updateLock );
				ApplicationWorker::pending->application.update.insert( keyValueUpdate );
				UNLOCK( &ApplicationWorker::pending->application.updateLock );

				keyValueUpdate.ptr = ( void * ) event.socket;

				LOCK( &ApplicationWorker::pending->clients.updateLock );
				ApplicationWorker::pending->clients.update.insert( keyValueUpdate );
				UNLOCK( &ApplicationWorker::pending->clients.updateLock );
				break;
			case CLIENT_EVENT_TYPE_DELETE_REQUEST:
				key.dup( event.message.del.keySize, event.message.del.key, 0 );

				LOCK( &ApplicationWorker::pending->application.delLock );
				ApplicationWorker::pending->application.del.insert( key );
				UNLOCK( &ApplicationWorker::pending->application.delLock );

				key.ptr = ( void * ) event.socket;

				LOCK( &ApplicationWorker::pending->clients.delLock );
				ApplicationWorker::pending->clients.del.insert( key );
				UNLOCK( &ApplicationWorker::pending->clients.delLock );
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
		struct KeyValueUpdateHeader keyValueUpdateHeader;
		Key key;
		KeyValueUpdate keyValueUpdate;
		std::set<Key>::iterator it;
		std::set<KeyValueUpdate>::iterator keyValueUpdateIt;
		int fd;

		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "ApplicationWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			if ( header.from != PROTO_MAGIC_FROM_CLIENT ) {
				__ERROR__( "ApplicationWorker", "dispatch", "Invalid message source from client." );
				break;
			}
			bool success;
			success = header.magic == PROTO_MAGIC_RESPONSE_SUCCESS;
			if ( ! success && header.magic != PROTO_MAGIC_RESPONSE_FAILURE ) {
				__ERROR__( "ApplicationWorker", "dispatch", "Invalid magic code from client." );
				break;
			}

			switch( header.opcode ) {
				case PROTO_OPCODE_REGISTER:
					if ( success ) {
						event.socket->registered = true;
						Application::instanceId = header.instanceId;
					} else {
						__ERROR__( "ApplicationWorker", "dispatch", "Failed to register with client." );
					}
					break;
				case PROTO_OPCODE_SET:
					this->protocol.parseKeyHeader( keyHeader, buffer.data, buffer.size );
					key.size = keyHeader.keySize;
					key.data = keyHeader.key;
					key.ptr = ( void * ) event.socket;

					LOCK( &ApplicationWorker::pending->clients.setLock );
					it = ApplicationWorker::pending->clients.set.find( key );
					if ( it == ApplicationWorker::pending->clients.set.end() ) {
						__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending client SET request that matches the response. This message will be discarded." );
						goto quit_1;
					}
					ApplicationWorker::pending->clients.set.erase( it );
					UNLOCK( &ApplicationWorker::pending->clients.setLock );

					key.ptr = 0;

					LOCK( &ApplicationWorker::pending->application.setLock );
					it = ApplicationWorker::pending->application.set.lower_bound( key );
					if ( it == ApplicationWorker::pending->application.set.end() || ! key.equal( *it ) ) {
						__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending application SET request that matches the response. This message will be discarded." );
						goto quit_1;
					}
					key = *it;
					ApplicationWorker::pending->application.set.erase( it );
					UNLOCK( &ApplicationWorker::pending->application.setLock );

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
						if ( this->protocol.parseKeyValueHeader( keyValueHeader, buffer.data, buffer.size, 0, false ) ) {
							key.size = keyValueHeader.keySize;
							key.data = keyValueHeader.key;
							key.ptr = ( void * ) event.socket;
						} else {
							__ERROR__( "ApplicationWorker", "dispatch", "Invalid key value header for GET." );
							goto quit_1;
						}
					} else {
						if ( this->protocol.parseKeyHeader( keyHeader, buffer.data, buffer.size ) ) {
							key.size = keyHeader.keySize;
							key.data = keyHeader.key;
							key.ptr = ( void * ) event.socket;
						} else {
							__ERROR__( "ApplicationWorker", "dispatch", "Invalid key header for GET." );
							goto quit_1;
						}
					}

					LOCK( &ApplicationWorker::pending->clients.getLock );
					it = ApplicationWorker::pending->clients.get.find( key );
					if ( it == ApplicationWorker::pending->clients.get.end() ) {
						__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending client GET request that matches the response. This message will be discarded. (key = %.*s)", key.size, key.data );
						goto quit_1;
					}
					ApplicationWorker::pending->clients.get.erase( it );
					UNLOCK( &ApplicationWorker::pending->clients.getLock );

					key.ptr = 0;

					LOCK( &ApplicationWorker::pending->application.getLock );
					it = ApplicationWorker::pending->application.get.lower_bound( key );
					if ( it == ApplicationWorker::pending->application.get.end() || ! key.equal( *it ) ) {
						__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending application GET request that matches the response. This message will be discarded." );
						goto quit_1;
					}
					key = *it;
					ApplicationWorker::pending->application.get.erase( it );
					UNLOCK( &ApplicationWorker::pending->application.getLock );

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
				case PROTO_OPCODE_UPDATE:
					if ( this->protocol.parseKeyValueUpdateHeader( keyValueUpdateHeader, false, buffer.data, buffer.size ) ) {
						keyValueUpdate.size = keyValueUpdateHeader.keySize;
						keyValueUpdate.data = keyValueUpdateHeader.key;
						keyValueUpdate.offset = keyValueUpdateHeader.valueUpdateOffset;
						keyValueUpdate.length = keyValueUpdateHeader.valueUpdateSize;
						keyValueUpdate.ptr = ( void * ) event.socket;

						LOCK( &ApplicationWorker::pending->clients.updateLock );
						keyValueUpdateIt = ApplicationWorker::pending->clients.update.find( keyValueUpdate );
						if ( keyValueUpdateIt == ApplicationWorker::pending->clients.update.end() ) {
							__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending client UPDATE request that matches the response. This message will be discarded. (key = %.*s)", keyValueUpdate.size, keyValueUpdate.data );
							goto quit_1;
						}
						ApplicationWorker::pending->clients.update.erase( keyValueUpdateIt );
						UNLOCK( &ApplicationWorker::pending->clients.updateLock );

						keyValueUpdate.ptr = 0;

						LOCK( &ApplicationWorker::pending->application.updateLock );
						keyValueUpdateIt = ApplicationWorker::pending->application.update.lower_bound( keyValueUpdate );
						if ( keyValueUpdateIt == ApplicationWorker::pending->application.update.end() || ! keyValueUpdate.equal( *keyValueUpdateIt ) ) {
							__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending application UPDATE request that matches the response. This message will be discarded. (key = %.*s, size = %u)", keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.size );
							goto quit_1;
						}
						keyValueUpdate = *keyValueUpdateIt;
						ApplicationWorker::pending->application.update.erase( keyValueUpdateIt );
						UNLOCK( &ApplicationWorker::pending->application.updateLock );

						if ( success ) {
							__ERROR__( "ApplicationWorker", "dispatch", "The key: %.*s is UPDATE successfully.", keyValueUpdate.size, keyValueUpdate.data );
						} else {
							__ERROR__( "ApplicationWorker", "dispatch", "The key: %.*s does not exist.", keyValueUpdate.size, keyValueUpdate.data );
						}
						keyValueUpdate.free();
					} else {
						__ERROR__( "ApplicationWorker", "dispatch", "Invalid key value update header for UPDATE." );
					}
					break;
				case PROTO_OPCODE_DELETE:
					if ( this->protocol.parseKeyHeader( keyHeader, buffer.data, buffer.size ) ) {
						key.size = keyHeader.keySize;
						key.data = keyHeader.key;
						key.ptr = ( void * ) event.socket;

						LOCK( &ApplicationWorker::pending->clients.delLock );
						it = ApplicationWorker::pending->clients.del.find( key );
						if ( it == ApplicationWorker::pending->clients.del.end() ) {
							__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending client DELETE request that matches the response. This message will be discarded. (key = %.*s)", key.size, key.data );
							goto quit_1;
						}
						ApplicationWorker::pending->clients.del.erase( it );
						UNLOCK( &ApplicationWorker::pending->clients.delLock );

						key.ptr = 0;

						LOCK( &ApplicationWorker::pending->application.delLock );
						it = ApplicationWorker::pending->application.del.lower_bound( key );
						if ( it == ApplicationWorker::pending->application.del.end() || ! key.equal( *it ) ) {
							__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending application DELETE request that matches the response. This message will be discarded." );
							goto quit_1;
						}
						key = *it;
						ApplicationWorker::pending->application.del.erase( it );
						UNLOCK( &ApplicationWorker::pending->application.delLock );

						if ( success ) {
							__ERROR__( "ApplicationWorker", "dispatch", "The key: %.*s is DELETE successfully.", key.size, key.data );
						} else {
							__ERROR__( "ApplicationWorker", "dispatch", "The key: %.*s does not exist.", key.size, key.data );
						}
						key.free();
					} else {
						__ERROR__( "ApplicationWorker", "dispatch", "Invalid key header for DELETE." );
					}
					break;
				default:
					__ERROR__( "ApplicationWorker", "dispatch", "Invalid opcode from client." );
					goto quit_1;
			}
quit_1:
			buffer.data += header.length;
			buffer.size -= header.length;
		}
		if ( connected ) event.socket->done();
	}
	if ( ! connected )
		__ERROR__( "ApplicationWorker", "dispatch", "The client is disconnected." );
}


void ApplicationWorker::free() {
	this->protocol.free();
	delete[] this->buffer.value;
}

void *ApplicationWorker::run( void *argv ) {
	ApplicationWorker *worker = ( ApplicationWorker * ) argv;
	ApplicationEventQueue *eventQueue = ApplicationWorker::eventQueue;

	MixedEvent event;
	bool ret;
	while( worker->getIsRunning() | ( ret = eventQueue->mixed->extract( event ) ) ) {
		if ( ret )
			worker->dispatch( event );
	}

	worker->free();
	pthread_exit( 0 );
	return 0;
}

bool ApplicationWorker::init() {
	Application *application = Application::getInstance();

	ApplicationWorker::idGenerator = &application->idGenerator;
	ApplicationWorker::eventQueue = &application->eventQueue;
	ApplicationWorker::pending = &application->pending;

	return true;
}

bool ApplicationWorker::init( ApplicationConfig &config, uint32_t workerId ) {
	uint32_t bufferSize = Protocol::getSuggestedBufferSize( config.size.key, config.size.chunk, true );
	this->buffer.value = new char[ bufferSize ];
	this->buffer.valueSize = bufferSize;
	this->protocol.init( bufferSize );
	this->workerId = workerId;
	return true;
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
	fprintf( f, "Worker #%u (Thread ID = %lu): %srunning\n", this->workerId, this->tid, this->isRunning ? "" : "not " );
}
