#include <cerrno>
#include "worker.hh"
#include "../main/application.hh"
#include "../../common/util/debug.hh"

#define WORKER_COLOR	YELLOW

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

	Application *application = Application::getInstance();
	Pending &pending = application->pending;

	if ( event.type != CLIENT_EVENT_TYPE_PENDING )
		requestId = Application::getInstance()->idGenerator.nextVal( this->workerId );

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

				LOCK( &pending.application.setLock );
				pending.application.set.insert( key );
				UNLOCK( &pending.application.setLock );

				key.ptr = ( void * ) event.socket;

				LOCK( &pending.clients.setLock );
				pending.clients.set.insert( key );
				UNLOCK( &pending.clients.setLock );
				break;
			case CLIENT_EVENT_TYPE_GET_REQUEST:
				key.dup(
					event.message.get.keySize,
					event.message.get.key,
					( void * ) ( uint64_t ) event.message.set.fd
				);

				LOCK( &pending.application.getLock );
				pending.application.get.insert( key );
				UNLOCK( &pending.application.getLock );

				key.ptr = ( void * ) event.socket;

				LOCK( &pending.clients.getLock );
				pending.clients.get.insert( key );
				UNLOCK( &pending.clients.getLock );
				break;
			case CLIENT_EVENT_TYPE_UPDATE_REQUEST:
				keyValueUpdate.dup(
					event.message.update.keySize,
					event.message.update.key,
					( void * ) ( uint64_t ) event.message.update.fd
				);
				keyValueUpdate.offset = event.message.update.offset;
				keyValueUpdate.length = valueSize;

				LOCK( &pending.application.updateLock );
				pending.application.update.insert( keyValueUpdate );
				UNLOCK( &pending.application.updateLock );

				keyValueUpdate.ptr = ( void * ) event.socket;

				LOCK( &pending.clients.updateLock );
				pending.clients.update.insert( keyValueUpdate );
				UNLOCK( &pending.clients.updateLock );
				break;
			case CLIENT_EVENT_TYPE_DELETE_REQUEST:
				key.dup( event.message.del.keySize, event.message.del.key, 0 );

				LOCK( &pending.application.delLock );
				pending.application.del.insert( key );
				UNLOCK( &pending.application.delLock );

				key.ptr = ( void * ) event.socket;

				LOCK( &pending.clients.delLock );
				pending.clients.del.insert( key );
				UNLOCK( &pending.clients.delLock );
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

					LOCK( &pending.clients.setLock );
					it = pending.clients.set.find( key );
					if ( it == pending.clients.set.end() ) {
						__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending client SET request that matches the response. This message will be discarded." );
						goto quit_1;
					}
					pending.clients.set.erase( it );
					UNLOCK( &pending.clients.setLock );

					key.ptr = 0;

					LOCK( &pending.application.setLock );
					it = pending.application.set.lower_bound( key );
					if ( it == pending.application.set.end() || ! key.equal( *it ) ) {
						__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending application SET request that matches the response. This message will be discarded." );
						goto quit_1;
					}
					key = *it;
					pending.application.set.erase( it );
					UNLOCK( &pending.application.setLock );

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
						if ( this->protocol.parseKeyValueHeader( keyValueHeader, buffer.data, buffer.size ) ) {
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

					LOCK( &pending.clients.getLock );
					it = pending.clients.get.find( key );
					if ( it == pending.clients.get.end() ) {
						__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending client GET request that matches the response. This message will be discarded. (key = %.*s)", key.size, key.data );
						goto quit_1;
					}
					pending.clients.get.erase( it );
					UNLOCK( &pending.clients.getLock );

					key.ptr = 0;

					LOCK( &pending.application.getLock );
					it = pending.application.get.lower_bound( key );
					if ( it == pending.application.get.end() || ! key.equal( *it ) ) {
						__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending application GET request that matches the response. This message will be discarded." );
						goto quit_1;
					}
					key = *it;
					pending.application.get.erase( it );
					UNLOCK( &pending.application.getLock );

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

						LOCK( &pending.clients.updateLock );
						keyValueUpdateIt = pending.clients.update.find( keyValueUpdate );
						if ( keyValueUpdateIt == pending.clients.update.end() ) {
							__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending client UPDATE request that matches the response. This message will be discarded. (key = %.*s)", keyValueUpdate.size, keyValueUpdate.data );
							goto quit_1;
						}
						pending.clients.update.erase( keyValueUpdateIt );
						UNLOCK( &pending.clients.updateLock );

						keyValueUpdate.ptr = 0;

						LOCK( &pending.application.updateLock );
						keyValueUpdateIt = pending.application.update.lower_bound( keyValueUpdate );
						if ( keyValueUpdateIt == pending.application.update.end() || ! keyValueUpdate.equal( *keyValueUpdateIt ) ) {
							__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending application UPDATE request that matches the response. This message will be discarded. (key = %.*s, size = %u)", keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.size );
							goto quit_1;
						}
						keyValueUpdate = *keyValueUpdateIt;
						pending.application.update.erase( keyValueUpdateIt );
						UNLOCK( &pending.application.updateLock );

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

						LOCK( &pending.clients.delLock );
						it = pending.clients.del.find( key );
						if ( it == pending.clients.del.end() ) {
							__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending client DELETE request that matches the response. This message will be discarded. (key = %.*s)", key.size, key.data );
							goto quit_1;
						}
						pending.clients.del.erase( it );
						UNLOCK( &pending.clients.delLock );

						key.ptr = 0;

						LOCK( &pending.application.delLock );
						it = pending.application.del.lower_bound( key );
						if ( it == pending.application.del.end() || ! key.equal( *it ) ) {
							__ERROR__( "ApplicationWorker", "dispatch", "Cannot find a pending application DELETE request that matches the response. This message will be discarded." );
							goto quit_1;
						}
						key = *it;
						pending.application.del.erase( it );
						UNLOCK( &pending.application.delLock );

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
	ApplicationEventQueue *eventQueue = &Application::getInstance()->eventQueue;

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

bool ApplicationWorker::init( ApplicationConfig &config, uint32_t workerId ) {
	this->buffer.value = new char[ config.size.chunk ];
	this->buffer.valueSize = config.size.chunk;
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			config.size.key,
			config.size.chunk
		)
	);
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
