#include "worker.hh"
#include "../main/slave.hh"
#include "../../common/util/debug.hh"
#include "../../common/util/time.hh"

#define WORKER_COLOR	YELLOW

Coding *SlaveWorker::coding;
SlaveEventQueue *SlaveWorker::eventQueue;
StripeList<SlavePeerSocket> *SlaveWorker::stripeList;
Map *SlaveWorker::map;
MemoryPool<Chunk> *SlaveWorker::chunkPool;
MemoryPool<Stripe> *SlaveWorker::stripePool;
std::vector<MixedChunkBuffer *> *SlaveWorker::chunkBuffer;

void SlaveWorker::dispatch( MixedEvent event ) {
	switch( event.type ) {
		case EVENT_TYPE_CODING:
			this->dispatch( event.event.coding );
			break;
		case EVENT_TYPE_COORDINATOR:
			this->dispatch( event.event.coordinator );
			break;
		case EVENT_TYPE_IO:
			this->dispatch( event.event.io );
			break;
		case EVENT_TYPE_MASTER:
			this->dispatch( event.event.master );
			break;
		case EVENT_TYPE_SLAVE:
			this->dispatch( event.event.slave );
			break;
		case EVENT_TYPE_SLAVE_PEER:
			this->dispatch( event.event.slavePeer );
			break;
		default:
			break;
	}
}

void SlaveWorker::dispatch( CodingEvent event ) {
	Chunk **dataChunks, *parityChunk;
	uint32_t parityChunkId;

	switch( event.type ) {
		case CODING_EVENT_TYPE_ENCODE:
			__ERROR__( "SlaveWorker", "dispatch", "Received an CODING_EVENT_TYPE_ENCODE event." );
			parityChunkId = event.message.stripe->get( dataChunks, parityChunk );
			SlaveWorker::coding->encode( dataChunks, parityChunk, parityChunkId );

			// Release memory for data chunks
			for ( uint32_t i = 0; i < Stripe::dataChunkCount; i++ ) {
				SlaveWorker::chunkPool->free( dataChunks[ i ] );
			}

			// Release memory for stripe
			SlaveWorker::stripePool->free( event.message.stripe );

			// Append a flush event to the event queue
			IOEvent ioEvent;
			ioEvent.flush( parityChunk );
			SlaveWorker::eventQueue->insert( ioEvent );			
			break;
		default:
			return;
	}
}

void SlaveWorker::dispatch( CoordinatorEvent event ) {
	bool connected, isSend;
	ssize_t ret;
	size_t count;
	struct {
		size_t size;
		char *data;
	} buffer;
	struct timespec t = start_timer();

	switch( event.type ) {
		case COORDINATOR_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterCoordinator( buffer.size );
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_SYNC:
			buffer.data = this->protocol.sendHeartbeat(
				buffer.size,
				Slave::getInstance()->aggregateLoad().ops,
				SlaveWorker::map->ops,
				count
			);
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
			__ERROR__( "SlaveWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );

		if ( ret > 0 ) {
			this->load.sentBytes += ret;
			this->load.elapsedTime += get_elapsed_time( t );
		}

		if ( event.type == COORDINATOR_EVENT_TYPE_SYNC && SlaveWorker::map->ops.size() ) {
			// Some metadata is not sent yet, continue to send
			SlaveWorker::eventQueue->insert( event );
		}
	} else {
		ProtocolHeader header;

		ret = event.socket->recv( this->protocol.buffer.recv, PROTO_HEADER_SIZE, connected, true );

		if ( ret == PROTO_HEADER_SIZE && connected ) {
			this->load.recvBytes += ret;
			this->load.elapsedTime += get_elapsed_time( t );

			this->protocol.parseHeader( header, this->protocol.buffer.recv, ret );
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_COORDINATOR ) {
				__ERROR__( "SlaveWorker", "dispatch", "Invalid message source from coordinator." );
				return;
			}
			switch( header.opcode ) {
				case PROTO_OPCODE_REGISTER:
					switch( header.magic ) {
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							event.socket->registered = true;
							break;
						case PROTO_MAGIC_RESPONSE_FAILURE:
							__ERROR__( "SlaveWorker", "dispatch", "Failed to register with coordinator." );
							break;
						default:
							__ERROR__( "SlaveWorker", "dispatch", "Invalid magic code from coordinator." );
							break;
					}
					break;
				default:
					__ERROR__( "SlaveWorker", "dispatch", "Invalid opcode from coordinator." );
					return;
			}
		}
	}
	if ( ! connected )
		__ERROR__( "SlaveWorker", "dispatch", "The coordinator is disconnected." );
}

void SlaveWorker::dispatch( IOEvent event ) {
	__ERROR__( "SlaveWorker", "dispatch", "Received an I/O event." );
	switch( event.type ) {
		case IO_EVENT_TYPE_FLUSH_CHUNK:
			this->storage->write(
				event.message.chunk,
				true
			);
			break;
	}
}

void SlaveWorker::dispatch( MasterEvent event ) {
	bool success = true, connected, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;
	struct timespec t = start_timer();

	switch( event.type ) {
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_GET_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS:
			success = true;
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_GET_RESPONSE_FAILURE:
		case MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE:
			success = false;
			isSend = true;
			break;
		default:
			isSend = false;
			break;
	}

	switch( event.type ) {
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRegisterMaster( buffer.size, success );
			break;
		case MASTER_EVENT_TYPE_GET_RESPONSE_SUCCESS:
		{
			char *key, *value;
			uint8_t keySize;
			uint32_t valueSize;

			__ERROR__( "MasterWorker", "dispatch", "MASTER_EVENT_TYPE_GET_RESPONSE_SUCCESS: key size = %u, value size = %u.", keySize, valueSize );

			event.message.keyValue.deserialize( key, keySize, value, valueSize );
			buffer.data = this->protocol.resGet( buffer.size, success, keySize, key, valueSize, value );
		}
			break;
		case MASTER_EVENT_TYPE_GET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resGet(
				buffer.size,
				success,
				event.message.key.size,
				event.message.key.data
			);
			break;
		case MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS:
		case MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resSet(
				buffer.size,
				success,
				event.message.key.size,
				event.message.key.data
			);
			break;
		case MASTER_EVENT_TYPE_PENDING:
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "SlaveWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );

		if ( ret > 0 ) {
			this->load.sentBytes += ret;
			this->load.elapsedTime += get_elapsed_time( t );
		}
	} else {
		// Parse requests from masters
		ProtocolHeader header;
		ret = event.socket->recv(
			this->protocol.buffer.recv,
			this->protocol.buffer.size,
			connected,
			false
		);
		if ( ret > 0 ) {
			this->load.recvBytes += ret;
			this->load.elapsedTime += get_elapsed_time( t );
		}
		if ( ! this->protocol.parseHeader( header ) ) {
			__ERROR__( "SlaveWorker", "dispatch", "Undefined message." );
		} else {
			if (
				header.magic != PROTO_MAGIC_REQUEST ||
				header.from != PROTO_MAGIC_FROM_MASTER ||
				header.to != PROTO_MAGIC_TO_SLAVE
			) {
				__ERROR__( "SlaveWorker", "dispatch", "Invalid protocol header." );
			}

			struct KeyHeader keyHeader;
			struct KeyValueHeader keyValueHeader;
			struct KeyValueUpdateHeader keyValueUpdateHeader;
			uint32_t listIndex, dataIndex;
			bool isParity;

			switch( header.opcode ) {
				///////////////////////////////////////////////////////////////
				case PROTO_OPCODE_GET:
					if ( this->protocol.parseKeyHeader( keyHeader ) ) {
						__DEBUG__(
							BLUE, "SlaveWorker", "dispatch",
							"[GET] Key: %.*s (key size = %u).",
							( int ) keyHeader.keySize,
							keyHeader.key,
							keyHeader.keySize
						);

						// Find the key in map
						std::map<Key, KeyMetadata>::iterator keysIt;
						Key key;

						key.size = keyHeader.keySize;
						key.data = keyHeader.key;
						keysIt = map->keys.find( key );
						if ( keysIt != map->keys.end() ) {
							std::map<Metadata, Chunk *>::iterator cacheIt;
							cacheIt = map->cache.find( keysIt->second );

							// TODO
							// event.resGet( event.socket, it->second );
						} else {
							event.resGet( event.socket, key );
						}

						this->load.get();

						// Send the response immediately
						this->dispatch( event );
					}
					break;
				///////////////////////////////////////////////////////////////
				case PROTO_OPCODE_SET:
					if ( this->protocol.parseKeyValueHeader( keyValueHeader ) ) {
						__DEBUG__(
							BLUE, "SlaveWorker", "dispatch",
							"[SET] Key: %.*s (key size = %u); Value: (value size = %u)",
							( int ) keyValueHeader.keySize,
							keyValueHeader.key,
							keyValueHeader.keySize,
							keyValueHeader.valueSize
						);

						listIndex = SlaveWorker::stripeList->get(
							keyValueHeader.key,
							( size_t ) keyValueHeader.keySize,
							0, 0, &dataIndex
						);

						KeyMetadata keyMetadata = SlaveWorker::chunkBuffer
							->at( listIndex )
							->set(
								keyValueHeader.key,
								keyValueHeader.keySize,
								keyValueHeader.value,
								keyValueHeader.valueSize,
								isParity,
								dataIndex
							);
						Key key;
						key.size = keyValueHeader.keySize;
						key.data = keyValueHeader.key;

						if ( ! isParity ) {
							OpMetadata opMetadata;
							opMetadata.clone( keyMetadata );
							opMetadata.opcode = PROTO_OPCODE_SET;
							key.dup( key.size, key.data );

							// Update mappings
							map->keys[ key ] = keyMetadata;
							map->ops[ key ] = opMetadata;
						}

						event.resSet( event.socket, key );

						this->load.set();

						// Send the response immediately
						this->dispatch( event );
					}
					break;
				///////////////////////////////////////////////////////////////
				case PROTO_OPCODE_UPDATE:
					break;
				///////////////////////////////////////////////////////////////
				case PROTO_OPCODE_UPDATE_DELTA:
					break;
				///////////////////////////////////////////////////////////////
				case PROTO_OPCODE_DELETE:
					break;
				///////////////////////////////////////////////////////////////
				case PROTO_OPCODE_DELETE_DELTA:
					break;
				///////////////////////////////////////////////////////////////
				default:
					__ERROR__( "SlaveWorker", "dispatch", "Invalid opcode from master." );
					return;
			}
		}
	}

	if ( ! connected )
		__ERROR__( "SlaveWorker", "dispatch", "The master is disconnected." );
}

void SlaveWorker::dispatch( SlaveEvent event ) {
}

void SlaveWorker::dispatch( SlavePeerEvent event ) {
	bool connected, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	switch( event.type ) {
		case SLAVE_PEER_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterSlavePeer( buffer.size );
			isSend = true;
			break;
		case SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resRegisterSlavePeer( buffer.size, true );
			isSend = true;
			break;
		case SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRegisterSlavePeer( buffer.size, false );
			isSend = true;
			break;
		case SLAVE_PEER_EVENT_TYPE_PENDING:
			isSend = false;
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "SlaveWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else {

	}
	if ( ! connected )
		__ERROR__( "SlaveWorker", "dispatch", "The slave is disconnected." );
}

void SlaveWorker::free() {
	if ( this->storage ) {
		this->storage->stop();
		Storage::destroy( this->storage );
	}
	this->protocol.free();
}

void *SlaveWorker::run( void *argv ) {
	SlaveWorker *worker = ( SlaveWorker * ) argv;
	WorkerRole role = worker->getRole();
	SlaveEventQueue *eventQueue = SlaveWorker::eventQueue;

#define SLAVE_WORKER_EVENT_LOOP(_EVENT_TYPE_, _EVENT_QUEUE_) \
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
			SLAVE_WORKER_EVENT_LOOP(
				MixedEvent,
				eventQueue->mixed
			);
			break;
		case WORKER_ROLE_CODING:
			SLAVE_WORKER_EVENT_LOOP(
				CodingEvent,
				eventQueue->separated.coding
			);
			break;
		case WORKER_ROLE_COORDINATOR:
			SLAVE_WORKER_EVENT_LOOP(
				CoordinatorEvent,
				eventQueue->separated.coordinator
			);
			break;
		case WORKER_ROLE_IO:
			SLAVE_WORKER_EVENT_LOOP(
				IOEvent,
				eventQueue->separated.io
			);
			break;
		case WORKER_ROLE_MASTER:
			SLAVE_WORKER_EVENT_LOOP(
				MasterEvent,
				eventQueue->separated.master
			);
			break;
		case WORKER_ROLE_SLAVE:
			SLAVE_WORKER_EVENT_LOOP(
				SlaveEvent,
				eventQueue->separated.slave
			);
			break;
		case WORKER_ROLE_SLAVE_PEER:
			SLAVE_WORKER_EVENT_LOOP(
				SlavePeerEvent,
				eventQueue->separated.slavePeer
			);
			break;
		default:
			break;
	}

	worker->free();
	pthread_exit( 0 );
	return 0;
}

bool SlaveWorker::init() {
	Slave *slave = Slave::getInstance();

	SlaveWorker::coding = slave->coding;
	SlaveWorker::eventQueue = &slave->eventQueue;
	SlaveWorker::stripeList = slave->stripeList;
	SlaveWorker::map = &slave->map;
	SlaveWorker::chunkPool = slave->chunkPool;
	SlaveWorker::stripePool = slave->stripePool;
	SlaveWorker::chunkBuffer = &slave->chunkBuffer;
	return true;
}

bool SlaveWorker::init( GlobalConfig &globalConfig, SlaveConfig &slaveConfig, WorkerRole role ) {
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			globalConfig.size.key,
			globalConfig.size.chunk
		)
	);
	this->role = role;
	switch( this->role ) {
		case WORKER_ROLE_MIXED:
		case WORKER_ROLE_IO:
			this->storage = Storage::instantiate( slaveConfig );
			this->storage->start();
			break;
		default:
			this->storage = 0;
			break;
	}
	return role != WORKER_ROLE_UNDEFINED;
}

bool SlaveWorker::start() {
	this->isRunning = true;
	if ( pthread_create( &this->tid, NULL, SlaveWorker::run, ( void * ) this ) != 0 ) {
		__ERROR__( "SlaveWorker", "start", "Cannot start worker thread." );
		return false;
	}
	return true;
}

void SlaveWorker::stop() {
	this->isRunning = false;
}

void SlaveWorker::print( FILE *f ) {
	char role[ 16 ];
	switch( this->role ) {
		case WORKER_ROLE_MIXED:
			strcpy( role, "Mixed" );
			break;
		case WORKER_ROLE_CODING:
			strcpy( role, "Coding" );
			break;
		case WORKER_ROLE_COORDINATOR:
			strcpy( role, "Coordinator" );
			break;
		case WORKER_ROLE_IO:
			strcpy( role, "I/O" );
			break;
		case WORKER_ROLE_MASTER:
			strcpy( role, "Master" );
			break;
		case WORKER_ROLE_SLAVE:
			strcpy( role, "Slave" );
			break;
		case WORKER_ROLE_SLAVE_PEER:
			strcpy( role, "Slave peer" );
			break;
		default:
			return;
	}
	fprintf( f, "%11s worker (Thread ID = %lu): %srunning\n", role, this->tid, this->isRunning ? "" : "not " );
}
