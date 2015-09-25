#include "worker.hh"
#include "../main/coordinator.hh"
#include "../../common/util/debug.hh"
#include "../../common/ds/sockaddr_in.hh"

#define WORKER_COLOR	YELLOW

IDGenerator *CoordinatorWorker::idGenerator;
CoordinatorEventQueue *CoordinatorWorker::eventQueue;

void CoordinatorWorker::dispatch( MixedEvent event ) {
	switch( event.type ) {
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
			return;
	}
}

void CoordinatorWorker::dispatch( CoordinatorEvent event ) {
}

void CoordinatorWorker::dispatch( MasterEvent event ) {
	bool connected = false, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;
	Coordinator *coordinator = Coordinator::getInstance();

	switch( event.type ) {
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resRegisterMaster( buffer.size, event.id, true );
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRegisterMaster( buffer.size, event.id, false );
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_PUSH_LOADING_STATS:
			buffer.data = this->protocol.reqPushLoadStats ( 
				buffer.size, 0, // id 
				event.message.slaveLoading.slaveGetLatency, 
				event.message.slaveLoading.slaveSetLatency,
				event.message.slaveLoading.overloadedSlaveSet
			);
			// release the ArrayMaps
			delete event.message.slaveLoading.slaveGetLatency;
			delete event.message.slaveLoading.slaveSetLatency;
			delete event.message.slaveLoading.overloadedSlaveSet;
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_SWITCH_PHASE:
			// just trigger / stop the remap phase, no message need to be handled
			if ( event.message.remap.toRemap ) {
				coordinator->remapMsgHandler.startRemap();
			} else {
				coordinator->remapMsgHandler.stopRemap();
			}
			isSend = false;
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
			__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else if ( event.type == MASTER_EVENT_TYPE_SWITCH_PHASE ) {
		// just to avoid error message
		connected = true;
	} else {
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();

		struct LoadStatsHeader loadStatsHeader;
		ArrayMap< struct sockaddr_in, Latency > getLatency;
		ArrayMap< struct sockaddr_in, Latency > setLatency;
		Coordinator *coordinator = Coordinator::getInstance();
		ArrayMap< struct sockaddr_in, Latency> *latencyPool = NULL;
		struct sockaddr_in masterAddr;

		while( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "CoordinatorWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;

			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_MASTER ) {
				__ERROR__( "CoordinatorWorker", "dispatch", "Invalid message source from master." );
			}

			int index = 0;

			switch ( header.magic ) {
				case PROTO_MAGIC_LOADING_STATS:
					this->protocol.parseLoadStatsHeader( loadStatsHeader, buffer.data, buffer.size );
					buffer.data += PROTO_LOAD_STATS_SIZE;
					buffer.size -= PROTO_LOAD_STATS_SIZE;
					if ( ! this->protocol.parseLoadingStats( loadStatsHeader, getLatency, setLatency, buffer.data, buffer.size ) )
						__ERROR__( "CoordinatorWorker", "dispatch", "Invalid amount of data received from master." );
					//fprintf( stderr, "get stats GET %d SET %d\n", loadStatsHeader.slaveGetCount, loadStatsHeader.slaveSetCount );
					// set the latest loading stats
					//fprintf( stderr, "fd %d IP %u:%hu\n", event.socket->getSocket(), ntohl( event.socket->getAddr().sin_addr.s_addr ), ntohs( event.socket->getAddr().sin_port ) );

#define SET_SLAVE_LATENCY_FOR_MASTER( _MASTER_ADDR_, _SRC_, _DST_ ) \
	for ( uint32_t i = 0; i < _SRC_.size(); i++ ) { \
		coordinator->slaveLoading._DST_.get( _SRC_.keys[ i ], &index ); \
		if ( index == -1 ) { \
			coordinator->slaveLoading._DST_.set( _SRC_.keys[ i ], new ArrayMap<struct sockaddr_in, Latency> () ); \
			index = coordinator->slaveLoading._DST_.size() - 1; \
			coordinator->slaveLoading._DST_.values[ index ]->set( _MASTER_ADDR_, _SRC_.values[ i ] ); \
		} else { \
			latencyPool = coordinator->slaveLoading._DST_.values[ index ]; \
			latencyPool->get( _MASTER_ADDR_, &index ); \
			if ( index == -1 ) { \
				latencyPool->set( _MASTER_ADDR_, _SRC_.values[ i ] ); \
			} else { \
				delete latencyPool->values[ index ]; \
				latencyPool->values[ index ] = _SRC_.values[ i ]; \
			} \
		} \
	} \

					masterAddr = event.socket->getAddr();
					pthread_mutex_lock ( &coordinator->slaveLoading.lock );
					SET_SLAVE_LATENCY_FOR_MASTER( masterAddr, getLatency, latestGet );
					SET_SLAVE_LATENCY_FOR_MASTER( masterAddr, setLatency, latestSet );
					pthread_mutex_unlock ( &coordinator->slaveLoading.lock ); 

					buffer.data -= PROTO_LOAD_STATS_SIZE;
					buffer.size += PROTO_LOAD_STATS_SIZE;
					break;
				case PROTO_MAGIC_REQUEST:
					goto quit_1;
				default:
					__ERROR__( "CoordinatorWorker", "dispatch", "Invalid magic code from master." );
					goto quit_1;
			}

#undef SET_SLAVE_LATENCY_FOR_MASTER
quit_1:
			buffer.data += header.length;
			buffer.size -= header.length;
		}

		if ( connected ) event.socket->done();

	}

	if ( ! connected )
		__ERROR__( "CoordinatorWorker", "dispatch", "The master is disconnected." );
}

void CoordinatorWorker::dispatch( SlaveEvent event ) {
	bool connected, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	switch( event.type ) {
		case SLAVE_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resRegisterSlave( buffer.size, event.id, true );
			isSend = true;
			break;
		case SLAVE_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRegisterSlave( buffer.size, event.id, false );
			isSend = true;
			break;
		case SLAVE_EVENT_TYPE_PENDING:
			isSend = false;
			break;
		case SLAVE_EVENT_TYPE_ANNOUNCE_SLAVE_CONNECTED:
			isSend = false;
			break;
		default:
			return;
	}

	if ( event.type == SLAVE_EVENT_TYPE_ANNOUNCE_SLAVE_CONNECTED ) {
		ArrayMap<int, SlaveSocket> &slaves = Coordinator::getInstance()->sockets.slaves;
		uint32_t requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );

		buffer.data = this->protocol.announceSlaveConnected( buffer.size, requestId, event.socket );

		connected = true;

		pthread_mutex_lock( &slaves.lock );
		for ( uint32_t i = 0; i < slaves.size(); i++ ) {
			SlaveSocket *slave = slaves.values[ i ];
			if ( event.socket->equal( slave ) )
				continue; // No need to tell the new socket

			ret = slave->send( buffer.data, buffer.size, connected );
			if ( ret != ( ssize_t ) buffer.size )
				__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
		}
		pthread_mutex_unlock( &slaves.lock );
	} else if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else {
		// Parse requests from slaves
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		while( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "CoordinatorWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_SLAVE ) {
				__ERROR__( "CoordinatorWorker", "dispatch", "Invalid message source from slave." );
				goto quit_1;
			}

			if ( header.opcode == PROTO_OPCODE_SYNC ) {
				if ( header.magic != PROTO_MAGIC_HEARTBEAT ) {
					__ERROR__( "CoordinatorWorker", "dispatch", "Invalid magic code from slave." );
					goto quit_1;
				}

				size_t bytes, offset, count = 0;
				struct HeartbeatHeader heartbeatHeader;
				struct SlaveSyncHeader slaveSyncHeader;

				if ( this->protocol.parseHeartbeatHeader( heartbeatHeader, buffer.data, buffer.size ) ) {
					event.socket->load.ops.get = heartbeatHeader.get;
					event.socket->load.ops.set = heartbeatHeader.set;
					event.socket->load.ops.update = heartbeatHeader.update;
					event.socket->load.ops.del = heartbeatHeader.del;

					offset = PROTO_HEADER_SIZE + PROTO_HEARTBEAT_SIZE;
					while ( offset < ( size_t ) ret ) {
						if ( ! this->protocol.parseSlaveSyncHeader( slaveSyncHeader, bytes, buffer.data, buffer.size, offset ) )
							break;
						offset += bytes;
						count++;

						// Update metadata map
						Key key;
						key.set( slaveSyncHeader.keySize, slaveSyncHeader.key, 0 );

						OpMetadata opMetadata;
						opMetadata.opcode = slaveSyncHeader.opcode;
						opMetadata.listId = slaveSyncHeader.listId;
						opMetadata.stripeId = slaveSyncHeader.stripeId;
						opMetadata.chunkId = slaveSyncHeader.chunkId;

						if ( event.socket->keys.find( key ) == event.socket->keys.end() )
							key.dup();
						event.socket->keys[ key ] = opMetadata;
					}
				} else {
					__ERROR__( "CoordinatorWorker", "dispatch", "Invalid heartbeat protocol header." );
				}
			} else {
				__ERROR__( "CoordinatorWorker", "dispatch", "Invalid opcode from slave." );
			}
quit_1:
			buffer.data += header.length;
			buffer.size -= header.length;
		}
		if ( connected ) event.socket->done();
	}

	if ( ! connected )
		__ERROR__( "CoordinatorWorker", "dispatch", "The slave is disconnected." );
}

void CoordinatorWorker::free() {
	this->protocol.free();
}

void *CoordinatorWorker::run( void *argv ) {
	CoordinatorWorker *worker = ( CoordinatorWorker * ) argv;
	WorkerRole role = worker->getRole();
	CoordinatorEventQueue *eventQueue = CoordinatorWorker::eventQueue;

#define COORDINATOR_WORKER_EVENT_LOOP(_EVENT_TYPE_, _EVENT_QUEUE_) \
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
			COORDINATOR_WORKER_EVENT_LOOP(
				MixedEvent,
				eventQueue->mixed
			);
			break;
		case WORKER_ROLE_COORDINATOR:
			COORDINATOR_WORKER_EVENT_LOOP(
				CoordinatorEvent,
				eventQueue->separated.coordinator
			);
			break;
		case WORKER_ROLE_MASTER:
			COORDINATOR_WORKER_EVENT_LOOP(
				MasterEvent,
				eventQueue->separated.master
			);
			break;
		case WORKER_ROLE_SLAVE:
			COORDINATOR_WORKER_EVENT_LOOP(
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

bool CoordinatorWorker::init() {
	Coordinator *coordinator = Coordinator::getInstance();

	CoordinatorWorker::idGenerator = &coordinator->idGenerator;
	CoordinatorWorker::eventQueue = &coordinator->eventQueue;

	return true;
}

bool CoordinatorWorker::init( GlobalConfig &config, WorkerRole role, uint32_t workerId ) {
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			config.size.key,
			config.size.chunk
		)
	);
	this->role = role;
	this->workerId = workerId;
	return role != WORKER_ROLE_UNDEFINED;
}

bool CoordinatorWorker::start() {
	this->isRunning = true;
	if ( pthread_create( &this->tid, NULL, CoordinatorWorker::run, ( void * ) this ) != 0 ) {
		__ERROR__( "CoordinatorWorker", "start", "Cannot start worker thread." );
		return false;
	}
	return true;
}

void CoordinatorWorker::stop() {
	this->isRunning = false;
}

void CoordinatorWorker::print( FILE *f ) {
	char role[ 16 ];
	switch( this->role ) {
		case WORKER_ROLE_MIXED:
			strcpy( role, "Mixed" );
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
