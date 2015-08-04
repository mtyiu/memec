#include "worker.hh"
#include "../main/coordinator.hh"
#include "../../common/util/debug.hh"

#define WORKER_COLOR	YELLOW

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
	bool connected, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	switch( event.type ) {
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resRegisterMaster( buffer.size, true );
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRegisterMaster( buffer.size, false );
			isSend = true;
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else {

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
			buffer.data = this->protocol.resRegisterSlave( buffer.size, true );
			isSend = true;
			break;
		case SLAVE_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRegisterSlave( buffer.size, false );
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
			__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else {
		// Parse requests from slaves
		ProtocolHeader header;
		ret = event.socket->recv(
			this->protocol.buffer.recv,
			this->protocol.buffer.size,
			connected,
			false
		);
		if ( ! this->protocol.parseHeader( header ) ) {
			__ERROR__( "CoordinatorWorker", "dispatch", "Undefined message." );
		} else {
			if (
				( header.magic != PROTO_MAGIC_REQUEST && header.magic != PROTO_MAGIC_HEARTBEAT ) ||
				header.from != PROTO_MAGIC_FROM_SLAVE ||
				header.to != PROTO_MAGIC_TO_COORDINATOR
			) {
				__ERROR__( "CoordinatorWorker", "dispatch", "Invalid protocol header." );
				return;
			}

			if ( header.opcode == PROTO_OPCODE_SYNC ) {
				size_t bytes, offset, count = 0;
				struct HeartbeatHeader heartbeatHeader;
				struct SlaveSyncHeader slaveSyncHeader;

				if ( this->protocol.parseHeartbeatHeader( heartbeatHeader ) ) {
					event.socket->load.ops.get = heartbeatHeader.get;
					event.socket->load.ops.set = heartbeatHeader.set;
					event.socket->load.ops.update = heartbeatHeader.update;
					event.socket->load.ops.del = heartbeatHeader.del;

					offset = PROTO_HEADER_SIZE + PROTO_HEARTBEAT_SIZE;
					while ( offset < ( size_t ) ret ) {
						if ( ! this->protocol.parseSlaveSyncHeader( slaveSyncHeader, bytes, offset ) )
							break;
						offset += bytes;
						count++;

						// Update metadata map
						Key key;
						key.size = slaveSyncHeader.keySize;
						key.data = slaveSyncHeader.key;

						Metadata metadata;
						metadata.opcode = slaveSyncHeader.opcode;
						metadata.listId = slaveSyncHeader.listId;
						metadata.stripeId = slaveSyncHeader.stripeId;
						metadata.chunkId = slaveSyncHeader.chunkId;

						if ( event.socket->keys.find( key ) == event.socket->keys.end() )
							key.dup();
						event.socket->keys[ key ] = metadata;
					}
				} else {
					__ERROR__( "CoordinatorWorker", "dispatch", "Invalid heartbeat protocol header." );
				}
			} else {
				__ERROR__( "CoordinatorWorker", "dispatch", "Invalid opcode from slave." );
			}
		}
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

	CoordinatorWorker::eventQueue = &coordinator->eventQueue;

	return true;
}

bool CoordinatorWorker::init( GlobalConfig &config, WorkerRole role ) {
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			config.size.key,
			config.size.chunk
		)
	);
	this->role = role;
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
