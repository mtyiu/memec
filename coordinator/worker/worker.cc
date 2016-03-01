#include "worker.hh"
#include "../main/coordinator.hh"

#define WORKER_COLOR	YELLOW

uint32_t CoordinatorWorker::dataChunkCount;
uint32_t CoordinatorWorker::parityChunkCount;
uint32_t CoordinatorWorker::chunkCount;
IDGenerator *CoordinatorWorker::idGenerator;
CoordinatorEventQueue *CoordinatorWorker::eventQueue;
RemappingRecordMap *CoordinatorWorker::remappingRecords;
StripeList<SlaveSocket> *CoordinatorWorker::stripeList;
Pending *CoordinatorWorker::pending;

void CoordinatorWorker::dispatch( MixedEvent event ) {
	switch( event.type ) {
		case EVENT_TYPE_COORDINATOR:
			this->dispatch( event.event.coordinator );
			break;
		case EVENT_TYPE_CLIENT:
			this->dispatch( event.event.master );
			break;
		case EVENT_TYPE_SERVER:
			this->dispatch( event.event.slave );
			break;
		default:
			return;
	}
}

void CoordinatorWorker::dispatch( CoordinatorEvent event ) {
	Coordinator *coordinator = Coordinator::getInstance();
	struct {
		size_t size;
		char *data;
	} buffer;

	switch( event.type ) {
		case COORDINATOR_EVENT_TYPE_SYNC_REMAPPED_PARITY:
		{
			uint32_t requestId = coordinator->idGenerator.nextVal( this->workerId );
			SlaveEvent slaveEvent;

			// prepare the request for all master
			Packet *packet = coordinator->packetPool.malloc();
			buffer.data = packet->data;
			this->protocol.reqSyncRemappedData(
				buffer.size, Coordinator::instanceId, requestId,
				event.message.parity.target, buffer.data
			);
			packet->size = buffer.size;

			LOCK( &coordinator->sockets.slaves.lock );
			uint32_t numSlaves = coordinator->sockets.slaves.size();
			coordinator->pending.insertRemappedDataRequest(
				requestId,
				event.message.parity.lock,
				event.message.parity.cond,
				event.message.parity.done,
				numSlaves
			);
			packet->setReferenceCount( numSlaves );
			for ( uint32_t i = 0; i < numSlaves; i++ ) {
				SlaveSocket *socket = coordinator->sockets.slaves[ i ];
				slaveEvent.syncRemappedData( socket, packet );
				coordinator->eventQueue.insert( slaveEvent );
			}
			UNLOCK( &coordinator->sockets.slaves.lock );

		}
			break;
		default:
			break;
	}

}

void CoordinatorWorker::free() {
	this->protocol.free();
	delete[] this->survivingChunkIds;
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
		case WORKER_ROLE_CLIENT:
			COORDINATOR_WORKER_EVENT_LOOP(
				MasterEvent,
				eventQueue->separated.master
			);
			break;
		case WORKER_ROLE_SERVER:
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

	CoordinatorWorker::dataChunkCount =
	coordinator->config.global.coding.params.getDataChunkCount();
	CoordinatorWorker::parityChunkCount = coordinator->config.global.coding.params.getParityChunkCount();
	CoordinatorWorker::chunkCount = CoordinatorWorker::dataChunkCount + CoordinatorWorker::parityChunkCount;
	CoordinatorWorker::idGenerator = &coordinator->idGenerator;
	CoordinatorWorker::eventQueue = &coordinator->eventQueue;
	CoordinatorWorker::remappingRecords = &coordinator->remappingRecords;
	CoordinatorWorker::stripeList = coordinator->stripeList;
	CoordinatorWorker::pending = &coordinator->pending;

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
	this->survivingChunkIds = new uint32_t[ CoordinatorWorker::chunkCount ];
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
		case WORKER_ROLE_CLIENT:
			strcpy( role, "Master" );
			break;
		case WORKER_ROLE_SERVER:
			strcpy( role, "Slave" );
			break;
		default:
			return;
	}
	fprintf( f, "%11s worker (Thread ID = %lu): %srunning\n", role, this->tid, this->isRunning ? "" : "not " );
}
