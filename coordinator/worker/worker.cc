#include "worker.hh"
#include "../main/coordinator.hh"

#define WORKER_COLOR	YELLOW

uint32_t CoordinatorWorker::dataChunkCount;
uint32_t CoordinatorWorker::parityChunkCount;
uint32_t CoordinatorWorker::chunkCount;
IDGenerator *CoordinatorWorker::idGenerator;
CoordinatorEventQueue *CoordinatorWorker::eventQueue;
RemappingRecordMap *CoordinatorWorker::remappingRecords;
StripeList<ServerSocket> *CoordinatorWorker::stripeList;
Pending *CoordinatorWorker::pending;

void CoordinatorWorker::dispatch( MixedEvent event ) {
	switch( event.type ) {
		case EVENT_TYPE_COORDINATOR:
			this->dispatch( event.event.coordinator );
			break;
		case EVENT_TYPE_CLIENT:
			this->dispatch( event.event.client );
			break;
		case EVENT_TYPE_SERVER:
			this->dispatch( event.event.server );
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
			ServerEvent serverEvent;

			// prepare the request for all client
			Packet *packet = coordinator->packetPool.malloc();
			buffer.data = packet->data;
			this->protocol.reqSyncRemappedData(
				buffer.size, Coordinator::instanceId, requestId,
				event.message.parity.target, buffer.data
			);
			packet->size = buffer.size;

			LOCK( &coordinator->sockets.servers.lock );
			uint32_t numServers = coordinator->sockets.servers.size();
			coordinator->pending.insertRemappedDataRequest(
				requestId,
				event.message.parity.lock,
				event.message.parity.cond,
				event.message.parity.done,
				numServers
			);
			packet->setReferenceCount( numServers );
			for ( uint32_t i = 0; i < numServers; i++ ) {
				ServerSocket *socket = coordinator->sockets.servers[ i ];
				serverEvent.syncRemappedData( socket, packet );
				coordinator->eventQueue.insert( serverEvent );
			}
			UNLOCK( &coordinator->sockets.servers.lock );
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
	CoordinatorEventQueue *eventQueue = CoordinatorWorker::eventQueue;

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

bool CoordinatorWorker::init( GlobalConfig &config, uint32_t workerId ) {
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			config.size.key,
			config.size.chunk
		)
	);
	this->workerId = workerId;
	this->survivingChunkIds = new uint32_t[ CoordinatorWorker::chunkCount ];
	return true;
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
	fprintf( f, "Worker (Thread ID = %lu): %srunning\n", this->tid, this->isRunning ? "" : "not " );
}
