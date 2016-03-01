#include "worker.hh"
#include "../main/server.hh"

uint32_t SlaveWorker::dataChunkCount;
uint32_t SlaveWorker::parityChunkCount;
uint32_t SlaveWorker::chunkCount;
bool SlaveWorker::disableSeal;
unsigned int SlaveWorker::delay;
IDGenerator *SlaveWorker::idGenerator;
ArrayMap<int, ServerPeerSocket> *SlaveWorker::slavePeers;
Pending *SlaveWorker::pending;
PendingAck *SlaveWorker::pendingAck;
ServerAddr *SlaveWorker::slaveServerAddr;
Coding *SlaveWorker::coding;
ServerEventQueue *SlaveWorker::eventQueue;
StripeList<ServerPeerSocket> *SlaveWorker::stripeList;
std::vector<StripeListIndex> *SlaveWorker::stripeListIndex;
Map *SlaveWorker::map;
MemoryPool<Chunk> *SlaveWorker::chunkPool;
std::vector<MixedChunkBuffer *> *SlaveWorker::chunkBuffer;
GetChunkBuffer *SlaveWorker::getChunkBuffer;
DegradedChunkBuffer *SlaveWorker::degradedChunkBuffer;
RemappedBuffer *SlaveWorker::remappedBuffer;
PacketPool *SlaveWorker::packetPool;
Timestamp *SlaveWorker::timestamp;

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
		case EVENT_TYPE_CLIENT:
			this->dispatch( event.event.master );
			break;
		case EVENT_TYPE_SERVER:
			this->dispatch( event.event.slave );
			break;
		case EVENT_TYPE_SERVER_PEER:
			this->dispatch( event.event.slavePeer );
			break;
		default:
			break;
	}
}

void SlaveWorker::dispatch( CodingEvent event ) {
	switch( event.type ) {
		case CODING_EVENT_TYPE_DECODE:
			SlaveWorker::coding->decode( event.message.decode.chunks, event.message.decode.status );
			break;
		default:
			return;
	}
}

void SlaveWorker::dispatch( IOEvent event ) {
	switch( event.type ) {
		case IO_EVENT_TYPE_FLUSH_CHUNK:
			this->storage->write(
				event.chunk,
				false
			);
			if ( event.clear )
				event.chunk->status = CHUNK_STATUS_NEEDS_LOAD_FROM_DISK;
			break;
	}
}

void SlaveWorker::dispatch( ServerEvent event ) {
}

ServerPeerSocket *SlaveWorker::getSlaves( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId ) {
	ServerPeerSocket *ret;
	listId = SlaveWorker::stripeList->get(
		data, ( size_t ) size,
		this->dataServerSockets,
		this->parityServerSockets,
		&chunkId, false
	);

	ret = *this->dataServerSockets;

	return ret;
}

bool SlaveWorker::getSlaves( uint32_t listId ) {
	SlaveWorker::stripeList->get( listId, this->parityServerSockets, this->dataServerSockets );

	for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ )
		if ( ! this->parityServerSockets[ i ]->ready() )
			return false;
	for ( uint32_t i = 0; i < SlaveWorker::dataChunkCount; i++ )
		if ( ! this->dataServerSockets[ i ]->ready() )
			return false;
	return true;
}

void SlaveWorker::free() {
	if ( this->storage ) {
		this->storage->stop();
		Storage::destroy( this->storage );
	}
	this->protocol.free();
	delete this->buffer.data;
	this->buffer.data = 0;
	this->buffer.size = 0;
	delete this->chunkStatus;

	this->dataChunk->free();
	this->parityChunk->free();
	delete this->dataChunk;
	delete this->parityChunk;
	delete[] this->chunks;

	delete[] this->sealIndicators[ SlaveWorker::parityChunkCount ];
	delete[] this->sealIndicators[ SlaveWorker::parityChunkCount + 1 ];
	delete[] this->sealIndicators;

	this->forward.dataChunk->free();
	this->forward.parityChunk->free();
	delete this->forward.dataChunk;
	delete this->forward.parityChunk;
	delete[] this->forward.chunks;

	delete[] this->freeChunks;
	delete[] this->dataServerSockets;
	delete[] this->parityServerSockets;
}

void *SlaveWorker::run( void *argv ) {
	SlaveWorker *worker = ( SlaveWorker * ) argv;
	WorkerRole role = worker->getRole();
	ServerEventQueue *eventQueue = SlaveWorker::eventQueue;

#define SERVER_WORKER_EVENT_LOOP(_EVENT_TYPE_, _EVENT_QUEUE_) \
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
			// SERVER_WORKER_EVENT_LOOP(
			// 	MixedEvent,
			// 	eventQueue->mixed
			// );
		{
			MixedEvent event;
			bool ret;
			while( worker->getIsRunning() | ( ret = eventQueue->extractMixed( event ) ) ) {
				if ( ret )
					worker->dispatch( event );
			}
		}
			break;
		case WORKER_ROLE_CODING:
			SERVER_WORKER_EVENT_LOOP(
				CodingEvent,
				eventQueue->separated.coding
			);
			break;
		case WORKER_ROLE_COORDINATOR:
			SERVER_WORKER_EVENT_LOOP(
				CoordinatorEvent,
				eventQueue->separated.coordinator
			);
			break;
		case WORKER_ROLE_IO:
			SERVER_WORKER_EVENT_LOOP(
				IOEvent,
				eventQueue->separated.io
			);
			break;
		case WORKER_ROLE_CLIENT:
			SERVER_WORKER_EVENT_LOOP(
				ClientEvent,
				eventQueue->separated.master
			);
			break;
		case WORKER_ROLE_SERVER:
			SERVER_WORKER_EVENT_LOOP(
				ServerEvent,
				eventQueue->separated.slave
			);
			break;
		case WORKER_ROLE_SERVER_PEER:
			SERVER_WORKER_EVENT_LOOP(
				ServerPeerEvent,
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

	SlaveWorker::idGenerator = &slave->idGenerator;
	SlaveWorker::dataChunkCount = slave->config.global.coding.params.getDataChunkCount();
	SlaveWorker::parityChunkCount = slave->config.global.coding.params.getParityChunkCount();
	SlaveWorker::chunkCount = SlaveWorker::dataChunkCount + SlaveWorker::parityChunkCount;
	SlaveWorker::disableSeal = slave->config.slave.seal.disabled;
	SlaveWorker::delay = 0;
	SlaveWorker::slavePeers = &slave->sockets.slavePeers;
	SlaveWorker::pending = &slave->pending;
	SlaveWorker::pendingAck = &slave->pendingAck;
	SlaveWorker::slaveServerAddr = &slave->config.slave.slave.addr;
	SlaveWorker::coding = slave->coding;
	SlaveWorker::eventQueue = &slave->eventQueue;
	SlaveWorker::stripeList = slave->stripeList;
	SlaveWorker::stripeListIndex = &slave->stripeListIndex;
	SlaveWorker::map = &slave->map;
	SlaveWorker::chunkPool = slave->chunkPool;
	SlaveWorker::chunkBuffer = &slave->chunkBuffer;
	SlaveWorker::getChunkBuffer = &slave->getChunkBuffer;
	SlaveWorker::degradedChunkBuffer = &slave->degradedChunkBuffer;
	SlaveWorker::remappedBuffer = &slave->remappedBuffer;
	SlaveWorker::packetPool = &slave->packetPool;
	SlaveWorker::timestamp = &slave->timestamp;
	return true;
}

bool SlaveWorker::init( GlobalConfig &globalConfig, ServerConfig &serverConfig, WorkerRole role, uint32_t workerId ) {
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			globalConfig.size.key,
			globalConfig.size.chunk
		)
	);
	this->role = role;
	this->workerId = workerId;
	this->buffer.data = new char[ globalConfig.size.chunk ];
	this->buffer.size = globalConfig.size.chunk;
	this->chunkStatus = new BitmaskArray( SlaveWorker::chunkCount, 1 );

	this->dataChunk = new Chunk();
	this->parityChunk = new Chunk();
	this->chunks = new Chunk*[ SlaveWorker::chunkCount ];

	this->forward.dataChunk = new Chunk();
	this->forward.parityChunk = new Chunk();
	this->forward.chunks = new Chunk*[ SlaveWorker::chunkCount ];

	this->freeChunks = new Chunk[ SlaveWorker::dataChunkCount ];
	for( uint32_t i = 0; i < SlaveWorker::dataChunkCount; i++ ) {
		this->freeChunks[ i ].init( globalConfig.size.chunk );
		this->freeChunks[ i ].init();
	}
	this->dataChunk->init( globalConfig.size.chunk );
	this->parityChunk->init( globalConfig.size.chunk );
	this->dataChunk->init();
	this->parityChunk->init();

	this->forward.dataChunk->init();
	this->forward.parityChunk->init();

	this->sealIndicators = new bool*[ SlaveWorker::parityChunkCount + 2 ];
	this->sealIndicators[ SlaveWorker::parityChunkCount ] = new bool[ SlaveWorker::dataChunkCount ];
	this->sealIndicators[ SlaveWorker::parityChunkCount + 1 ] = new bool[ SlaveWorker::dataChunkCount ];

	this->dataServerSockets = new ServerPeerSocket*[ SlaveWorker::dataChunkCount ];
	this->parityServerSockets = new ServerPeerSocket*[ SlaveWorker::parityChunkCount ];
	switch( this->role ) {
		case WORKER_ROLE_MIXED:
		case WORKER_ROLE_IO:
			this->storage = Storage::instantiate( serverConfig );
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
		case WORKER_ROLE_CLIENT:
			strcpy( role, "Master" );
			break;
		case WORKER_ROLE_SERVER:
			strcpy( role, "Slave" );
			break;
		case WORKER_ROLE_SERVER_PEER:
			strcpy( role, "Slave peer" );
			break;
		default:
			return;
	}
	fprintf( f, "%11s worker (Thread ID = %lu): %srunning\n", role, this->tid, this->isRunning ? "" : "not " );
}
