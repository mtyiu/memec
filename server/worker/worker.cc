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
	ServerEventQueue *eventQueue = SlaveWorker::eventQueue;

	MixedEvent event;
	bool ret;
	while( worker->getIsRunning() | ( ret = eventQueue->extractMixed( event ) ) ) {
		if ( ret )
			worker->dispatch( event );
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
	SlaveWorker::disableSeal = slave->config.server.seal.disabled;
	SlaveWorker::delay = 0;
	SlaveWorker::slavePeers = &slave->sockets.slavePeers;
	SlaveWorker::pending = &slave->pending;
	SlaveWorker::pendingAck = &slave->pendingAck;
	SlaveWorker::slaveServerAddr = &slave->config.server.server.addr;
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

bool SlaveWorker::init( GlobalConfig &globalConfig, ServerConfig &serverConfig, uint32_t workerId ) {
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			globalConfig.size.key,
			globalConfig.size.chunk
		)
	);
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

	this->storage = Storage::instantiate( serverConfig );
	this->storage->start();

	return true;
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
	fprintf( f, "Worker (Thread ID = %lu): %srunning\n", this->tid, this->isRunning ? "" : "not " );
}
