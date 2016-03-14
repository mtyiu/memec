#include "worker.hh"
#include "../main/server.hh"

uint32_t ServerWorker::dataChunkCount;
uint32_t ServerWorker::parityChunkCount;
uint32_t ServerWorker::chunkCount;
bool ServerWorker::disableSeal;
unsigned int ServerWorker::delay;
IDGenerator *ServerWorker::idGenerator;
ArrayMap<int, ServerPeerSocket> *ServerWorker::serverPeers;
Pending *ServerWorker::pending;
PendingAck *ServerWorker::pendingAck;
ServerAddr *ServerWorker::serverServerAddr;
Coding *ServerWorker::coding;
ServerEventQueue *ServerWorker::eventQueue;
StripeList<ServerPeerSocket> *ServerWorker::stripeList;
std::vector<StripeListIndex> *ServerWorker::stripeListIndex;
Map *ServerWorker::map;
MemoryPool<Chunk> *ServerWorker::chunkPool;
std::vector<MixedChunkBuffer *> *ServerWorker::chunkBuffer;
GetChunkBuffer *ServerWorker::getChunkBuffer;
DegradedChunkBuffer *ServerWorker::degradedChunkBuffer;
RemappedBuffer *ServerWorker::remappedBuffer;
PacketPool *ServerWorker::packetPool;
Timestamp *ServerWorker::timestamp;

void ServerWorker::dispatch( MixedEvent event ) {
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
			this->dispatch( event.event.client );
			break;
		case EVENT_TYPE_SERVER:
			this->dispatch( event.event.server );
			break;
		case EVENT_TYPE_SERVER_PEER:
			this->dispatch( event.event.serverPeer );
			break;
		default:
			break;
	}
}

void ServerWorker::dispatch( CodingEvent event ) {
	switch( event.type ) {
		case CODING_EVENT_TYPE_DECODE:
			ServerWorker::coding->decode( event.message.decode.chunks, event.message.decode.status );
			break;
		default:
			return;
	}
}

void ServerWorker::dispatch( IOEvent event ) {
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

void ServerWorker::dispatch( ServerEvent event ) {
}

ServerPeerSocket *ServerWorker::getServers( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId ) {
	ServerPeerSocket *ret;
	listId = ServerWorker::stripeList->get(
		data, ( size_t ) size,
		this->dataServerSockets,
		this->parityServerSockets,
		&chunkId, false
	);

	ret = *this->dataServerSockets;

	return ret;
}

bool ServerWorker::getServers( uint32_t listId ) {
	ServerWorker::stripeList->get( listId, this->parityServerSockets, this->dataServerSockets );

	for ( uint32_t i = 0; i < ServerWorker::parityChunkCount; i++ )
		if ( ! this->parityServerSockets[ i ]->ready() )
			return false;
	for ( uint32_t i = 0; i < ServerWorker::dataChunkCount; i++ )
		if ( ! this->dataServerSockets[ i ]->ready() )
			return false;
	return true;
}

void ServerWorker::free() {
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

	delete[] this->sealIndicators[ ServerWorker::parityChunkCount ];
	delete[] this->sealIndicators[ ServerWorker::parityChunkCount + 1 ];
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

void *ServerWorker::run( void *argv ) {
	ServerWorker *worker = ( ServerWorker * ) argv;
	ServerEventQueue *eventQueue = ServerWorker::eventQueue;

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

bool ServerWorker::init() {
	Server *server = Server::getInstance();

	ServerWorker::idGenerator = &server->idGenerator;
	ServerWorker::dataChunkCount = server->config.global.coding.params.getDataChunkCount();
	ServerWorker::parityChunkCount = server->config.global.coding.params.getParityChunkCount();
	ServerWorker::chunkCount = ServerWorker::dataChunkCount + ServerWorker::parityChunkCount;
	ServerWorker::disableSeal = server->config.server.seal.disabled;
	ServerWorker::delay = 0;
	ServerWorker::serverPeers = &server->sockets.serverPeers;
	ServerWorker::pending = &server->pending;
	ServerWorker::pendingAck = &server->pendingAck;
	ServerWorker::serverServerAddr = &server->config.server.server.addr;
	ServerWorker::coding = server->coding;
	ServerWorker::eventQueue = &server->eventQueue;
	ServerWorker::stripeList = server->stripeList;
	ServerWorker::stripeListIndex = &server->stripeListIndex;
	ServerWorker::map = &server->map;
	ServerWorker::chunkPool = server->chunkPool;
	ServerWorker::chunkBuffer = &server->chunkBuffer;
	ServerWorker::getChunkBuffer = &server->getChunkBuffer;
	ServerWorker::degradedChunkBuffer = &server->degradedChunkBuffer;
	ServerWorker::remappedBuffer = &server->remappedBuffer;
	ServerWorker::packetPool = &server->packetPool;
	ServerWorker::timestamp = &server->timestamp;
	return true;
}

bool ServerWorker::init( GlobalConfig &globalConfig, ServerConfig &serverConfig, uint32_t workerId ) {
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			globalConfig.size.key,
			globalConfig.size.chunk
		)
	);
	this->workerId = workerId;
	this->buffer.data = new char[ globalConfig.size.chunk ];
	this->buffer.size = globalConfig.size.chunk;
	this->chunkStatus = new BitmaskArray( ServerWorker::chunkCount, 1 );

	this->dataChunk = new Chunk();
	this->parityChunk = new Chunk();
	this->chunks = new Chunk*[ ServerWorker::chunkCount ];

	this->forward.dataChunk = new Chunk();
	this->forward.parityChunk = new Chunk();
	this->forward.chunks = new Chunk*[ ServerWorker::chunkCount ];

	this->freeChunks = new Chunk[ ServerWorker::dataChunkCount ];
	for( uint32_t i = 0; i < ServerWorker::dataChunkCount; i++ ) {
		this->freeChunks[ i ].init( globalConfig.size.chunk );
		this->freeChunks[ i ].init();
	}
	this->dataChunk->init( globalConfig.size.chunk );
	this->parityChunk->init( globalConfig.size.chunk );
	this->dataChunk->init();
	this->parityChunk->init();

	this->forward.dataChunk->init();
	this->forward.parityChunk->init();

	this->sealIndicators = new bool*[ ServerWorker::parityChunkCount + 2 ];
	this->sealIndicators[ ServerWorker::parityChunkCount ] = new bool[ ServerWorker::dataChunkCount ];
	this->sealIndicators[ ServerWorker::parityChunkCount + 1 ] = new bool[ ServerWorker::dataChunkCount ];

	this->dataServerSockets = new ServerPeerSocket*[ ServerWorker::dataChunkCount ];
	this->parityServerSockets = new ServerPeerSocket*[ ServerWorker::parityChunkCount ];

	this->storage = Storage::instantiate( serverConfig );
	this->storage->start();

	return true;
}

bool ServerWorker::start() {
	this->isRunning = true;
	if ( pthread_create( &this->tid, NULL, ServerWorker::run, ( void * ) this ) != 0 ) {
		__ERROR__( "ServerWorker", "start", "Cannot start worker thread." );
		return false;
	}
	return true;
}

void ServerWorker::stop() {
	this->isRunning = false;
}

void ServerWorker::print( FILE *f ) {
	fprintf( f, "Worker (Thread ID = %lu): %srunning\n", this->tid, this->isRunning ? "" : "not " );
}
