#include "worker.hh"
#include "../main/server.hh"

uint32_t ServerWorker::dataChunkCount;
uint32_t ServerWorker::parityChunkCount;
uint32_t ServerWorker::chunkCount;
unsigned int ServerWorker::delay;
IDGenerator *ServerWorker::idGenerator;
ArrayMap<int, ServerPeerSocket> *ServerWorker::serverPeers;
Pending *ServerWorker::pending;
ServerEventQueue *ServerWorker::eventQueue;
StripeList<ServerPeerSocket> *ServerWorker::stripeList;
Map *ServerWorker::map;
std::vector<MixedChunkBuffer *> *ServerWorker::chunkBuffer;
GetChunkBuffer *ServerWorker::getChunkBuffer;
DegradedChunkBuffer *ServerWorker::degradedChunkBuffer;
RemappedBuffer *ServerWorker::remappedBuffer;
PacketPool *ServerWorker::packetPool;
ChunkPool *ServerWorker::chunkPool;

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
		case EVENT_TYPE_SERVER_PEER:
			this->dispatch( event.event.serverPeer );
			break;
		case EVENT_TYPE_DUMMY:
			break;
		default:
			__ERROR__( "ClientWorker", "dispatch", "Unsupported event type." );
			break;
	}
}

void ServerWorker::dispatch( CodingEvent event ) {
	switch( event.type ) {
		case CODING_EVENT_TYPE_DECODE:
			Server::getInstance()->coding->decode( event.message.decode.chunks, event.message.decode.status );
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
			break;
		default:
			return;
	}
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
	delete this->chunkStatusBackup;

	this->tempChunkPool.free( this->dataChunk );
	this->tempChunkPool.free( this->parityChunk );
	delete[] this->chunks;

	delete[] this->sealIndicators[ ServerWorker::parityChunkCount ];
	delete[] this->sealIndicators[ ServerWorker::parityChunkCount + 1 ];
	delete[] this->sealIndicators;

	this->tempChunkPool.free( this->forward.dataChunk );
	this->tempChunkPool.free( this->forward.parityChunk );
	delete[] this->forward.chunks;

	for( uint32_t i = 0; i < ServerWorker::dataChunkCount; i++ ) {
		this->tempChunkPool.free( this->freeChunks[ i ] );
	}
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
	ServerWorker::delay = 0;
	ServerWorker::serverPeers = &server->sockets.serverPeers;
	ServerWorker::pending = &server->pending;
	ServerWorker::eventQueue = &server->eventQueue;
	ServerWorker::stripeList = server->stripeList;
	ServerWorker::map = &server->map;
	ServerWorker::chunkBuffer = &server->chunkBuffer;
	ServerWorker::getChunkBuffer = &server->getChunkBuffer;
	ServerWorker::degradedChunkBuffer = &server->degradedChunkBuffer;
	ServerWorker::remappedBuffer = &server->remappedBuffer;
	ServerWorker::packetPool = &server->packetPool;
	ServerWorker::chunkPool = &server->chunkPool;
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
	this->chunkStatusBackup = new BitmaskArray( ServerWorker::chunkCount, 1 );

	this->dataChunk = this->tempChunkPool.alloc();
	this->parityChunk = this->tempChunkPool.alloc();
	this->chunks = new Chunk*[ ServerWorker::chunkCount ];

	this->forward.dataChunk = this->tempChunkPool.alloc();
	this->forward.parityChunk = this->tempChunkPool.alloc();
	this->forward.chunks = new Chunk*[ ServerWorker::chunkCount ];

	this->freeChunks = new Chunk*[ ServerWorker::dataChunkCount ];
	for( uint32_t i = 0; i < ServerWorker::dataChunkCount; i++ ) {
		this->freeChunks[ i ] = this->tempChunkPool.alloc();
	}

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
