#include "chunk_buffer.hh"
#include "../main/server.hh"

uint32_t ChunkBuffer::capacity;
uint32_t ChunkBuffer::dataChunkCount;
Coding *ChunkBuffer::coding;
// MemoryPool<Chunk> *ChunkBuffer::chunkPool;
ChunkPool *ChunkBuffer::chunkPool;
ServerEventQueue *ChunkBuffer::eventQueue;
Map *ChunkBuffer::map;

void ChunkBuffer::init() {
	Server *server = Server::getInstance();
	ChunkBuffer::capacity = server->config.global.size.chunk;
	ChunkBuffer::dataChunkCount = server->config.global.coding.params.getDataChunkCount();
	ChunkBuffer::coding = server->coding;
	ChunkBuffer::chunkPool = &server->chunkPool;
	ChunkBuffer::eventQueue = &server->eventQueue;
	ChunkBuffer::map = &server->map;
}

ChunkBuffer::ChunkBuffer( bool isReady ) {
	LOCK_INIT( &this->lock );
	this->isReady = isReady;
}

ChunkBuffer::~ChunkBuffer() {}
