#include "chunk_buffer.hh"
#include "../main/slave.hh"

uint32_t ChunkBuffer::capacity;
uint32_t ChunkBuffer::dataChunkCount;
Coding *ChunkBuffer::coding;
MemoryPool<Chunk> *ChunkBuffer::chunkPool;
SlaveEventQueue *ChunkBuffer::eventQueue;
Map *ChunkBuffer::map;

void ChunkBuffer::init() {
	Slave *slave = Slave::getInstance();
	ChunkBuffer::capacity = slave->config.global.size.chunk;
	ChunkBuffer::dataChunkCount = slave->config.global.coding.params.getDataChunkCount();
	ChunkBuffer::coding = slave->coding;
	ChunkBuffer::chunkPool = slave->chunkPool;
	ChunkBuffer::eventQueue = &slave->eventQueue;
	ChunkBuffer::map = &slave->map;
}

ChunkBuffer::ChunkBuffer() {
	LOCK_INIT( &this->lock );
}

ChunkBuffer::~ChunkBuffer() {}
