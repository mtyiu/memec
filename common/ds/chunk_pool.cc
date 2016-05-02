#include "chunk_pool.hh"
#include "../../common/util/debug.hh"

uint32_t ChunkUtil::chunkSize;
uint32_t ChunkUtil::dataChunkCount;

ChunkPool::ChunkPool() {
	this->total = 0;
	this->count = 0;
	this->startAddress = 0;
}

ChunkPool::~ChunkPool() {
	if ( this->startAddress )
		free( this->startAddress );
	this->total = 0;
	this->count = 0;
	this->startAddress = 0;
}

void ChunkPool::init( uint32_t chunkSize, uint64_t capacity ) {
	chunkSize += CHUNK_METADATA_SIZE;
	this->total = ( uint32_t )( capacity / chunkSize );
	capacity = ( uint64_t ) this->total * chunkSize;
	this->startAddress = ( char * ) malloc( capacity );
	if ( ! this->startAddress ) {
		__ERROR__( "ChunkPool", "init", "Cannot allocate memory." );
		exit( 1 );
	} else {
		memset( this->startAddress, 0, capacity );
	}
}

Chunk *ChunkPool::alloc( uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
	// Update counter
	uint32_t index = ( this->count++ ); // index = the value of this->count before increment

	// Check whether there are still free chunks
	if ( index >= total ) {
		this->count = total;
		return 0;
	}

	// Calculate memory address
	Chunk *chunk = ( Chunk * )( this->startAddress + ( index * ( CHUNK_METADATA_SIZE + ChunkUtil::chunkSize ) ) );
	if ( chunk ) {
		ChunkUtil::clear( chunk );
		ChunkUtil::set( chunk, listId, stripeId, chunkId );
	}
	return chunk;
}

Chunk *ChunkPool::getChunk( char *ptr, uint32_t &offset ) {
	Chunk *chunk;
	offset = ( uint64_t )( ( ( uint64_t )( ptr - this->startAddress ) ) % ( CHUNK_METADATA_SIZE + ChunkUtil::chunkSize ) );

	chunk = ( Chunk * )( ptr - offset );

	return chunk;
}

bool ChunkPool::isInChunkPool( Chunk *chunk ) {
	char *endAddress = this->startAddress + ( ChunkUtil::chunkSize * this->total );
	return (
		( char * ) chunk >= this->startAddress &&
		( char * ) chunk < endAddress
	);
}

void ChunkPool::print( FILE *f ) {
	uint32_t count = this->count;
	fprintf(
		f,
		"Chunk size       : %u bytes\n"
		"Metadata size    : %u bytes\n"
		"Allocated chunks : %u / %u\n"
		"Start address    : 0x%p\n",
		ChunkUtil::chunkSize,
		( uint32_t ) CHUNK_METADATA_SIZE,
		count, this->total,
		this->startAddress
	);
}
