#include "chunk_pool.hh"
#include "../../common/util/debug.hh"

ChunkPool::ChunkPool() {
	this->chunkSize = 0;
	this->total = 0;
	this->count = 0;
	this->startAddress = 0;
}

ChunkPool::~ChunkPool() {
	if ( this->startAddress )
		free( this->startAddress );
	this->chunkSize = 0;
	this->total = 0;
	this->count = 0;
	this->startAddress = 0;
}

void ChunkPool::init( uint32_t chunkSize, uint64_t capacity ) {
	this->chunkSize = chunkSize;
	chunkSize += CHUNK_METADATA_SIZE;
	this->total = ( uint32_t )( capacity / chunkSize );
	capacity = ( uint64_t ) this->total * chunkSize;
	this->startAddress = ( char * ) malloc( capacity );

	if ( ! this->startAddress )
		__ERROR__( "ChunkPool", "init", "Cannot allocate memory." );
}

char *ChunkPool::alloc( uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
	// Update counter
	uint32_t index = ( this->count++ ); // index = the value of this->count before increment

	// Check whether there are still free chunks
	if ( index >= total ) {
		this->count = index;
		return 0;
	}

	// Calculate memory address
	char *ret = ( this->startAddress + ( index * ( CHUNK_METADATA_SIZE + this->chunkSize ) ) );
	struct ChunkMetadata *chunkMetadata = ( struct ChunkMetadata * ) ret;

	chunkMetadata->listId = listId;
	chunkMetadata->stripeId = stripeId;
	chunkMetadata->size = 0;

	return ret;
}

void ChunkPool::print( FILE *f ) {
	uint32_t count = this->count;
	fprintf(
		f,
		"---------- Chunk Pool ----------\n"
		"Chunk size       : %u bytes\n"
		"Metadata size    : %u bytes\n"
		"Allocated chunks : %u / %u\n"
		"Start address    : 0x%p\n",
		this->chunkSize,
		( uint32_t ) CHUNK_METADATA_SIZE,
		count, this->total,
		this->startAddress
	);
}
