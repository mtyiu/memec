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
	if ( ! this->startAddress ) {
		__ERROR__( "ChunkPool", "init", "Cannot allocate memory." );
		exit( 1 );
	} else {
		memset( this->startAddress, 0, capacity );
	}
}

Chunk *ChunkPool::alloc() {
	// Update counter
	uint32_t index = ( this->count++ ); // index = the value of this->count before increment

	// Check whether there are still free chunks
	if ( index >= total ) {
		this->count = total;
		return 0;
	}

	// Calculate memory address
	return ( Chunk * )( this->startAddress + ( index * ( CHUNK_METADATA_SIZE + this->chunkSize ) ) );
}

Chunk *ChunkPool::alloc( uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
	Chunk *chunk = this->alloc();
	if ( chunk )
		this->setChunk( chunk, listId, stripeId, chunkId );
	return chunk;
}

void ChunkPool::setChunk( Chunk *chunk, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t size ) {
	struct ChunkMetadata *chunkMetadata = ( struct ChunkMetadata * ) chunk;

	chunkMetadata->listId = listId;
	chunkMetadata->stripeId = stripeId;
	chunkMetadata->size = size;
}

Chunk *ChunkPool::getChunk( char *ptr, uint32_t *listIdPtr, uint32_t *stripeIdPtr, uint32_t *sizePtr, uint32_t *offsetPtr ) {
	Chunk *chunk;
	uint32_t *metadata;
	uint32_t offset = ( uint64_t )( ( ( uint64_t )( ptr - this->startAddress ) ) % ( CHUNK_METADATA_SIZE + this->chunkSize ) );

	chunk = ( Chunk * )( ptr - offset );
	metadata = ( uint32_t * ) chunk;

	if ( listIdPtr )   *listIdPtr   = metadata[ 0 ];
	if ( stripeIdPtr ) *stripeIdPtr = metadata[ 1 ];
	if ( sizePtr )     *sizePtr     = metadata[ 2 ];
	if ( offsetPtr )   *offsetPtr   = offset;

	return chunk;
}

void ChunkPool::print( FILE *f ) {
	uint32_t count = this->count;
	fprintf(
		f,
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
