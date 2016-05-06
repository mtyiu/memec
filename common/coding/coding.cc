#include <cassert>
#include "coding.hh"
#include "all_coding.hh"
#include "../util/debug.hh"
#include "../ds/chunk_pool.hh"
#include "../ds/chunk_util.hh"

Chunk *Coding::zeros;

Coding::~Coding() {}

Coding *Coding::instantiate( CodingScheme scheme, CodingParams &params, uint32_t chunkSize ) {
	// Initialize zero block
	ChunkUtil::chunkSize = chunkSize;
	TempChunkPool tempChunkPool;
	Coding::zeros = tempChunkPool.alloc();

	switch( scheme ) {
		case CS_RAID0:
		{
			RAID0Coding *coding;
			coding = new RAID0Coding();
			coding->init( params.getN() );
			return coding;
		}
		case CS_RAID1:
		{
			RAID1Coding *coding;
			coding = new RAID1Coding();
			coding->init( params.getN() );
			return coding;
		}
		case CS_RAID5:
		{
			RAID5Coding *coding;
			coding = new RAID5Coding();
			coding->init( params.getN() );
			return coding;
		}
		case CS_RS:
			return new RSCoding( params.getK(), params.getM(), chunkSize );
		case CS_EMBR:
			break;
		case CS_RDP:
			return new RDPCoding( params.getK(), chunkSize );
		case CS_EVENODD:
			return new EvenOddCoding( params.getK(), chunkSize );
		case CS_CAUCHY:
			return new CauchyCoding( params.getK(), params.getM(), chunkSize );
		default:
			break;
	}

	__ERROR__( "Coding", "instantiate", "Coding scheme is not yet implemented." );
	return 0;
}

void Coding::destroy( Coding *coding ) {
	switch( coding->scheme ) {
		case CS_RAID0:
			delete static_cast<RAID0Coding *>( coding );
			break;
		case CS_RAID1:
			delete static_cast<RAID1Coding *>( coding );
			break;
		case CS_RAID5:
			delete static_cast<RAID5Coding *>( coding );
			break;
		case CS_RS:
			delete static_cast<RSCoding *>( coding );
			break;
		case CS_EMBR:
			break;
		case CS_RDP:
			delete static_cast<RDPCoding *>( coding );
			break;
		case CS_EVENODD:
			delete static_cast<EvenOddCoding *>( coding );
			break;
		case CS_CAUCHY:
			delete static_cast<CauchyCoding *>( coding );
			break;
		default:
			return;
	}
	if ( Coding::zeros ) {
		delete[] Coding::zeros;
		Coding::zeros = 0;
	}
}

char *Coding::bitwiseXOR( char *dst, char *srcA, char *srcB, uint32_t len ) {
	uint64_t *srcA64 = ( uint64_t * ) srcA;
	uint64_t *srcB64 = ( uint64_t * ) srcB;
	uint64_t *dst64 = ( uint64_t * ) dst;

	uint64_t xor64Count = len / sizeof( uint64_t );
	uint64_t i = 0;

	// Word-by-word XOR
	for ( i = 0; i < xor64Count; i++ ) {
		dst64[ i ] = srcA64[ i ] ^ srcB64[ i ];
	}

	i = xor64Count * sizeof( uint64_t );

	for ( ; i < len; i++ ) {
		dst[ i ] = srcA[ i ] ^ srcB[ i ];
	}

	return dst;
}

Chunk *Coding::bitwiseXOR( Chunk *dst, Chunk *srcA, Chunk *srcB, uint32_t size ) {
	Coding::bitwiseXOR(
		ChunkUtil::getData( dst ),
		ChunkUtil::getData( srcA ),
		ChunkUtil::getData( srcB ),
		size
	);
	return dst;
}

uint32_t Coding::forceSeal( Coding *coding, Chunk **chunks, Chunk *tmpParityChunk, bool **sealIndicator, uint32_t dataChunkCount, uint32_t parityChunkCount ) {
	Chunk **tmpChunks = new Chunk *[ dataChunkCount ];
	bool *trueSealIndicator = sealIndicator[ parityChunkCount ];
	uint32_t fixedDataCount = 0;

	// Follow the seal indicators of the parity chunks
	for ( uint32_t j = 0; j < dataChunkCount; j++ ) {
		uint32_t count = 0, total = 0;
		char indicator = -1;
		for ( uint32_t i = 0; i < parityChunkCount; i++ ) {
			if ( ! chunks[ i + dataChunkCount ] )
				continue;
			total++;
			if ( sealIndicator[ i ][ j ] )
				count++;
			indicator = sealIndicator[ i ][ j ];
		}
		if ( count == 0 || count == total ) {
			// All parity chunk is consistent in terms of the seal indicator of data chunk #j
			if ( indicator != trueSealIndicator[ j ] ) {
				assert( trueSealIndicator[ j ] );
				// Remove the j-th entry from chunks
				chunks[ j ] = Coding::zeros;
				trueSealIndicator[ j ] = false;
			}
		}
	}

	// Follow the seal indicators of GET_CHUNK
	for ( uint32_t i = 0; i < parityChunkCount; i++ ) {
		if ( ! chunks[ i + dataChunkCount ] )
			continue;
		for ( uint32_t j = 0; j < dataChunkCount; j++ ) {
			if ( sealIndicator[ i ][ j ] != trueSealIndicator[ j ] ) {
				assert( trueSealIndicator[ j ] );

				// Seal the i-th parity chunk with the j-th data chunk
				for ( uint32_t k = 0; k < dataChunkCount; k++ ) {
					tmpChunks[ k ] = ( k == j ) ? chunks[ j ] : Coding::zeros;
				}

				ChunkUtil::clear( tmpParityChunk );

				coding->encode(
					tmpChunks, tmpParityChunk, i,
					0, ChunkUtil::chunkSize
				);

				char *parity = ChunkUtil::getData( chunks[ i + dataChunkCount ] );
				Coding::bitwiseXOR(
					parity,
					parity,
					ChunkUtil::getData( tmpParityChunk ),
					ChunkUtil::chunkSize
				);
				sealIndicator[ i ][ j ] = true;

				fixedDataCount++;
			}
		}
	}

	delete[] tmpChunks;

	return fixedDataCount;
}
