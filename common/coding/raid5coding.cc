#include <stdio.h>
#include "raid5coding.hh"
#include "../ds/chunk_pool.hh"

bool RAID5Coding::init( uint32_t n ) {
	this->n = n;
#ifdef USE_ISAL
	gf_gen_raid5_matrix( this->_encodeMatrix, n, n-1 );
	ec_init_tables( n - 1, n, &this->_encodeMatrix[ ( n - 1 ) * ( n - 1 ) ], this->_gftbl );
#endif
	return true;
}

void RAID5Coding::encode( Chunk **data, Chunk *parity, uint32_t index, uint32_t startOff, uint32_t endOff ) {
#ifdef USE_ISAL
	dataType *dataBuf[ RAID5_N_MAX - 1 ], *codeBuf[ 1 ];
	for ( uint32_t i = 0; i < this->n - 1; i++ ) {
		dataBuf[ i ] = ( dataType * ) ChunkUtil::getData( data[ i ] );
	}
	codeBuf[ 0 ] = ( dataType * ) ChunkUtil::getData( parity );
	if ( startOff == 0 && endOff == 0 ) {
		ec_encode_data( ChunkUtil::chunkSize, this->n - 1, 1, this->_gftbl, dataBuf, codeBuf );
	} else {
		for ( uint32_t i = startOff / ChunkUtil::chunkSize; i <= endOff / ChunkUtil::chunkSize; i++ ) {
			// note: the update is in-place "xor"ed on parityChunk
			ec_encode_data_update( ChunkUtil::chunkSize, this->n - 1, 1, i, this->_gftbl, dataBuf[ i ], codeBuf );
		}
	}
#else
	for ( uint32_t i = 0; i < this->n - 1; i++ ) {
		this->bitwiseXOR(
			parity,
			parity,
			data[ i ],
			ChunkUtil::chunkSize
		);
	}
#endif
}

bool RAID5Coding::decode( Chunk **chunks, BitmaskArray *bitmap ) {
	uint32_t lostIndex = 0;
	uint32_t failed = 0;
#ifdef USE_ISAL
	dataType *alive[ RAID5_N_MAX ], *missing[ 1 ];
	uint32_t rpos = 0;
#endif

	// Check which chunk is lost
	for ( uint32_t i = 0; i < this->n; i++ ) {
		if ( ! bitmap->check( i ) ) {
			lostIndex = i;
			failed++;
			// Check whether there are more lost chunks
			if ( failed > 1 )
				return false;
#ifdef USE_ISAL
			missing[ 0 ] = ( dataType * ) ChunkUtil::getData( chunks[ i ] );
		} else {
			alive[ rpos++ ] = ( dataType * ) ChunkUtil::getData( chunks[ i ] );
#endif
		}
	}

#ifdef USE_ISAL
	dataType decodeMatrix[ RAID5_N_MAX * RAID5_N_MAX ];
	dataType invertedMatrix[ RAID5_N_MAX * RAID5_N_MAX ];
	dataType gftbl[ RAID5_N_MAX * RAID5_N_MAX * 32 ];
	// get the row where data is alive
	for ( uint32_t i = 0, oi = 0; i < this->n; i++ ) {
		if ( i != lostIndex ) {
			memcpy( decodeMatrix + ( this->n - 1 ) * oi, this->_encodeMatrix + ( this->n - 1 ) * i, ( this->n - 1 ) );
			oi++;
		}
	}
	// get the inverse of the matrix of alive data
	if ( gf_invert_matrix( decodeMatrix, invertedMatrix, ( this->n - 1 ) ) < 0 ) {
		fprintf( stderr, "Cannot find the inverse for decoding ...\n" );
		return false;
	}

	memset( decodeMatrix, 0, RAID5_N_MAX * RAID5_N_MAX );
	memcpy( decodeMatrix, invertedMatrix + ( this->n - 1 ) * lostIndex, ( this->n - 1 ) );

	ec_init_tables( this->n - 1, failed, decodeMatrix, gftbl );
	ec_encode_data( ChunkUtil::chunkSize, n - 1, failed, gftbl, alive, missing );
#else
	// Reconstruct the lost chunk
	Chunk *lostChunk = chunks[ lostIndex ];
	for ( uint32_t i = 0; i < this->n; i++ ) {
		if ( i == lostIndex )
			continue;
		this->bitwiseXOR(
			lostChunk,
			lostChunk,
			chunks[ i ],
			ChunkUtil::chunkSize
		);
	}
#endif

	return true;
}
