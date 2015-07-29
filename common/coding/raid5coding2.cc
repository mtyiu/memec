#include "raid5coding2.hh"
#include "../config/global_config.hh"

extern "C" {
#include "../../lib/jerasure/include/galois.h"
}

Raid5Coding2::Raid5Coding2( uint32_t k, uint32_t chunkSize ) {
    // force init on gfp_array for w = { 8, 16, 32 }
    galois_single_divide( 10, 2 , 8 );
    galois_single_divide( 10, 2 , 16 );
    galois_single_divide( 10, 2 , 32 );

    this->_k = k;
    this->_chunkSize = chunkSize;
}

Raid5Coding2::~Raid5Coding2() {
}

void Raid5Coding2::encode( Chunk **dataChunks, Chunk *parityChunk, uint32_t index ) {
    uint32_t k = this->_k; 
    uint32_t chunkSize = this->_chunkSize; 

    // cannot perform encoding
    if ( k < 2 || chunkSize <= 0 ) 
        return;

    // XOR all the chunks 
    for ( uint32_t idx = 0; idx < k; idx++ ) {
        //fprintf( stderr, "XOR disk %d %d bytes [%x] on [%x]\n", idx , chunkSize, *(dataChunks[ idx ]->data), *(parityChunk->data) );
        if ( idx == 0 ) 
            memcpy( parityChunk->data, dataChunks[idx]->data, chunkSize );
        else 
            galois_region_xor ( dataChunks[idx]->data, parityChunk->data, chunkSize );
    }
}

bool Raid5Coding2::decode( Chunk **chunks, BitmaskArray *chunkStatus ) {
    uint32_t k = this->_k; 
    uint32_t chunkSize = this->_chunkSize; 
    uint32_t failed = ( uint32_t ) -1;

    // cannot perform decoding
    if ( k < 2 || chunkSize <= 0 ) 
        return false;

    // check if the data chunk lost can be recovered
    for ( uint32_t idx = 0; idx < k; idx++ ) {
        if ( chunkStatus->check(idx) == 0 ) {
            if ( failed == ( uint32_t ) -1 )
                failed = idx;
            else
                return false;
        }
    }

    // no data chunk is lost
    if ( failed == ( uint32_t ) -1 ) return false;

    // decode for the lost chunk
    memset( chunks[ failed ]->data, 0, chunkSize );

    for ( uint32_t idx = 0; idx < k + 1; idx++ ) {
        if ( idx == failed ) 
            continue;
        //fprintf( stderr, "XOR disk %d %d bytes [%x] on [%x]\n", idx , chunkSize, *(chunks[ idx ]->data), *(chunks[ failed ]->data) );
        galois_region_xor ( chunks[ idx ]->data, chunks[ failed ]->data, chunkSize );
    }

    return true;
}
