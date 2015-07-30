#include <cstdio>
#include <cstdlib>
#include <cstring>
#include "rdpcoding.hh"

extern "C" {
#include "../../lib/jerasure/include/galois.h"
}

const uint32_t RDPCoding::primeList[ primeCount ] = {
            2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 
            31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 
            73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 
            127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 
            179, 181, 191, 193, 197, 199};


RDPCoding::RDPCoding( uint32_t k, uint32_t chunkSize ) {
    this->_raid5Coding = new Raid5Coding2( k, chunkSize );
    this->_k = k;
    this->_chunkSize = chunkSize;
    this->_symbolSize = getSymbolSize();
}

RDPCoding::~RDPCoding() {
}

void RDPCoding::encode( Chunk **dataChunks, Chunk *parityChunk, uint32_t index ) {

    uint32_t k = this->_k;
    uint32_t p = this->_p;
    uint32_t chunkSize = this->_chunkSize;
    uint32_t symbolSize = this->_symbolSize;

    // first parity
    if ( index == 1 ) {

        this->_raid5Coding->encode( dataChunks, parityChunk, index );

    } else if ( index == 2 ) {

        // need the row parity for encoding the diagonal parity
        Chunk firstParity;
        firstParity.data = new char[ chunkSize ];

        this->_raid5Coding->encode( dataChunks, &firstParity, index );
        
        // XOR symbols for diagonal parity, assume 
        //  (0)  (1)   (2)   (3)   (4)   .....   (p)  (p+1)
        // -------------------------------------
        // D_0 | D_1 | D_2 | D_3 | P_r | [0] ... [0] | P_d
        // [0]      ...      [0] | [0]   ...     [0] | P_d
        for ( uint32_t cidx = 0 ; cidx < k + 1 ; cidx ++ ) {
            // symbols within each data chunk
            for ( uint32_t sidx = 0 ; sidx < p - 1 ; sidx ++ ) {
                uint32_t pidx = ( cidx + sidx ) % p;

                // missing diagonal
                if ( pidx == p - 1 )
                    continue;

                uint32_t len = symbolSize;
                // last symbol of each chunk is assume to be packed with zeros, 
                // i.e. can be ignored in XOR operations
                if ( sidx == p - 2 )  
                    len = chunkSize - sidx * symbolSize;

                //fprintf( stderr, " encode (%d, %d) on (%d, %d) with len %d \n", cidx, sidx , p - 1, pidx, len );
                if ( cidx < k ) 
                    galois_region_xor ( dataChunks[ cidx ]->data + sidx * symbolSize , 
                            parityChunk->data + pidx * symbolSize , len );
                else 
                    galois_region_xor ( firstParity.data + sidx * symbolSize , 
                            parityChunk->data + pidx * symbolSize , len );
            }
        }

        delete firstParity.data;

    } else {
        // ignored
    }

}

bool RDPCoding::decode( Chunk **chunks, BitmaskArray *chunkStatus ) {

    uint32_t k = this->_k;
    uint32_t p = this->_p;
    uint32_t chunkSize = this->_chunkSize;
    uint32_t symbolSize = this->_symbolSize;
    std::vector< uint32_t > failed;

    // check for failed disk
    for ( uint32_t idx = 0; idx < k ; idx ++ ) {
        if ( chunkStatus->check( idx ) == 0 ) {
            if ( failed.size() < 2 )
                failed.push_back( idx );
            else
                return false;
        }
    }

    // no data lost for decode
    if ( failed.size() == 0 ) 
        return false;

    uint32_t recoveredSymbolCount = 0;

    if ( failed.size() == 1 ) {
        // TODO : optimize for single failure recovery

        // diagonal parity, or data/row parity
        if ( failed[ 0 ] == k + 1 )
            encode( chunks, chunks[ k + 1 ], 2 );
        else
            this->_raid5Coding->decode ( chunks, chunkStatus );

    } else if ( failed[ 1 ] == k + 1 ) {

        // data/row parity + diagonal parity
        this->_raid5Coding->decode ( chunks, chunkStatus );
        encode( chunks, chunks[ k + 1 ], 2 );

    } else {

        // zero out the chunks for XOR 
        for ( uint32_t idx = 0 ; idx < failed.size() ; idx ++ ) {
            memset( chunks[ failed [ idx ] ]->data, 0, chunkSize );
        }

        uint32_t chunkToRRepair = failed[ 0 ];
        uint32_t chunkToDRepair = failed[ 1 ];

        // avoid the missing diagonal
        if ( failed[ 0 ] == 0 ) {
            chunkToDRepair = failed[ 0 ];
            chunkToRRepair = failed[ 1 ];
        }
        uint32_t didxToRepair = chunkToRRepair - 1;
        uint32_t sidxToRepair =  ( didxToRepair + p - chunkToDRepair ) % p;

        while ( recoveredSymbolCount < ( p - 1 ) * failed.size() ) {

            uint32_t sidx, len;
        
            //fprintf( stderr, "repair symbol (%d,%d) (%d,%d)\n", chunkToDRepair, 
            //        sidxToRepair, chunkToRRepair, sidxToRepair );

            // xor in both diagonal and row for data and row parities
            for ( uint32_t cidx = 0 ; cidx < k + 1 ; cidx ++ ) {

                // skip the symbol to repair in the diagonal direction
                if ( cidx == chunkToDRepair ) 
                    continue;

                // figure out the symbol id contribute to the diagonal parity
                sidx = ( didxToRepair + p - cidx ) % p;

                // diagonal, max. idx of symbols (within a chunk) is p-2
                if ( sidx != p - 1 ) {
                    len = ( sidx == p - 2 || sidxToRepair == p - 2 ) ? 
                            chunkSize - (p - 2) * symbolSize : symbolSize;
                    galois_region_xor ( chunks[ cidx ]->data + sidx * symbolSize, 
                            chunks[ chunkToDRepair ]->data + sidxToRepair * symbolSize, len );
                }

                // skip the symbol to repair in the row direction
                if ( cidx == chunkToRRepair )
                    continue;

                // row, XOR symbols in the same row
                sidx = sidxToRepair;
                len = ( sidx == p - 2 ) ? 
                        chunkSize - (p - 2) * symbolSize : symbolSize;
                galois_region_xor ( chunks[ cidx ]->data + sidxToRepair * symbolSize,
                        chunks[ chunkToRRepair ]->data + sidxToRepair * symbolSize, len );

            }

            // diagonal, XOR the diagonal parity
            len = ( sidxToRepair == p - 2 || didxToRepair == p - 2 ) ? 
                    chunkSize - (p - 2) * symbolSize : symbolSize;
            galois_region_xor ( chunks[ k + 1 ]->data + didxToRepair * symbolSize,
                    chunks[ chunkToDRepair ]->data + sidxToRepair * symbolSize, len);
            //fprintf( stderr, " decode (%d, %d) on (%d, %d) with len %d \n", k + 1, didxToRepair , chunkToDRepair , sidxToRepair, len );

            // row, add back the just recovered symbol from diagonal decoding
            len = ( sidxToRepair == p - 2 ) ? 
                    chunkSize - (p - 2) * symbolSize : symbolSize;
            galois_region_xor ( chunks[ chunkToDRepair ]->data + sidxToRepair * symbolSize,
                    chunks[ chunkToRRepair ]->data + sidxToRepair * symbolSize, len );
            //fprintf( stderr, " decode (%d, %d) on (%d, %d) with len %d \n", chunkToDRepair, sidxToRepair , chunkToRRepair , sidxToRepair, len );

            recoveredSymbolCount += 2;       

            // search for next symbol to recover
            didxToRepair = ( chunkToRRepair + sidxToRepair ) % p;
            sidxToRepair = ( didxToRepair + p - chunkToDRepair ) % p;
            // avoid missing diagonal
            if ( didxToRepair == p - 1 ) {
                std::swap( chunkToRRepair, chunkToDRepair );
                didxToRepair = chunkToRRepair - 1;
                sidxToRepair = ( didxToRepair + p - chunkToDRepair ) % p;
            }
        }

    }

    return true;
}

uint32_t RDPCoding::getPrime() {
    uint32_t k = this->_k;
    uint32_t start = 0, end = primeCount - 1, mid = ( start + end ) / 2;

    // binary search on the list of prime numbers
    while (start < end) {
        mid = ( start + end ) / 2;

        if ( primeList [ mid ] < k + 1 ) {
            start = mid;
        } 
        if ( mid + 1 < primeCount && primeList [ mid + 1 ] >= k + 1 ) {
            end  = mid + 1;
        } 
        if ( start + 1 == end ) {
            this->_p = end;
            break;
        }
    }

    return this->_p;
}

uint32_t RDPCoding::getSymbolSize() {
    uint32_t pp = getPrime();
    uint32_t chunkSize = this->_chunkSize;

    // p - 1 symbols per chunk
    // cannot support chunk sizes which are not multiples of ( p - 1 )
    // since any remaining bytes will not be stored
    while ( chunkSize % ( primeList[ pp ] - 1 ) && pp < primeCount ) {
        pp ++;
    }

    if ( pp == primeCount ) {
        fprintf( stderr, "Cannot find a prime number p > k+1 such that the size of chunk, %d, is a multiple of (p - 1)\n", chunkSize);
        exit( -1 );
    }
    
    this->_p = primeList[ pp ];
    this->_symbolSize = ( this->_chunkSize ) / ( this->_p - 1 );

    //fprintf( stderr, "symbol size %d\n", this->_symbolSize );
    //fprintf( stderr, "p %d\n", this->_p );

    return this->_symbolSize;
}

