#include <cstdlib>
#include <vector>
#include "rscoding.hh"

extern "C" {
#include "../../lib/jerasure/include/galois.h"
#include "../../lib/jerasure/include/reed_sol.h"
#include "../../lib/jerasure/include/jerasure.h"
}

#define RS_W_LIMIT (32)

RSCoding::RSCoding( uint32_t k, uint32_t m, uint32_t chunkSize ) {
    // force init on gfp_array for w = { 8, 16, 32 }
    galois_single_divide( 10, 2 , 8 );
    galois_single_divide( 10, 2 , 16 );
    galois_single_divide( 10, 2 , 32 );
    
    this->_k = k;
    this->_m = m;
    this->_chunkSize = chunkSize;
    this->_w = getW();

    // preallocate the matrix and schedule used by jerasure
    generateCodeMatrix();
}

RSCoding::~RSCoding() {
    free( this->_jmatrix );
}

void RSCoding::encode( Chunk **dataChunks, Chunk *parityChunk, uint32_t index, uint32_t startOff, uint32_t endOff ) {
    uint32_t k = this->_k;
    uint32_t m = this->_m;
    uint32_t w = this->_w;
    uint32_t chunkSize = this->_chunkSize;

    if ( this->_jmatrix == NULL ) 
        generateCodeMatrix();

    int* matrix = this->_jmatrix;
    char** data = new char* [ k ];
    char** code = new char* [ m ];

    // use local buffer for encoding correctness
    char* chunk = new char [ m * chunkSize ];

    for ( uint32_t idx = 0 ; idx < k + m ; idx ++ ) {
        if ( idx < k ) {
            data[ idx ] = dataChunks[ idx ]->data;
        } else if ( idx - k == index - 1 ) {
            code[ idx - k ] = parityChunk->data;
        } else {
            code[ idx - k ] = chunk + ( idx - k ) * chunkSize;
        }
    }

    // encode
    jerasure_matrix_encode( k, m, w, matrix, data, code, chunkSize ); 

    delete data;
    delete code;
    delete chunk;
}

bool RSCoding::decode( Chunk **chunks, BitmaskArray * chunkStatus ) {
    uint32_t k = this->_k;
    uint32_t m = this->_m;
    uint32_t w = this->_w;
    uint32_t chunkSize = this->_chunkSize;

    if ( this->_jmatrix == NULL ) 
        generateCodeMatrix();

    int* matrix = this->_jmatrix;

    uint32_t failed = 0;
    for ( uint32_t idx = 0 ; idx < k + m ; idx ++ ) {
        if ( chunkStatus->check( idx ) ==  0) {
            failed++;
        }
    }

    if ( failed > m ) {
        fprintf ( stderr, "RS coding: Too many failure to recover (%d>%d)!!\n", 
                failed, m );
        return false;
    }

    if ( failed == 0 ) {
        return true;
    }

    int *erasures = new int[ failed + 1 ];
    int pos = 0;
    
    char** data = new char* [ k ];
    char** code = new char* [ m ];

    for ( uint32_t idx = 0 ; idx < k + m ; idx ++ ) {
        if ( idx < k )
            data[ idx ] = chunks[ idx ]->data;
        else
            code[ idx - k ] = chunks[ idx ]->data;

        if ( chunkStatus->check( idx ) == 0 ) {
            erasures[ pos++ ] = idx;
        }
    }

    // required for jerasure
    erasures[ failed ] = -1;
    
    // decode
    jerasure_matrix_decode( k, m, w, matrix, 1, erasures, data, code, 
            chunkSize);

    delete [] erasures;
    delete [] data;
    delete [] code;

    return false;
}

uint32_t RSCoding::getW() {
    uint32_t w = 1;
    // k + m >= 2 ^ w
    while ( ( uint32_t ) ( 0x1 << w ) < this->_k + this->_m ) {
        w++;
    }

    // find the smallest w = { 8,16,32 } 
    // TODO : allow more flexibility on the choices of w, e.g. by padding chunks with zeros
    if ( w < 8 ) {
        w = 8;
    } else if ( w < 16 ) {
        w = 16;
    } else if ( w < 32 ) {
        w = 32;
    } else {
        fprintf( stderr, "k + m is too large to fit any possible w < 32, k=%d, m=%d, w=%d\n", 
                this->_k, this->_m, w );
        exit( -1 );
    }

    if ( this->_chunkSize % w ) {
        fprintf( stderr, "chunkSize is not a multiple of %d bytes is of supported\n", w );
        exit( -1 );
    }

    // TODO : avoid bad choice of w
    this->_w = w;
    //fprintf( stderr, "RS: w = %d\n", w );

    return this->_w;
}

void RSCoding::generateCodeMatrix() {
    uint32_t k = this->_k;
    uint32_t m = this->_m;
    uint32_t w = this->_w;

    if ( this->_jmatrix != NULL )
        free( this->_jmatrix );

    this->_jmatrix = reed_sol_vandermonde_coding_matrix( k, m, w );
    if ( this ->_jmatrix == NULL ) {
        fprintf( stderr, "No coding matrix can be generated with k=%d, m=%d, w=%d\n", 
                k, m, w );
        exit( -1 );
    }

}
