#include <cstdlib>
#include <vector>
#include "rscoding.hh"

#ifndef USE_ISAL
extern "C" {
#include "../../lib/jerasure/include/galois.h"
#include "../../lib/jerasure/include/reed_sol.h"
#include "../../lib/jerasure/include/jerasure.h"
}
typedef char dataType;
#else
#include "../../lib/isa-l-2.14.0/include/erasure_code.h"
typedef unsigned char dataType;
#endif

#define RS_W_LIMIT (32)

RSCoding::RSCoding( uint32_t k, uint32_t m, uint32_t chunkSize ) {

	this->_k = k;
	this->_m = m;
	this->_chunkSize = chunkSize;

	if ( k + m > RS_N_MAX ) {
		fprintf( stderr, "Only support N up to %u for RS coding.\n", RS_N_MAX );
		exit( -1 );
	}

#ifndef USE_ISAL
	this->_w = getW();
	// force init on gfp_array for w = { 8, 16, 32 }
	galois_single_divide( 10, 2 , 8 );
	galois_single_divide( 10, 2 , 16 );
	galois_single_divide( 10, 2 , 32 );

#endif

	// preallocate the matrix and schedule used by jerasure
	generateCodeMatrix();
}

RSCoding::~RSCoding() {
#ifndef USE_ISAL
	free( this->_jmatrix );
#endif
}

void RSCoding::encode( Chunk **dataChunks, Chunk *parityChunk, uint32_t index, uint32_t startOff, uint32_t endOff ) {
	uint32_t k = this->_k;
	uint32_t m = this->_m;
	uint32_t chunkSize = this->_chunkSize;

#ifndef USE_ISAL
	uint32_t w = this->_w;

	if ( this->_jmatrix == NULL )
		generateCodeMatrix();

	int* matrix = this->_jmatrix;
#endif

	dataType *data[ RS_N_MAX ], *code[ RS_N_MAX ];

	// use local buffer for encoding correctness
	dataType *chunk = new dataType[ m * chunkSize ];

	for ( uint32_t idx = 0 ; idx < k + m ; idx ++ ) {
		if ( idx < k ) {
			data[ idx ] = ( dataType * ) dataChunks[ idx ]->getData();
		} else if ( idx - k == index - 1 ) {
			code[ idx - k ] = ( dataType * ) parityChunk->getData();
		} else {
			code[ idx - k ] = chunk + ( idx - k ) * chunkSize;
		}
	}

	// encode
#ifdef USE_ISAL
	ec_encode_data( chunkSize, k, m, this->_gftbl, data, code );
#else
	jerasure_matrix_encode( k, m, w, matrix, data, code, chunkSize );
#endif

	delete chunk;
}

bool RSCoding::decode( Chunk **chunks, BitmaskArray * chunkStatus ) {
	uint32_t k = this->_k;
	uint32_t m = this->_m;
#ifndef USE_ISAL
	uint32_t w = this->_w;
#endif
	uint32_t chunkSize = this->_chunkSize;

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

	int erasures[ RS_N_MAX ];
	int pos = 0;
	dataType *data[ RS_N_MAX ], *code[ RS_N_MAX ]; 
#ifdef USE_ISAL
	int rpos = 0;
	dataType *alive[ RS_N_MAX ], *missing[ RS_N_MAX ];
#endif

	for ( uint32_t idx = 0 ; idx < k + m ; idx ++ ) {
		if ( idx < k )
			data[ idx ] = ( dataType * ) chunks[ idx ]->getData();
		else
			code[ idx - k ] = ( dataType * ) chunks[ idx ]->getData();

#ifdef USE_ISAL
		if ( chunkStatus->check( idx ) == 0 ) {
			erasures[ pos ] = idx;
			missing[ pos++ ] = ( idx < k )? data[ idx ] : code[ idx - k ];
		} else {
			alive[ rpos++ ] = ( idx < k )? data[ idx ] : code[ idx - k ];
		}
#else
		if ( chunkStatus->check( idx ) == 0 ) {
			erasures[ pos++ ] = idx;
		}
#endif
		
	}

	// required for jerasure
	erasures[ failed ] = -1;

	// decode
#ifdef USE_ISAL
	dataType decodeMatrix[ RS_N_MAX * RS_N_MAX ];
	dataType invertedMatrix[ RS_N_MAX * RS_N_MAX ];
	dataType gftbl[ RS_N_MAX * RS_N_MAX ];
	// get the row where data is alive
	for ( uint32_t i = 0, pos = 0, oi = 0; i < k+m; i++ ) {
		if ( ( int ) i != erasures[ pos ] ) {
			memcpy( decodeMatrix + k * oi, this->_encodeMatrix + k * i, k );
			oi++;
		} else if ( ( int ) i == erasures[ pos ] ) {
			pos++;
		}
	}
	// get the inverse of the matrix of alive data
	if ( gf_invert_matrix( decodeMatrix, invertedMatrix, k ) < 0 ) {
		fprintf( stderr, "Cannot find the inverse for decoding ...\n" );
		return false;
	}
	memset( decodeMatrix, 0, RS_N_MAX * RS_N_MAX );
	for ( uint32_t i = 0; i < failed; i++ ) {
		memcpy( decodeMatrix + k * i, invertedMatrix + k * erasures[ i ], k );
	}
	ec_init_tables( k, failed, decodeMatrix, gftbl );
	ec_encode_data( chunkSize, k, failed, gftbl, alive, missing );
#else
	if ( this->_jmatrix == NULL )
		generateCodeMatrix();

	jerasure_matrix_decode( k, m, w, this->_jmatrix, 1, erasures, data, code,
			chunkSize);
#endif

	return true;
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

#ifdef USE_ISAL
	gf_gen_rs_matrix( this->_encodeMatrix, m+k, k );
	ec_init_tables( k, m, &this->_encodeMatrix[ k * k ], this->_gftbl );
#else
	uint32_t w = this->_w;

	if ( this->_jmatrix != NULL )
		free( this->_jmatrix );

	this->_jmatrix = reed_sol_vandermonde_coding_matrix( k, m, w );
	if ( this ->_jmatrix == NULL ) {
		fprintf( stderr, "No coding matrix can be generated with k=%d, m=%d, w=%d\n",
				k, m, w );
		exit( -1 );
	}
#endif
}
