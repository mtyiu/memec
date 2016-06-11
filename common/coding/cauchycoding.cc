#include <cstdlib>
#include <vector>
#include "cauchycoding.hh"
#include "../ds/chunk_pool.hh"

#ifndef USE_ISAL
extern "C" {
#include "../../lib/jerasure/include/galois.h"
#include "../../lib/jerasure/include/cauchy.h"
#include "../../lib/jerasure/include/jerasure.h"
}
typedef char dataType;
#else
#include "../../lib/isa-l-2.14.0/include/erasure_code.h"
typedef unsigned char dataType;
#endif

#define CAUCHY_W_LIMIT (32)

CauchyCoding::CauchyCoding( uint32_t k, uint32_t m, uint32_t chunkSize ) {
	this->_k = k;
	this->_m = m;
	this->_chunkSize = chunkSize;
#ifndef USE_ISAL
	this->_w = getW();

	// force init on gfp_array for w = { 8, 16, 32 }
	galois_single_divide( 10, 2 , 8 );
	galois_single_divide( 10, 2 , 16 );
	galois_single_divide( 10, 2 , 32 );
	
	this->_jmatrix = 0;
	this->_jbitmatrix = 0;
	this->_jschedule = 0;
#endif

	// preallocate the matrix and schedule used by jerasure
	generateCodeMatrix();
}

CauchyCoding::~CauchyCoding() {
#ifndef USE_ISAL
	free( this->_jmatrix );
	free( this->_jbitmatrix );
	free( this->_jschedule );
#endif
}

void CauchyCoding::encode( Chunk **dataChunks, Chunk *parityChunk, uint32_t index, uint32_t startOff, uint32_t endOff ) {
	uint32_t k = this->_k;
	uint32_t m = this->_m;
	uint32_t chunkSize = this->_chunkSize;

#ifndef USE_ISAL
	uint32_t w = this->_w;

	if ( this->_jmatrix == NULL )
		generateCodeMatrix();

	int** schedule = this->_jschedule;
#endif
	dataType *data[ CRS_N_MAX ], *code[ CRS_N_MAX ];

	// use local buffer for encoding correctness
	dataType *chunk = new dataType [ m * chunkSize ];

	for ( uint32_t idx = 0 ; idx < k + m ; idx ++ ) {
		if ( idx < k ) {
			data[ idx ] = ( dataType * ) ChunkUtil::getData( dataChunks[ idx ] );
		} else if ( idx - k == index - 1 ) {
			code[ idx - k ] = ( dataType * ) ChunkUtil::getData( parityChunk );
		} else {
			code[ idx - k ] = chunk + ( idx - k ) * chunkSize;
		}
	}

	// encode
#ifdef USE_ISAL
	if ( startOff == 0 && endOff == 0 ) {
		ec_encode_data( chunkSize, k, m, this->_gftbl, data, code );
	} else {
		for ( uint32_t i = startOff / chunkSize; i <= endOff / chunkSize; i++ ) {
			// note: the update is in-place "xor"ed on parityChunk
			ec_encode_data_update( chunkSize, k, m, i, this->_gftbl, data[ i ], code );
		}
	}
#else
	jerasure_schedule_encode( k, m, w, schedule, data, code, chunkSize, chunkSize / w );
#endif

	delete chunk;
}

bool CauchyCoding::decode( Chunk **chunks, BitmaskArray * chunkStatus ) {
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
		fprintf ( stderr, "Cauchy coding: Too many failure to recover (%d>%d)!!\n",
				failed, m );
		return false;
	}

	if ( failed == 0 ) {
		return true;
	}

	int erasures[ CRS_N_MAX ];
	int pos = 0;
	dataType *data[ CRS_N_MAX ], *code[ CRS_N_MAX ];
#ifdef USE_ISAL
	int rpos = 0;
	dataType *alive[ CRS_N_MAX ], *missing[ CRS_N_MAX ];
#endif

	for ( uint32_t idx = 0 ; idx < k + m ; idx ++ ) {
		if ( idx < k )
			data[ idx ] = ( dataType * ) ChunkUtil::getData( chunks[ idx ] );
		else
			code[ idx - k ] = ( dataType * ) ChunkUtil::getData( chunks[ idx ] );

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
	dataType decodeMatrix[ CRS_N_MAX * CRS_N_MAX ];
	dataType invertedMatrix[ CRS_N_MAX * CRS_N_MAX ];
	dataType gftbl[ CRS_N_MAX * CRS_N_MAX * 32 ];
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
	memset( decodeMatrix, 0, CRS_N_MAX * CRS_N_MAX );
	for ( uint32_t i = 0; i < failed; i++ ) {
		memcpy( decodeMatrix + k * i, invertedMatrix + k * erasures[ i ], k );
	}
	ec_init_tables( k, failed, decodeMatrix, gftbl );
	ec_encode_data( chunkSize, k, failed, gftbl, alive, missing );
#else
	if ( this->_jmatrix == NULL )
		generateCodeMatrix();

	int* bitmatrix = this->_jbitmatrix;

	jerasure_schedule_decode_lazy( k, m, w, bitmatrix, erasures, data, code,
			chunkSize, chunkSize / w, 1 );
#endif

	return true;
}

uint32_t CauchyCoding::getW() {
	uint32_t w = 1;
	// k + m >= 2 ^ w
	while ( ( uint32_t ) ( 0x1 << w ) < this->_k + this->_m ) {
		w++;
	}

	// find a "w" such that, size of packet * w = chunk size
	// TODO : allow more flexibility on the choices of w, e.g. by padding chunks with zeros
	while ( this->_chunkSize % w ) {
		w++;
		if ( w > CAUCHY_W_LIMIT ) {
			fprintf( stderr, "Cannot find a suitable w for k=%d,m=%d,chunkSize=%u\n",
					this->_k, this->_m, this->_chunkSize );
			exit( -1 );
		}
	}

	// TODO : avoid bad choice of w
	this->_w = w;
	//fprintf( stderr, "CRS: w = %d\n", w );

	return this->_w;
}

void CauchyCoding::generateCodeMatrix() {
	uint32_t k = this->_k;
	uint32_t m = this->_m;

#ifdef USE_ISAL
	gf_gen_cauchy1_matrix( this->_encodeMatrix, m+k, k);
	ec_init_tables( k, m, &this->_encodeMatrix[ k * k ], this->_gftbl );
#else
	uint32_t w = this->_w;

	if ( this->_jmatrix != NULL )
		free( this->_jmatrix );
	if ( this->_jbitmatrix != NULL )
		free( this->_jbitmatrix );
	if ( this->_jschedule != NULL )
		free( this->_jschedule );

	this->_jmatrix = cauchy_good_general_coding_matrix( k, m, w );
	if ( this ->_jmatrix == NULL ) {
		fprintf( stderr, "No coding matrix can be generated with k=%d, m=%d, w=%d\n",
				k, m, w );
		exit( -1 );
	}

	this->_jbitmatrix = jerasure_matrix_to_bitmatrix( k, m, w, this->_jmatrix );

	this->_jschedule = jerasure_smart_bitmatrix_to_schedule( k, m , w, this->_jbitmatrix );
#endif

}
