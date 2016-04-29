#include <cstdlib>
#include <vector>
#include "cauchycoding.hh"
#include "../ds/chunk_pool.hh"

extern "C" {
#include "../../lib/jerasure/include/galois.h"
#include "../../lib/jerasure/include/cauchy.h"
#include "../../lib/jerasure/include/jerasure.h"
}

#define CAUCHY_W_LIMIT (32)

CauchyCoding::CauchyCoding( uint32_t k, uint32_t m, uint32_t chunkSize ) {
	this->_jmatrix = 0;
	this->_jbitmatrix = 0;
	this->_jschedule = 0;

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

CauchyCoding::~CauchyCoding() {
	free( this->_jmatrix );
	free( this->_jbitmatrix );
	free( this->_jschedule );
}

void CauchyCoding::encode( Chunk **dataChunks, Chunk *parityChunk, uint32_t index, uint32_t startOff, uint32_t endOff ) {
	uint32_t k = this->_k;
	uint32_t m = this->_m;
	uint32_t w = this->_w;
	uint32_t chunkSize = this->_chunkSize;

	if ( this->_jmatrix == NULL )
		generateCodeMatrix();

	int** schedule = this->_jschedule;
	char** data = new char* [ k ];
	char** code = new char* [ m ];

	// use local buffer for encoding correctness
	char* chunk = new char [ m * chunkSize ];

	for ( uint32_t idx = 0 ; idx < k + m ; idx ++ ) {
		if ( idx < k ) {
			data[ idx ] = ChunkUtil::getData( dataChunks[ idx ] );
		} else if ( idx - k == index - 1 ) {
			code[ idx - k ] = ChunkUtil::getData( parityChunk );
		} else {
			code[ idx - k ] = chunk + ( idx - k ) * chunkSize;
		}
	}

	// encode
	jerasure_schedule_encode( k, m, w, schedule, data, code, chunkSize, chunkSize / w );

	delete [] data;
	delete [] code;
	delete [] chunk;
}

bool CauchyCoding::decode( Chunk **chunks, BitmaskArray * chunkStatus ) {
	uint32_t k = this->_k;
	uint32_t m = this->_m;
	uint32_t w = this->_w;
	uint32_t chunkSize = this->_chunkSize;

	if ( this->_jmatrix == NULL )
		generateCodeMatrix();

	int* bitmatrix = this->_jbitmatrix;

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

	int *erasures = new int [ failed + 1 ];
	int pos = 0;

	char** data = new char* [ k ];
	char** code = new char* [ m ];

	for ( uint32_t idx = 0 ; idx < k + m ; idx ++ ) {
		if ( idx < k )
			data[ idx ] = ChunkUtil::getData( chunks[ idx ] );
		else
			code[ idx - k ] = ChunkUtil::getData( chunks[ idx ] );

		if ( chunkStatus->check( idx ) == 0 ) {
			erasures[ pos++ ] = idx;
		}
	}

	// required for jerasure
	erasures[ failed ] = -1;

	// decode
	jerasure_schedule_decode_lazy( k, m, w, bitmatrix, erasures, data, code,
			chunkSize, chunkSize / w, 1 );

	delete [] erasures;
	delete [] data;
	delete [] code;

	return false;
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

}
