#include <vector>
#include "evenoddcoding.hh"

EvenOddCoding::EvenOddCoding( uint32_t k, uint32_t chunkSize ) :
		RDPCoding( k, chunkSize ) {
	this->_symbolSize = this->getSymbolSize();
}

EvenOddCoding::~EvenOddCoding() {
}

void EvenOddCoding::encode( Chunk **dataChunks, Chunk *parityChunk, uint32_t index, uint32_t startOff, uint32_t endOff ) {

	uint32_t k = this->_k;
	uint32_t p = this->_p;
	uint32_t chunkSize = this->_chunkSize;
	uint32_t symbolSize = this->_symbolSize;
	uint32_t size = ( endOff == 0 )? 0 : endOff - startOff;

	if ( index == 1 ) {

		// first parity
		this->_raid5Coding->encode( dataChunks, parityChunk, index );

	} else if ( index == 2 ) {

		// the missing diagonal parity S
		char *s = new char [ symbolSize ];
		memset( s, 0, symbolSize );

		// construct the diagonal xor on data symbols, including S
		for ( uint32_t sidx = 0 ; sidx < p - 1 ; sidx ++ ) {
			// symbols within each data chunk
			for ( uint32_t cidx = 0 ; cidx < k ; cidx ++ ) {
				// skip unmodified symbols
				uint32_t symbolOff = cidx * chunkSize + sidx * symbolSize;
				if ( size > 0 && ( symbolOff + symbolSize < startOff || symbolOff > endOff ) )
					continue;

				uint32_t pidx = ( cidx + sidx ) % p;
				uint32_t offset = ( size > 0 )? ( startOff > symbolOff ) ? startOff - symbolOff : 0 : 0;
				uint32_t len = ( size > 0 )? endOff - ( symbolOff + offset ) : symbolSize;
				len = ( len + offset > symbolSize )? symbolSize - offset : len;

				//fprintf( stderr, " encode (%d, %d) on (%d, %d) with off %d len %d \n", cidx, sidx , p - 1, pidx, offset, len );
				// missing diagonal
				if ( pidx == p - 1 ) {
					this->bitwiseXOR( s + offset, dataChunks[ cidx ]->getData() + sidx * symbolSize + offset,
							s + offset, len );
				} else {
					this->bitwiseXOR( parityChunk->getData() + pidx * symbolSize + offset,
							dataChunks[ cidx ]->getData() + sidx * symbolSize + offset,
							parityChunk->getData() + pidx * symbolSize + offset, len );
				}
			}
		}

		// add S back
		for ( uint32_t sidx = 0 ; sidx < p - 1 ; sidx ++ ) {
			this->bitwiseXOR( parityChunk->getData() + sidx * symbolSize, s,
					parityChunk->getData() + sidx * symbolSize, symbolSize );
		}

		delete [] s;
	}

}

bool EvenOddCoding::decode( Chunk **chunks, BitmaskArray *chunkStatus ) {

	uint32_t k = this->_k;
	uint32_t p = this->_p;
	uint32_t chunkSize = this->_chunkSize;
	uint32_t symbolSize = this->_symbolSize;
	std::vector< uint32_t > failed;

	// check for failed disk
	for ( uint32_t idx = 0 ; idx < k + 2 ; idx ++ ) {
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

	// zero out the chunks for XOR
	for ( uint32_t idx = 0 ; idx < failed.size() ; idx ++ ) {
		memset( chunks[ failed[ idx ] ]->getData(), 0, chunkSize );
	}

	char* s;
	// data chunk loss only
	if ( failed.size() == 1 ) {
		// TODO : optimize for single failure recovery

		// diagonal parity, or data/row parity
		if ( failed[ 0 ] == k + 1 )
			this->encode( chunks, chunks[ k + 1 ], 2 );
		else
			this->_raid5Coding->decode( chunks, chunkStatus );

	} else if ( failed[ 1 ] == k + 1 ) {

		this->_raid5Coding->decode( chunks, chunkStatus );
		this->encode( chunks, chunks[ k + 1 ], 2 );

	} else if ( failed[ 1 ] == k ) {
		// missing row parity ... find the diagonal that gives S directly

		// recover S first
		uint32_t pidx =  ( failed[ 0 ] - 1 + p ) % p;
		s = new char [ symbolSize ];
		memset( s, 0, symbolSize );
		// data symbols
		for ( uint32_t cidx = 0 ; cidx < k ; cidx ++ ) {
			if ( cidx == failed[ 0 ] )
				continue;
			uint32_t sidx = ( pidx + p - cidx ) % p;
			this->bitwiseXOR( s, chunks[ cidx ]->getData() + sidx * symbolSize, s , symbolSize );
		}
		// diagonal symbol
		if ( pidx != p - 1 )
			this->bitwiseXOR( s, chunks[ k + 1 ]->getData() + pidx * symbolSize, s , symbolSize );

		// recover data using diagonal parity and S
		for ( uint32_t sidxToRepair = 0 ; sidxToRepair < p - 1 ; sidxToRepair ++ ) {
			char* failedSymbol = chunks[ failed[ 0 ] ]->getData() + sidxToRepair * symbolSize;
			pidx = ( failed[ 0 ] +  sidxToRepair ) % p;
			for ( uint32_t cidx = 0 ; cidx < k ; cidx ++ ) {
				if ( cidx == failed[ 0 ] )
					continue;
				uint32_t sidx = ( pidx + p - cidx ) % p;
				if ( sidx > p - 2 )
					continue;
				this->bitwiseXOR( failedSymbol, chunks[ cidx ]->getData() + sidx * symbolSize,
						failedSymbol, symbolSize );
			}
			if ( pidx != p - 1 )
				this->bitwiseXOR( failedSymbol, chunks[ k + 1 ]->getData() + pidx * symbolSize,
						failedSymbol, symbolSize );
			this->bitwiseXOR( failedSymbol, s, failedSymbol, symbolSize );
		}

		// recover row parity
		this->encode( chunks, chunks[ k ], 1 );

		delete [] s;

	} else {

		// get back S, xor all symbols in chunk[ k + 1 ] and chunk[ k ]
		s = new char [ symbolSize ];
		// first symbol
		memcpy( s, chunks[ k ]->getData(), symbolSize );
		this->bitwiseXOR( s, chunks[ k + 1 ]->getData(), s, symbolSize );
		// remainging symbols
		for ( uint32_t sidx = 1 ; sidx < p - 1 ; sidx ++ ) {
			this->bitwiseXOR( s, chunks[ k ]->getData() + sidx * symbolSize, s, symbolSize );
			this->bitwiseXOR( s, chunks[ k + 1 ]->getData() + sidx * symbolSize, s, symbolSize );
		}

		uint32_t idx = 1;
		uint32_t cidxToRepair = failed[ idx % 2 ];
		uint32_t pidx = ( failed[ ( idx + 1 ) % 2 ] - 1 + p ) % p;
		uint32_t sidxToRepair = ( pidx + p - cidxToRepair ) % p;
		char* symbolToRepair;

		for ( uint32_t recoveredSymbolCount = 0;
				recoveredSymbolCount < ( p - 1 ) * failed.size();
				recoveredSymbolCount += 2 ) {

			// diagonal
			symbolToRepair = chunks[ cidxToRepair ]->getData() + sidxToRepair * symbolSize;
			// data symbols
			for ( uint32_t cidx = 0 ; cidx < k ; cidx ++ ) {
				if ( cidx == cidxToRepair )
					continue;

				uint32_t sidx = ( p + pidx - cidx ) % p;
				if ( sidx == sidxToRepair || sidx == p - 1 )
					continue;

				this->bitwiseXOR( symbolToRepair, chunks[ cidx ]->getData() + sidx * symbolSize,
						symbolToRepair, symbolSize );
			}
			// diagonal parity symbols
			if ( pidx != p - 1 ) {
				this->bitwiseXOR( symbolToRepair, chunks[ k + 1 ]->getData() + pidx * symbolSize,
						symbolToRepair, symbolSize );
			}
			// S
			this->bitwiseXOR( symbolToRepair, s, symbolToRepair, symbolSize );

			// row
			cidxToRepair = failed[ ( idx + 1 ) % 2 ];
			symbolToRepair = chunks[ cidxToRepair ]->getData() + sidxToRepair * symbolSize;

			for ( uint32_t cidx = 0 ; cidx < k + 1 ; cidx ++ ) {
				if ( cidx == cidxToRepair )
					continue;
				this->bitwiseXOR( symbolToRepair, chunks[ cidx ]->getData() + sidxToRepair * symbolSize,
						symbolToRepair, symbolSize );
			}

			// next ("row") symbol
			pidx = ( cidxToRepair + sidxToRepair ) % p;
			cidxToRepair = failed[ idx % 2 ];
			sidxToRepair = ( pidx + p - cidxToRepair ) % p;

		}
		delete [] s;

	}

	return true;
}

uint32_t EvenOddCoding::getPrime() {
	uint32_t k = this->_k;
	uint32_t start = 0, end = primeCount - 1, mid = ( start + end ) / 2;

	// binary search on the list of prime numbers
	while (start < end) {
		mid = ( start + end ) / 2;

		if ( primeList [ mid ] < k ) {
			start = mid;
		}
		if ( primeList [ mid ] >= k ) {
			end  = mid;
		}
		if ( start + 1 == end ) {
			this->_p = ( primeList[ start ] >= k && primeList[ start ] != 2 )? start : end;
			break;
		}
	}

	return this->_p;
}

uint32_t EvenOddCoding::getSymbolSize() {
	uint32_t pp = this->getPrime();
	uint32_t chunkSize = this->_chunkSize;

	// p - 1 symbols per chunk
	// cannot support chunk sizes which are not multiples of ( p - 1 )
	// since any remaining bytes will not be stored
	while ( chunkSize % ( primeList[ pp ] - 1 ) && pp < primeCount ) {
		pp ++;
	}

	if ( pp == primeCount ) {
		fprintf( stderr, "Cannot find a prime number p > k such that the size of chunk, %d, is a multiple of (p - 1)\n", chunkSize);
		exit( -1 );
	}

	this->_p = primeList[ pp ];
	this->_symbolSize = ( this->_chunkSize ) / ( this->_p - 1 );

	//fprintf( stderr, "symbol size %d\n", this->_symbolSize );
	//fprintf( stderr, "p %d\n", this->_p );

	return this->_symbolSize;
}
