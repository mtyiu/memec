#ifndef __COMMON_CODING_CODING_HH__
#define __COMMON_CODING_CODING_HH__

#include <stdint.h>
#include "../ds/chunk.hh"
#include "../ds/bitmask_array.hh"

class Coding {
	/**
	 * Encoding k data chunks to generate 1 parity chunk.
	 *
	 * E.g., for a (6, 4)-code,
	 * data[ 0 ] --> 1st data chunk
	 * data[ 1 ] --> 2nd data chunk
	 * data[ 2 ] --> 3rd data chunk
	 * data[ 3 ] --> 4th data chunk
	 * parity --> buffer for storing the (index)-th parity chunk
	 *
	 * @param data   array of data chunks (assumed to have k entries)
	 * @param parity buffer for storing the encoded parity chunk
	 * @param index  indicate which parity chunk should be generated (e.g., index = 2 if the 2nd parity chunk is needed)
	 */
	virtual void encode( Chunk **data, Chunk *parity, uint32_t index ) = 0;

	/**
	 * Decode k data/parity chunks.
	 * 
	 * @param  chunks array of data and parity chunks (assumed to have n = k + m entries); NULL if the chunk is lost and need to be recovered
	 * @param  bitmap indicate which entries in the chunks array are available
	 * @return        indicate whether the decoding is successful
	 */
	virtual bool decode( Chunk **chunks, BitmaskArray *bitmap ) = 0;
};

/**
 * Q: How to check whether the i-th bit is set in BitmaskArray (*bitmap)?
 * A: Call "bitmap->check( i )", which return true if it is set.
 */

#endif
