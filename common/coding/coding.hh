#ifndef __COMMON_CODING_CODING_HH__
#define __COMMON_CODING_CODING_HH__

#include <stdint.h>
#include "coding_params.hh"
#include "../ds/chunk.hh"
#include "../ds/bitmask_array.hh"

class Coding {
public:
	CodingScheme scheme;
	static char *zeros;

	virtual ~Coding();
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
	
	static Coding *instantiate( CodingScheme scheme, CodingParams &params, uint32_t chunkSize );
	static void destroy( Coding *coding );

	/**
	 * Perform bitwise XOR.
	 * @param dst  Output (XOR-ed value)
	 * @param srcA Input 1
	 * @param srcB Input 2
	 * @param len  Size of the input
	 * @return     Output (same as dst)
	 */
	static char *bitwiseXOR( char *dst, char *srcA, char *srcB, uint32_t len );
	static Chunk *bitwiseXOR( Chunk *dst, Chunk *srcA, Chunk *srcB, uint32_t size );
};

/**
 * Q: How to check whether the i-th bit is set in BitmaskArray (*bitmap)?
 * A: Call "bitmap->check( i )", which return true if it is set.
 */

#endif
