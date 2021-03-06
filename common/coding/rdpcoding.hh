#ifndef __COMMON_CODING_RDPCODING_HH__
#define __COMMON_CODING_RDPCODING_HH__

#include <vector>
#include "coding.hh"
#include "raid5coding.hh"
#include "../ds/chunk_pool.hh"

class RDPCoding : public Coding {
protected:
	/**
	 * Get the index to smallest prime number p > k + 1 for encoding in primeList
	 *
	 * this->_p will be set
	 * @return  the index to the smallest prime number p > k + 1 in primeList
	 */
	uint32_t getPrime();

	/**
	 * Get the size of a symbol based on prime number p for encoding
	 *
	 * this->_symbolSize will be set
	 * @return  the size of a symbol
	 */
	uint32_t getSymbolSize();

	/**
	 * XOR chunks (RAID-5)
	 *
	 * @param chunks data and parity chunks involved
	 * @param target index of the target chunk to encode / decode
	 */
	void performXOR( Chunk **chunks, uint32_t target );

	RAID5Coding* _raid5Coding;
	uint32_t _k;
	uint32_t _p;
	uint32_t _chunkSize;
	uint32_t _symbolSize;
	TempChunkPool tempChunkPool;

	// use some memory to save computation (assume p < 200)
	static const uint32_t primeCount = 168;
	static const uint32_t primeList[ primeCount ];

public:
	RDPCoding( uint32_t k = 0, uint32_t chunkSize = 0 );
	~RDPCoding();

	void encode( Chunk **dataChunks, Chunk *parityChunk, uint32_t index, uint32_t startOff = 0, uint32_t endOff = 0 );
	bool decode( Chunk **chunks, BitmaskArray *chunkStatus );
};

#endif
