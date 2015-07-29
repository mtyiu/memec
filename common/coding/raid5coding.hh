#ifndef __COMMON_CODING_RAID5CODING_HH__
#define __COMMON_CODING_RAID5CODING_HH__

#include "coding.hh"

class RAID5Coding : public Coding {
private:
	uint32_t n; // Number of chunks in a stripe

public:
	bool init( uint32_t n );
	void encode( Chunk **data, Chunk *parity, uint32_t index );
	bool decode( Chunk **chunks, BitmaskArray *bitmap );
};

#endif
