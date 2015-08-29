#ifndef __COMMON_CODING_RAID0CODING_HH__
#define __COMMON_CODING_RAID0CODING_HH__

#include "coding.hh"

class RAID0Coding : public Coding {
private:
	uint32_t n; // Number of chunks in a stripe

public:
	bool init( uint32_t n );
	void encode( Chunk **data, Chunk *parity, uint32_t index, uint32_t startOff = 0, uint32_t endOff = 0);
	bool decode( Chunk **chunks, BitmaskArray *bitmap );
};

#endif
