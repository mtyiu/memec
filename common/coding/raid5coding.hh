#ifndef __COMMON_CODING_RAID5CODING_HH__
#define __COMMON_CODING_RAID5CODING_HH__

#include "coding.hh"
#ifdef USE_ISAL
#include "../../lib/isa-l-2.14.0/include/erasure_code.h"
typedef unsigned char dataType;
#else
typedef char dataType;
#endif

#define RAID5_N_MAX	(32)

class RAID5Coding : public Coding {
private:
	uint32_t n; // Number of chunks in a stripe
#ifdef USE_ISAL
	unsigned char _gftbl[ RAID5_N_MAX * RAID5_N_MAX * 32 ];
	unsigned char _encodeMatrix[ RAID5_N_MAX * RAID5_N_MAX ];
#endif

public:
	bool init( uint32_t n );
	void encode( Chunk **data, Chunk *parity, uint32_t index, uint32_t startOff = 0, uint32_t endOff = 0);
	bool decode( Chunk **chunks, BitmaskArray *bitmap );
};

#endif
