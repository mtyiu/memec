#ifndef __COMMON_CODING_RAID5CODING_HH__
#define __COMMON_CODING_RAID5CODING_HH__

#include "coding.hh"

class Raid5Coding2: Coding {

public:
    Raid5Coding2 ( uint32_t k = 0, uint32_t chunkSize = 0 );
    ~Raid5Coding2 ();

    void encode( Chunk **dataChunks, Chunk *parityChunk, uint32_t index );
    bool decode( Chunk **chunks, BitmaskArray *chunkStatus );

private:
    uint32_t _k;
    uint32_t _chunkSize;

};

#endif
