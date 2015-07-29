#ifndef __COMMON_CODING_RDPCODING_HH__
#define __COMMON_CODING_RDPCODING_HH__

#include <vector>
#include "coding.hh"
#include "raid5coding2.hh"

class RDPCoding : Coding {

public:

    RDPCoding ( uint32_t k = 0, uint32_t chunkSize = 0 );
    ~ RDPCoding ();

    void encode ( Chunk **dataChunks, Chunk *parityChunks, uint32_t index ); 

    bool decode ( Chunk **chunks, BitmaskArray *chunkStatus );

private:

    /**
     * Get the smallest prime number p > k + 1 for encoding
     * 
     * this->_p will be set
     * @return  the smallest prime number p > k + 1
     */
    uint32_t getPrime ();

    /**
     * Get the size of a symbol based on prime number p for encoding
     * 
     * this->_symbolSize will be set
     * @return  the size of a symbol 
     */
    uint32_t getSymbolSize();

    Raid5Coding2* _raid5Coding;
    uint32_t _k;
    uint32_t _p;
    uint32_t _chunkSize;
    uint32_t _symbolSize;

    // use some memory to save computation (assume k < 200)
    static const uint32_t primeCount = 46;
    static const uint32_t primeList[primeCount];

};

#endif
