#ifndef __COMMON_CODING_EVENODDCODING_HH__
#define __COMMON_CODING_EVENODDCODING_HH__

#include "coding.hh"
#include "rdpcoding.hh"

class EvenOddCoding : public RDPCoding {

protected:

    /**
     * Get the index to smallest prime number p > k for encoding in primeList
     * 
     * this->_p will be set
     * @return  the index to the smallest prime number p > k in primeList
     */
    uint32_t getPrime();

    /**
     * Get the size of a symbol based on prime number p for encoding
     * 
     * this->_symbolSize will be set
     * @return  the size of a symbol 
     */
    uint32_t getSymbolSize();

public:

    EvenOddCoding ( uint32_t k = 0, uint32_t chunkSize = 0 );
    ~ EvenOddCoding ();

    void encode ( Chunk **dataChunks, Chunk *parityChunk, uint32_t index, uint32_t startOff = 0, uint32_t endOff = 0 ); 

    bool decode ( Chunk **chunks, BitmaskArray *chunkStatus );

};

#endif
