#ifndef __COMMON_CODING_RSCODING_HH__
#define __COMMON_CODING_RSCODING_HH__

#include "coding.hh"

class RSCoding : public Coding {

private:

    /**
     * Get the minimal no. of packets per chunk, w
     *
     * this->_w will be set
     * @return the minimal no. of packets per chunk
     */
    uint32_t getW ();

    /**
     * Generate the matrix and schedule for jerasure
     *
     * this->__jmatrix, this->_jbitmatrix, this->_jschedule will be set
     *
     */
    void generateCodeMatrix();

    uint32_t _k;
    uint32_t _m;
    uint32_t _w;
    uint32_t _chunkSize;

    int *_jmatrix;
    
public:
    
    RSCoding ( uint32_t k = 0, uint32_t m = 0, uint32_t chunkSize = 0 );
    ~ RSCoding ();

    void encode ( Chunk **dataChunks, Chunk *parityChunk, uint32_t index, uint32_t startOff = 0, uint32_t endOff = 0 );

    bool decode ( Chunk **chunks, BitmaskArray * chunkStatus );

};

#endif
