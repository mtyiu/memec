#ifndef __COMMON_DS_ID_GENERATOR_HH__
#define __COMMON_DS_ID_GENERATOR_HH__

#include <stdint.h>
#include <limits.h>

#ifndef UINT32_MAX
#define UINT32_MAX 0xFFFFFFFF
#endif

/* Provide a non-overlapping ID range for each thread */
class IDGenerator {
private:
	uint32_t numberOfThreads;
	uint32_t rangeSize;
	uint32_t *currentValue;
	uint32_t *limits;

public:
	IDGenerator() {
		this->numberOfThreads = 0;
		this->rangeSize = 0;
		this->currentValue = 0;
		this->limits = 0;
	}

	void init( uint32_t numberOfThreads ) {
		this->numberOfThreads = numberOfThreads;
		this->currentValue = new uint32_t[ numberOfThreads ];
		this->limits = new uint32_t[ numberOfThreads ];

		this->rangeSize = ( UINT32_MAX - 1 ) / numberOfThreads;
		for ( uint32_t i = 0; i < numberOfThreads; i++ ) {
			this->currentValue[ i ] = this->rangeSize * i;
			this->limits[ i ] = this->rangeSize * i + ( this->rangeSize - 1 );
		}
	}

	void free() {
		if ( this->currentValue )
			delete[] this->currentValue;
		if ( this->limits )
			delete[] this->limits;
		this->currentValue = 0;
		this->limits = 0;
	}

	uint32_t nextVal( uint32_t threadId ) {
		if ( this->currentValue[ threadId ] == this->limits[ threadId ] )
			this->currentValue[ threadId ] = this->rangeSize * threadId;
		else
			this->currentValue[ threadId ]++;
		return this->currentValue[ threadId ];
	}
};

#endif
