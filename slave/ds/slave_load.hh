#ifndef __SLAVE_DS_SLAVE_LOAD_HH__
#define __SLAVE_DS_SLAVE_LOAD_HH__

#include "../../common/ds/load.hh"

class SlaveLoad : public Load {
public:
	double elapsedTime;
	uint64_t sentBytes;
	uint64_t recvBytes;

	SlaveLoad();
	void reset();
	void aggregate( SlaveLoad &l );
	void print( FILE *f = stdout );
	void updateChunk();
	void delChunk();
};

#endif
