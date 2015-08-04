#ifndef __SLAVE_DS_LOAD_HH__
#define __SLAVE_DS_LOAD_HH__

#include <cstdio>
#include "../../common/protocol/protocol.hh"

typedef struct HeartbeatHeader OpLoad;

class Load {
public:
	double elapsedTime;
	uint64_t sentBytes;
	uint64_t recvBytes;
	OpLoad ops;

	Load();
	void reset();
	void aggregate( Load &l );
	void print( FILE *f = stdout );
	inline void get() { this->ops.get++; }
	inline void set() { this->ops.set++; }
	inline void update() { this->ops.update++; }
	inline void del() { this->ops.del++; }
};

#endif
