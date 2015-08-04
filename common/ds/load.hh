#ifndef __COMMON_DS_LOAD_HH__
#define __COMMON_DS_LOAD_HH__

#include <cstdio>
#include "../../common/protocol/protocol.hh"

typedef struct HeartbeatHeader OpLoad;

class Load {
public:
	OpLoad ops;

	Load();
	virtual void reset();
	virtual void aggregate( Load &l );
	virtual void print( FILE *f = stdout );
	inline void get() { this->ops.get++; }
	inline void set() { this->ops.set++; }
	inline void update() { this->ops.update++; }
	inline void del() { this->ops.del++; }
};

#endif
