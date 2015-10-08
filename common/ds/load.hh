#ifndef __COMMON_DS_LOAD_HH__
#define __COMMON_DS_LOAD_HH__

#include <cstdio>
#include "../../common/protocol/protocol.hh"

class Load {
public:
	uint32_t _get;
	uint32_t _set;
	uint32_t _update;
	uint32_t _del;

	Load();
	virtual void reset();
	virtual void aggregate( Load &l );
	virtual void print( FILE *f = stdout );

	inline void get() { this->_get++; }
	inline void set() { this->_set++; }
	inline void update() { this->_update++; }
	inline void del() { this->_del++; }
};

#endif
