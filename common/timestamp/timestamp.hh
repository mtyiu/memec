#ifndef __COMMON_TIMESTAMP_TIMESTAMP_HH__
#define __COMMON_TIMESTAMP_TIMESTAMP_HH__

#include "../lock/lock.hh"

class Timestamp {
private:
	LOCK_T lock;
	uint32_t value;

public:
	Timestamp() {
		LOCK_INIT( &this->lock );
		this->value = 0;
	}

	inline uint32_t nextVal() {
		uint32_t ret;
		LOCK( &this->lock );
		ret = value++;
		UNLOCK( &this->lock );
		return ret;
	}
};

#endif
