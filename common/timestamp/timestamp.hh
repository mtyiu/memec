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

	Timestamp( uint32_t ts ) {
		LOCK_INIT( &this->lock );
		this->value = ts;
	}

	inline uint32_t nextVal() {
		uint32_t ret;
		LOCK( &this->lock );
		ret = value++;
		UNLOCK( &this->lock );
		return ret;
	}

	void setVal( uint32_t ts ) {
		LOCK( &this->lock );
		value = ts;
		UNLOCK( &this->lock );
	}

	uint32_t getVal() const {
		return this->value;
	}

	bool operator<( const Timestamp &rhs ) const {
		return this->value < rhs.value;
	}

	bool operator==( const Timestamp &rhs ) const {
		return this->value == rhs.value;
	}

	Timestamp operator-( const Timestamp &rhs ) const {
		uint32_t diff = this->value - rhs.value;
		if ( this->value < rhs.value )
			diff += UINT32_MAX;
		return Timestamp( diff );
	}

};

#endif
